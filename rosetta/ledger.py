import pandas as pd
import uuid
import re
from typing import List, Optional
import instructor
from openai import OpenAI
from pydantic import BaseModel
from .config import get_logger

logger = get_logger(__name__)

class InvestmentExtract(BaseModel):
    action: str
    quantity: float
    ticker: str
    price: float

class LedgerEngine:
    def __init__(self):
        pass

    def generate_splits(self, categorized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the categorized single-row transactions into multi-row splits.
        Hybrid Mode: Uses Regex (Fast) -> LLM Fallback (Slow) for Investments.
        """
        logger.info("Stage 6: Generating Ledger Splits (Hybrid Mode)...")
        
        splits = []
        
        # Initialize LLM client
        client = instructor.from_openai(
            OpenAI(base_url="http://localhost:11434/v1", api_key="ollama"),
            mode=instructor.Mode.JSON,
        )
        
        for _, row in categorized_df.iterrows():
            trans_id = row['transaction_id']
            date = row['date']
            desc = row['description']
            amount = row['amount'] # Signed: -50 for expense
            currency = row['currency']
            category_account = row['account'] # e.g. Expenses:Groceries
            
            inv_data = None
            
            # Step 1: Regex (Fast Path)
            # Pattern: "Buy <qty> <ticker> @ <price>"
            inv_match = re.search(r"(Buy|Sell)\s+(\d+(?:\.\d+)?)\s+([A-Z]+)\s+@\s+(\d+(?:\.\d+)?)", desc, re.IGNORECASE)
            
            if inv_match:
                action, qty, ticker, price = inv_match.groups()
                inv_data = {
                    "action": action, 
                    "qty": float(qty), 
                    "ticker": ticker, 
                    "price": float(price)
                }
            
            # Step 2: Fallback LLM (Slow Path)
            elif any(k in desc for k in ["ISIN", "Shares", "@"]):
                 try:
                     extract = client.chat.completions.create(
                        model="llama3.2",
                        messages=[{
                            "role": "user", 
                            "content": f"Extract investment details from: '{desc}'. Return JSON with action (Buy/Sell), quantity, ticker, and price."
                        }],
                        response_model=InvestmentExtract,
                        max_retries=1
                     )
                     inv_data = {
                         "action": extract.action,
                         "qty": extract.quantity,
                         "ticker": extract.ticker,
                         "price": extract.price
                     }
                 except Exception as e:
                     logger.warning(f"LLM Fallback failed for '{desc}': {e}")

            if inv_data:
                # Investment Transaction detected
                action = inv_data['action']
                qty = inv_data['qty']
                ticker = inv_data['ticker']
                price = inv_data['price']
                
                # Logic:
                # If Buy: Money Out (amount is negative), Asset In (Positive Qty)
                # If Sell: Money In (amount is positive), Asset Out (Negative Qty)
                
                # Split 1: Bank (Currency Side) - Use original row amount
                splits.append({
                    "transaction_id": trans_id,
                    "date": date,
                    "account": "Assets:Bank:Unknown", # Source
                    "amount": amount,
                    "currency": currency,
                    "price": None,
                    "meta": row['meta'],
                    "description": desc
                })
                
                # Split 2: Investment (Asset Side)
                target_qty = qty if 'buy' in action.lower() else -qty
                
                splits.append({
                    "transaction_id": trans_id,
                    "date": date,
                    "account": f"Assets:Investments:{ticker.upper()}",
                    "amount": target_qty, # Quantity
                    "currency": ticker.upper(), # Commodity
                    "price": price, # Price per unit
                    "meta": row['meta'],
                    "description": desc
                })
                
            else:
                # Standard Transaction
                # Split 1: Bank
                splits.append({
                    "transaction_id": trans_id,
                    "date": date,
                    "account": "Assets:Bank:Unknown", # Should ideally be parameterized or preserved from Rules
                    "amount": amount,
                    "currency": currency,
                    "price": None,
                    "meta": row['meta'],
                    "description": desc
                })
                
                # Split 2: Category
                splits.append({
                    "transaction_id": trans_id,
                    "date": date,
                    "account": category_account,
                    "amount": -amount,
                    "currency": currency,
                    "price": None,
                    "meta": row['meta'],
                    "description": desc
                })
                
        splits_df = pd.DataFrame(splits)
        
        # Coerce types to match schema
        if not splits_df.empty:
            splits_df['amount'] = splits_df['amount'].astype(float)
            splits_df['price'] = pd.to_numeric(splits_df['price'], errors='coerce') # object -> float
            
        return splits_df
