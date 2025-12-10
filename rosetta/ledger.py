import pandas as pd
import uuid
import re
from typing import List, Optional
from .config import get_logger

logger = get_logger(__name__)

class LedgerEngine:
    def __init__(self):
        pass

    def generate_splits(self, categorized_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the categorized single-row transactions into multi-row splits.
        Input DF usually has: [transaction_id, date, amount, account (category), currency, price, meta, description]
        
        Logic:
        1. Parse description for Investment Intent (if not already done).
        2. Generate Split 1 (Source/Bank):
           - Account: 'Assets:Bank:Unknown' (or derived)
           - Amount: Original signed amount (-50 means money left bank, asset decreases by 50. Wait. )
             Standard Ledger Polarity:
             - Assets are Positive. Expenses are Positive. Income is Negative (Equity).
             - Or: 
               - Asset DB: -50 (Credit Bank)
               - Expense CR: +50 (Debit Expense)
             - Let's stick to standard sign conventions used in the 'amount' column so far:
               - -50.00 means Outflow.
               - In a Ledger:
                 - Split 1: Account=Assets:Bank, Amount= -50.00
                 - Split 2: Account=Expenses:Groceries, Amount= +50.00
                 - Sum = 0.
        
        3. Handle Investments (Secondary Parsing Logic inside Loop):
           - If description matches "Buy 10 AAPL @ 150":
             - Original Amount: -1500 (Currency Outflow)
             - Split 1: Account=Assets:Bank, Amount=-1500 USD
             - Split 2: Account=Assets:Investments:AAPL, Amount=10 (Quantity). 
               - Wait, usually ledger balances Currency.
               - -1500 USD + 10 AAPL != 0 unless priced.
               - In text ledgers like Beancount/Ledger:
                 - Assets:Bank  -1500.00 USD
                 - Assets:Stock  10 AAPL @ 150.00 USD
               - We need columns for `amount` (quantity) and `commodity` (currency/ticker).
               - The `amount` column in our schema is likely the currency value.
               - The schema has `price`.
               
        For now, let's implement standard Expense/Income splitting first.
        """
        logger.info("Stage 6: Generating Ledger Splits...")
        
        splits = []
        
        for _, row in categorized_df.iterrows():
            trans_id = row['transaction_id']
            date = row['date']
            desc = row['description']
            amount = row['amount'] # Signed: -50 for expense
            currency = row['currency']
            category_account = row['account'] # e.g. Expenses:Groceries
            
            # --- Secondary Parser for Investments ---
            # Very basic Regex POC
            # Pattern: "Buy <qty> <ticker> @ <price>"
            # Example: "Buy 10 AAPL @ 150"
            inv_match = re.search(r"(Buy|Sell)\s+(\d+(?:\.\d+)?)\s+([A-Z]+)\s+@\s+(\d+(?:\.\d+)?)", desc, re.IGNORECASE)
            
            if inv_match:
                # Investment Transaction detected
                action, qty, ticker, price = inv_match.groups()
                qty = float(qty)
                price = float(price)
                
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
                # If Buy, we gain 10 AAPL.
                target_qty = qty if action.lower() == 'buy' else -qty
                
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
