import re
import uuid
import pandas as pd
import instructor
import ollama
from openai import OpenAI
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from rosetta.utils import get_logger
from rosetta.data.constants import (
    DEFAULT_ASSET_ACCOUNT, 
    DEFAULT_CURRENCY, 
    INVESTMENT_KEYWORDS, 
    INVESTMENT_REGEX_PATTERNS,
    LEDGER_INVESTMENT_PROMPT,
    LLM_BASE_URL,
    LLM_API_KEY,
    LLM_MODEL_NAME
)

logger = get_logger(__name__)

class InvestmentDetails(BaseModel):
    action: str
    quantity: float
    ticker: str
    price: float

class LedgerEngine:
    """
    Stage 6: Double-Entry Ledger Generator.
    Converts single-row transactions into multi-row splits.
    Handles standard expenses and complex investment transactions.
    """

    def __init__(self):
        # LLM client for backup investment extraction
        self.client = instructor.from_openai(
            OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY),
            mode=instructor.Mode.JSON,
        )

    def generate_splits(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main entry point. Iterates through the DataFrame and generates splits.
        """
        logger.info("Generating Double-Entry Ledger Splits...")
        all_splits = []

        for _, row in df.iterrows():
            # Check for Investment
            if self._detect_investment(row.get('description', '')):
                splits = self._create_investment_splits(row)
            else:
                splits = self._create_standard_splits(row)
            
            all_splits.extend(splits)

        out_df = pd.DataFrame(all_splits)
        
        # Schema Compliance: Ensure all expected columns exist
        if not out_df.empty:
            if 'price' not in out_df.columns:
                out_df['price'] = pd.Series([None] * len(out_df), dtype="float64")
            if 'meta' not in out_df.columns:
                out_df['meta'] = None # Object is fine with None
                
        return out_df

    def _create_standard_splits(self, row: pd.Series) -> List[Dict]:
        """
        Standard Source -> Destination flow.
        Split 1: Asset Account (Negative Amount)
        Split 2: Expense Account (Positive Amount)
        """
        transaction_id = row.get('transaction_id', str(uuid.uuid4()))
        date = row['date']
        desc = row['description']
        amount = float(row['amount'])
        
        # Account from Categorizer
        category_account = row.get('account', 'Expenses:Uncategorized')
        
        # Split 1: The Bank (Asset)
        # Verify polarity. If I spent -50, Bank should go -50.
        # If I earned 50, Bank should go +50.
        split_bank = {
            'transaction_id': transaction_id,
            'date': date,
            'description': desc,
            'account': DEFAULT_ASSET_ACCOUNT,
            'amount': amount, 
            'currency': DEFAULT_CURRENCY
        }

        # Split 2: The Category (Expense/Income)
        # Double Entry: Sum must be 0.
        # So Category amount is -1 * Bank Amount.
        # Spent -50 -> Bank -50 -> Expense +50.
        split_category = {
            'transaction_id': transaction_id,
            'date': date,
            'description': desc,
            'account': category_account,
            'amount': -amount,
            'currency': DEFAULT_CURRENCY
        }

        return [split_bank, split_category]

    def _detect_investment(self, description: str) -> bool:
        """
        Fast keyword check to see if this MIGHT be an investment.
        """
        if not description:
            return False
            
        desc_lower = description.lower()
        all_keywords = [k for sublist in INVESTMENT_KEYWORDS.values() for k in sublist]
        return any(k in desc_lower for k in all_keywords)

    def _create_investment_splits(self, row: pd.Series) -> List[Dict]:
        """
        Handles Buy/Sell flows.
        """
        desc = row['description']
        details = self._extract_investment_data(desc)
        
        if not details or not details.ticker:
            logger.warning(f"Investment detected but failed extraction: '{desc}'. Fallback to Standard.")
            return self._create_standard_splits(row)
            
        # Success!
        transaction_id = row.get('transaction_id', str(uuid.uuid4()))
        date = row['date']
        amount = float(row['amount']) # Total cash impact (e.g. -1500)
        
        # Split 1: Cash Flow (Bank)
        split_bank = {
            'transaction_id': transaction_id,
            'date': date,
            'description': desc,
            'account': DEFAULT_ASSET_ACCOUNT,
            'amount': amount,
            'currency': DEFAULT_CURRENCY
        }
        
        # Split 2: Asset Flow (Security)
        # If I bought (-1500 EUR), I GAINED (+10 AAPL).
        # We need to determine sign of quantity. 
        # Usually 'Buy' -> Positive Qty, 'Sell' -> Negative Qty.
        
        # Normalize action
        is_buy = any(kw in details.action.lower() for kw in INVESTMENT_KEYWORDS['buy'])
        qty_sign = 1 if is_buy else -1
        
        final_qty = abs(details.quantity) * qty_sign
        
        split_asset = {
            'transaction_id': transaction_id,
            'date': date,
            'description': f"{details.action} {details.quantity} {details.ticker} @ {details.price}",
            'account': f"Assets:Investments:{details.ticker.upper()}",
            'amount': final_qty,
            'currency': details.ticker.upper() # Tracks units
        }
        
        return [split_bank, split_asset]

    def _extract_investment_data(self, description: str) -> Optional[InvestmentDetails]:
        """
        Hybrid Extractor: Regex Fast Path -> LLM Slow Path.
        """
        # 1. Fast Path (Regex)
        for pattern in INVESTMENT_REGEX_PATTERNS:
            match = re.search(pattern, description)
            if match:
                try:
                    # Groups: 1=Action, 2=Qty, 3=Ticker, 4=Price
                    action, qty, ticker, price = match.groups()
                    # Clean price (replace , with .)
                    price = price.replace(',', '.')
                    
                    details = InvestmentDetails(
                        action=action,
                        quantity=float(qty),
                        ticker=ticker,
                        price=float(price)
                    )
                    logger.info(f"FAST PATH INVESTMENT: {details}")
                    return details
                except Exception as e:
                    logger.warning(f"Regex match failed parsing: {e}")

        # 2. Slow Path (LLM)
        logger.info(f"SLOW PATH INVESTMENT: Detecting details for '{description}' via LLM...")
        try:
            details = self.client.chat.completions.create(
                model=LLM_MODEL_NAME,
                messages=[
                    {"role": "system", "content": LEDGER_INVESTMENT_PROMPT},
                    {"role": "user", "content": description}
                ],
                response_model=InvestmentDetails,
                max_retries=1
            )
            logger.info(f"LLM EXTRACTED: {details}")
            return details
        except Exception as e:
            logger.error(f"LLM Investment Extraction Failed: {e}")
            return None
