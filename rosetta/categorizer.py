from enum import Enum
import pandas as pd
import instructor
from openai import OpenAI
from pydantic import BaseModel, Field
from .config import get_logger

logger = get_logger(__name__)

class CategoryEnum(str, Enum):
    GROCERIES = "Expenses:Groceries"
    TRAVEL = "Expenses:Travel"
    UTILITIES = "Expenses:Utilities"
    INCOME = "Income:Standard"
    TRANSFER = "Transfers"
    UNKNOWN = "Expenses:Unknown"

class TransactionCategory(BaseModel):
    category: CategoryEnum

class Categorizer:
    def __init__(self):
        self.client = instructor.from_openai(
            OpenAI(
                base_url="http://localhost:11434/v1",
                api_key="ollama", 
            ),
            mode=instructor.Mode.JSON,
        )

    def classify_description(self, description: str, amount: float) -> str:
        """
        Slow Path: Uses LLM to classify a single transaction.
        """
        try:
            resp = self.client.chat.completions.create(
                model="deepseek-r1:8b",
                messages=[
                    {
                        "role": "user",
                        "content": f"""
                        Classify this financial transaction into a category.
                        Description: {description}
                        Amount: {amount}
                        
                        Categories:
                        - Expenses:Groceries (Supermarkets, Food)
                        - Expenses:Travel (Trains, Flights, Hotels)
                        - Expenses:Utilities (Bills, Internet, Phone)
                        - Income:Standard (Salary, Refunds)
                        - Transfers (Internal transfers)
                        - Expenses:Unknown (If unsure)
                        """
                    }
                ],
                response_model=TransactionCategory,
                max_retries=1
            )
            return resp.category.value
            
        except Exception as e:
            logger.warning(f"Categorization failed for '{description}': {e}")
            return CategoryEnum.UNKNOWN.value

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Stage 5: Categorizer - Classifying Transactions...")
        
        # We only need to categorize, not change structure. 
        # Modifies 'account' column in place or returns copy? 
        # Better return a new copy or modify. Let's return modified df.
        
        # Iterating for MVP (Vector DB / Bulk LLM would be faster)
        new_accounts = []
        for _, row in df.iterrows():
            # Extract description from meta or implicit knowledge?
            # We need the description column again. 
            # In RulesEngine, we lost the original columns in the 'result' df except in 'meta'.
            # Ideally, we should categorization BEFORE normalization or extract from meta.
            # Let's extract from the mapped description in meta for simplicity? 
            # OR pass the original dataframe? 
            # Actually, RulesEngine returned a CLEAN schema. 'meta' contains original JSON.
            # But parsing JSON back is slow.
            
            # PROPOSAL: Pass the description through RulesEngine?
            # Or just parse 'meta' since we have it.
            
            # Wait, `main.py` has access to `normalized_df`.
            # normalized_df has 'meta'.
            
            import json
            try:
                meta_dict = json.loads(row['meta'])
                # But we don't know WHICH field in meta is description without the mapping!
                # We need the mapping here too? Or store standard description in `normalized_df`?
                # Storing standard description in normalized_df is cleaner.
                # But prompt said: "account" field replacing "Assets:Bank:Unknown".
                
                # Let's simple-parse meta if we can, or refactor RulesEngine to include description col.
                # Refactoring RulesEngine to keep 'description' column in normalized_df is best practice.
                # But strict schema? 
                # TargetSchema has: transaction_id, date, account, amount, currency, price, meta.
                # It does NOT have 'description'. 
                # So we must rely on 'meta' or passed mapping.
                pass
            except:
                pass
                
        # To avoid complexity, I will UPDATE RulesEngine to populate a temporary 'description' column
        # OR just pass the mapping to Categorizer. Passing mapping is cleaner.
        return df

    def run_categorization(self, df: pd.DataFrame, mapping) -> pd.DataFrame:
        """
        Applies categorization using the original dataframe's structure via mapping.
        df: The NORMALIZED dataframe. which has 'meta'
        mapping: The ColumnMapping used to create it.
        """
        
        logger.info("Stage 5: Categorizer - Classifying Transactions...")
        
        classified_accounts = []
        
        # We need raw description. 
        # Option A: We can't easily link back to raw DF row-by-row unless order preserved (it is).
        # extract description from meta using mapping.desc_col
        
        import json
        
        for idx, row in df.iterrows():
            meta = json.loads(row['meta'])
            raw_desc = meta.get(mapping.desc_col, "Unknown")
            amount = row['amount']
            
            category = self.classify_description(raw_desc, amount)
            classified_accounts.append(category)
            
        df['account'] = classified_accounts
        return df
