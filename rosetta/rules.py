import pandas as pd
import numpy as np
import uuid
from .models import ColumnMapping, DecimalSeparator
from .config import get_logger

logger = get_logger(__name__)

class RulesEngine:
    def __init__(self, mapping: ColumnMapping):
        self.mapping = mapping

    def parse_float(self, val, decimal_sep: DecimalSeparator):
        if pd.isna(val) or val == "":
            return 0.0
        
        if isinstance(val, (int, float)):
            return float(val)
            
        s = str(val).strip()
        # Clean common currency symbols
        s = s.replace("EUR", "").replace("USD", "").replace("$", "").replace("€", "").strip()
        
        if decimal_sep == DecimalSeparator.DOT:
             # Standard US/Scientific: 1,000.50 -> 1000.50
             # Remove thousands separator (comma)
             s = s.replace(",", "")
        else:
             # European: 1.000,50 -> 1000.50
             # Remove thousands separator (dot) and replace decimal comma with dot
             s = s.replace(".", "").replace(",", ".")
             
        try:
            return float(s)
        except ValueError:
            return 0.0

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Stage 4: Rules Engine - Normalizing Data...")
        
        # Date Parsing
        date_series = pd.to_datetime(df[self.mapping.date_col], errors='coerce')
        
        # Polarity Handling & Amount Parsing
        p = self.mapping.polarity
        signed_amount = pd.Series(0.0, index=df.index)
        
        if p.type == "signed":
            # Case A
            if not self.mapping.amount_col:
                raise ValueError("Amount column is required for Signed polarity case")
            raw_col = self.mapping.amount_col
            signed_amount = df[raw_col].apply(lambda x: self.parse_float(x, self.mapping.decimal_separator))
            
        elif p.type == "direction":
            # Case B
            if not self.mapping.amount_col:
                 raise ValueError("Amount column is required for Direction polarity case")
                 
            # Calculate absolute amount first
            raw_col = self.mapping.amount_col
            # Parse and take abs (just in case raw data has inconsistent signs)
            abs_amount = df[raw_col].apply(lambda x: abs(self.parse_float(x, self.mapping.decimal_separator)))
            
            direction_col = p.direction_col
            
            def calculate_signed(row):
                # Use row.name for index alignment or row directly
                idx = row.name
                amount = abs_amount.loc[idx]
                
                d_val = str(row[direction_col]).strip()
                
                # Check for exact or substring matches (case sensitive or insensitive? prompt gave capitalized)
                # Let's be slightly loose
                if p.outgoing_value in d_val:
                    return -amount
                elif p.incoming_value in d_val:
                    return amount
                else:
                    # Default handling: Log warning? return amount?
                    return amount
            
            signed_amount = df.apply(calculate_signed, axis=1)

        elif p.type == "credit_debit":
            # Case C
            # Parse both columns
            credit_vals = df[p.credit_col].apply(lambda x: self.parse_float(x, self.mapping.decimal_separator))
            debit_vals = df[p.debit_col].apply(lambda x: self.parse_float(x, self.mapping.decimal_separator))
            
            # Income (Credit) is positive, Expense (Debit) is negative
            # We assume values in columns are absolute magnitudes
            signed_amount = credit_vals.abs() - debit_vals.abs()
            
        # Create Result DataFrame
        result = pd.DataFrame()
        
        # Deterministic ID Generation
        import hashlib
        def generate_id(row):
            # content string: date_str + amount_str + desc_str
            d_str = str(row[self.mapping.date_col])
            a_str = str(row[self.mapping.amount_col]) if self.mapping.amount_col else ""
            desc_str = str(row[self.mapping.desc_col])
            
            content = f"{d_str}{a_str}{desc_str}".encode('utf-8')
            return str(uuid.UUID(hashlib.sha256(content).hexdigest()[:32]))

        result['transaction_id'] = df.apply(generate_id, axis=1)
        result['date'] = date_series
        result['account'] = "Assets:Bank:Unknown" 
        result['amount'] = signed_amount
        result['currency'] = "EUR" 
        result['price'] = pd.Series([None] * len(df), dtype="float64")
        
        # Create Meta (JSON dump of original row)
        result['meta'] = df.apply(lambda row: row.to_json(), axis=1)
        
        return result
