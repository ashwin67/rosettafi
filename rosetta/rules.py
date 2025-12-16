import pandas as pd
import numpy as np
import uuid
import re
from abc import ABC, abstractmethod
from typing import Union
from .models import ColumnMapping, DecimalSeparator
from rosetta.utils import get_logger
from .data.constants import CLEAN_CURRENCY_REGEX, UNICODE_REPLACEMENTS

logger = get_logger(__name__)

# ==============================================================================
# STRATEGY PATTERN: PARSING
# ==============================================================================

class ParsingStrategy(ABC):
    """Abstract Base Class for Locale-Specific Parsing Strategies."""
    
    @abstractmethod
    def parse_float(self, val: any) -> float:
        pass

class USParsingStrategy(ParsingStrategy):
    """
    Handles standard US/UK formats: 1,234.56
    - Decimal Separator: Dot (.)
    - Thousands Separator: Comma (,)
    """
    def parse_float(self, val: any) -> float:
        if pd.isna(val) or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
            
        s = str(val).strip()
        # 1. Normalize Unicode (e.g. minus signs)
        for k, v in UNICODE_REPLACEMENTS.items():
            s = s.replace(k, v)
            
        # 2. Clean Currency Symbols and Spaces
        s = re.sub(CLEAN_CURRENCY_REGEX, '', s)

        # 3. Remove Thousands Separator (Comma)
        s = s.replace(',', '')
        
        try:
            return float(s)
        except ValueError:
            logger.warning(f"USParsingStrategy failed for value: {val}")
            return 0.0

class EUParsingStrategy(ParsingStrategy):
    """
    Handles European formats: 1.234,56
    - Decimal Separator: Comma (,)
    - Thousands Separator: Dot (.) or Space
    """
    def parse_float(self, val: any) -> float:
        if pd.isna(val) or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
            
        s = str(val).strip()
        # 1. Normalize Unicode
        for k, v in UNICODE_REPLACEMENTS.items():
            s = s.replace(k, v)
        
        # 2. Clean Currency & Garbage
        s = re.sub(CLEAN_CURRENCY_REGEX, '', s)
        
        # 3. Handle EU Logic: Remove dots (thousands), Replace comma with dot (decimal)
        s = s.replace('.', '') # Remove thousands separator
        s = s.replace(',', '.') # Convert decimal comma to dot
        
        try:
            return float(s)
        except ValueError:
            logger.warning(f"EUParsingStrategy failed for value: {val}")
            return 0.0

# ==============================================================================
# RULES ENGINE
# ==============================================================================

class RulesEngine:
    def __init__(self, mapping: ColumnMapping):
        self.mapping = mapping
        self.strategy = self._get_strategy()
        
    def _get_strategy(self) -> ParsingStrategy:
        if self.mapping.decimal_separator == DecimalSeparator.DOT:
            return USParsingStrategy()
        else:
            return EUParsingStrategy()

    def parse_float(self, val) -> float:
        """Facade to the strategy's parse method."""
        return self.strategy.parse_float(val)

    # --- Polarity Helpers ---
    
    def _apply_case_a(self, df: pd.DataFrame) -> pd.Series:
        """Case A: Signed Amount column."""
        col = self.mapping.amount_col
        if not col:
            logger.warning("Case A requres amount_col. Returning zeros.")
            return pd.Series(0.0, index=df.index)
            
        return df[col].apply(self.parse_float)

    def _apply_case_b(self, df: pd.DataFrame) -> pd.Series:
        """Case B: Absolute Amount + Direction Column."""
        amt_col = self.mapping.amount_col
        dir_col = self.mapping.polarity.direction_col
        
        # Pre-parse absolute amounts
        abs_amounts = df[amt_col].apply(lambda x: abs(self.parse_float(x)))
        
        outgoing_kw = self.mapping.polarity.outgoing_value.lower()
        incoming_kw = self.mapping.polarity.incoming_value.lower()
        
        def calculate(row):
            idx = row.name
            val = abs_amounts.loc[idx]
            d_val = str(row[dir_col]).lower().strip()
            
            if outgoing_kw in d_val:
                return -val
            elif incoming_kw in d_val:
                return val
            return val # Default or unmatched
            
        return df.apply(calculate, axis=1)

    def _apply_case_c(self, df: pd.DataFrame) -> pd.Series:
        """Case C: Separate Credit and Debit columns."""
        credit_col = self.mapping.polarity.credit_col
        debit_col = self.mapping.polarity.debit_col
        
        credit_vals = df[credit_col].apply(self.parse_float).abs()
        debit_vals = df[debit_col].apply(self.parse_float).abs()
        
        return credit_vals - debit_vals

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Stage 4: Rules Engine - Normalizing Data using {self.strategy.__class__.__name__}...")
        
        # 1. Date Parsing
        # Clean '.0' suffix from float-like dates (20240831.0)
        date_str = df[self.mapping.date_col].astype(str).str.replace(r'\.0$', '', regex=True)
        date_series = pd.to_datetime(date_str, errors='coerce')
        
        # 2. Polarity & Parsing
        ptype = self.mapping.polarity.type
        signed_amount = pd.Series(0.0, index=df.index)
        
        if ptype == "signed":
            signed_amount = self._apply_case_a(df)
        elif ptype == "direction":
            signed_amount = self._apply_case_b(df)
        elif ptype == "credit_debit":
            signed_amount = self._apply_case_c(df)
            
        # 3. Create Result DataFrame
        result = pd.DataFrame()
        
        # Deterministic ID Generation
        import hashlib
        def generate_id(row):
            # content string: date_str + amount_str + desc_str
            d_str = str(row['date'])
            a_str = str(row['amount'])
            desc_str = str(row['description']) # Using the mapped description
            content = f"{d_str}{a_str}{desc_str}".encode('utf-8')
            return str(uuid.UUID(hashlib.sha256(content).hexdigest()[:32]))

        # Temporary assign for ID generation
        temp_df = pd.DataFrame()
        temp_df['date'] = date_series
        temp_df['amount'] = signed_amount
        temp_df['description'] = df[self.mapping.desc_col].astype(str).str.strip()
        
        result['transaction_id'] = temp_df.apply(generate_id, axis=1)
        result['date'] = date_series
        result['account'] = "Assets:Bank:Unknown" 
        result['amount'] = signed_amount
        result['currency'] = "EUR" 
        result['price'] = pd.Series([None] * len(df), dtype="float64")
        result['description'] = temp_df['description']
        
        # Meta JSON
        result['meta'] = df.apply(lambda row: row.to_json(), axis=1)
        
        return result
