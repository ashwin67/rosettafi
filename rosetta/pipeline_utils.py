import pandas as pd
from rosetta.models import ColumnMapping

def normalize_amounts(df: pd.DataFrame, mapping: ColumnMapping) -> pd.DataFrame:
    """
    Standardizes amounts based on the Polarity logic described in the mapping.
    Ensures a single 'amount' column where negative is outflow and positive is inflow.
    """
    df = df.copy()
    
    # 1. Handle Decimal Separator
    def parse_amount(val):
        if pd.isna(val) or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        
        s = str(val).strip()
        if mapping.decimal_separator.value == ',':
            # Dutch/German format: 1.234,56
            s = s.replace('.', '').replace(',', '.')
        else:
            # English format: 1,234.56
            s = s.replace(',', '')
        
        try:
            return float(s)
        except ValueError:
            return 0.0

    # 2. Apply Polarity Cases
    if mapping.polarity.type == 'signed':
        df['amount'] = df[mapping.amount_col].apply(parse_amount)
    
    elif mapping.polarity.type == 'direction':
        col = mapping.polarity.direction_col
        df['amount_raw'] = df[mapping.amount_col].apply(parse_amount)
        
        def adjust(row):
            val = row[col]
            amt = row['amount_raw']
            if str(val).lower() == mapping.polarity.outgoing_value.lower():
                return -abs(amt)
            return abs(amt)
            
        df['amount'] = df.apply(adjust, axis=1)

    elif mapping.polarity.type == 'credit_debit':
        credit_col = mapping.polarity.credit_col
        debit_col = mapping.polarity.debit_col
        
        def resolve_cd(row):
            c = parse_amount(row.get(credit_col, 0))
            d = parse_amount(row.get(debit_col, 0))
            # If both are present, we might have a problem, but usually one is NaN
            if abs(c) > 0: return abs(c)
            if abs(d) > 0: return -abs(d)
            return 0.0
            
        df['amount'] = df.apply(resolve_cd, axis=1)

    return df
