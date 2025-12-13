from typing import List, Optional
from rosetta.models import ColumnMapping, PolarityCaseA, PolarityCaseB, PolarityCaseC, DecimalSeparator
from rosetta.data.constants import (
    KEYWORDS_DATE, KEYWORDS_AMOUNT, KEYWORDS_DESC,
    KEYWORDS_CREDIT, KEYWORDS_DEBIT, KEYWORDS_DIRECTION,
    DECIMAL_COMMA_INDICATORS
)

def heuristic_map_columns(headers: List[str]) -> ColumnMapping:
    """
    Robust pure-Python heuristic fallback for column mapping.
    Determines Date, Amount, Description, and Polarity based on keyword matching.
    """
    # Create mapping of "clean lower" -> original
    # We iterate over original headers but match against clean lower
    header_map = {h.strip().lower(): h for h in headers}
    lower_headers = list(header_map.keys())
    # But we need to preserve order usually for fallback indices. 
    # Actually, let's just use a list of (cleaned, original) for iteration
    clean_pairs = [(h.strip().lower(), h.strip()) for h in headers]
    cleaned_headers = [p[0] for p in clean_pairs]
    original_headers = [p[1] for p in clean_pairs]

    def find_col(keywords: List[str], default_idx: int) -> str:
        for keyword in keywords:
            for i, h in enumerate(cleaned_headers):
                if keyword in h:
                    return original_headers[i]
        # Default
        try:
            return original_headers[default_idx]
        except IndexError:
            return original_headers[0] if original_headers else "Unknown"

    # 1. Identify key columns
    date_col = find_col(KEYWORDS_DATE, 0)
    desc_col = find_col(KEYWORDS_DESC, -1) # Default to last for desc
    
    # 2. Polarity Logic
    polarity = None
    amount_col = None
    
    # Helper
    def _matches_any(text: str, keywords: List[str]) -> bool:
        return any(k in text for k in keywords)

    # Check for Credit/Debit columns (Case C)
    # Exclude 'card' to avoid "Credit Card Number" or "Debit Card ID"
    credit_idx = next((i for i, h in enumerate(cleaned_headers) if _matches_any(h, KEYWORDS_CREDIT) and 'card' not in h), -1)
    debit_idx = next((i for i, h in enumerate(cleaned_headers) if _matches_any(h, KEYWORDS_DEBIT) and 'card' not in h), -1)
    
    if credit_idx != -1 and debit_idx != -1:
        polarity = PolarityCaseC(
            credit_col=original_headers[credit_idx],
            debit_col=original_headers[debit_idx]
        )
    else:
        # Look for Amount
        amount_col = find_col(KEYWORDS_AMOUNT, 1) # Default to 2nd col usually
        
        # Check for Direction column (Case B)
        dir_idx = next((i for i, h in enumerate(cleaned_headers) if _matches_any(h, KEYWORDS_DIRECTION)), -1)
        
        if dir_idx != -1:
             polarity = PolarityCaseB(
                direction_col=original_headers[dir_idx],
                outgoing_value='Debit',
                incoming_value='Credit'
            )
        else:
            polarity = PolarityCaseA()

    # 3. Decimal Separator
    # If we see German/Dutch words, assume Comma.
    all_text = " ".join(cleaned_headers)
    decimal_sep = DecimalSeparator.DOT
    if any(k in all_text for k in DECIMAL_COMMA_INDICATORS):
        decimal_sep = DecimalSeparator.COMMA
        
    return ColumnMapping(
        date_col=date_col,
        amount_col=amount_col,
        desc_col=desc_col,
        decimal_separator=decimal_sep,
        polarity=polarity
    )
