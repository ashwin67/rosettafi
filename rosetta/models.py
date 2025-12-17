from enum import Enum
from typing import Optional, Union, Literal, List, Dict
from pydantic import BaseModel, Field

# --- Schema Models (Existing) ---
class DecimalSeparator(str, Enum):
    DOT = "."
    COMMA = ","

class PolarityCaseA(BaseModel):
    type: Literal["signed"] = "signed"

class PolarityCaseB(BaseModel):
    type: Literal["direction"] = "direction"
    direction_col: str = Field(..., description="The column containing the direction indicator")
    outgoing_value: str = Field(..., description="Value indicating money leaving the account")
    incoming_value: str = Field(..., description="Value indicating money entering the account")

class PolarityCaseC(BaseModel):
    type: Literal["credit_debit"] = "credit_debit"
    credit_col: str = Field(..., description="Column for money entering (Credit)")
    debit_col: str = Field(..., description="Column for money leaving (Debit)")

class ColumnMapping(BaseModel):
    date_col: str = Field(..., description="The column name for date.")
    amount_col: Optional[str] = Field(None, description="The column name for amount.")
    desc_col: str = Field(..., description="The column name for description.")
    decimal_separator: DecimalSeparator = Field(..., description="Decimal separator.")
    polarity: Union[PolarityCaseA, PolarityCaseB, PolarityCaseC] = Field(..., description="Polarity logic.")

# --- NEW: Entity Models (The Phonebook) ---

class ContextRule(BaseModel):
    """Rule to override category based on specific keywords in the description."""
    contains_keyword: str = Field(..., description="Keyword to look for (e.g. 'AWS', 'To Go').")
    assign_category: str = Field(..., description="Category to assign if keyword is found.")

class MerchantEntity(BaseModel):
    """Represents a canonical real-world entity (e.g. 'Albert Heijn')."""
    id: str = Field(..., description="Unique slug ID (e.g. 'albert_heijn').")
    canonical_name: str = Field(..., description="The clean display name.")
    aliases: List[str] = Field(default_factory=list, description="List of raw variations mapped to this entity.")
    default_category: str = Field(..., description="The default category for this merchant.")
    rules: List[ContextRule] = Field(default_factory=list, description="Context-specific overrides.")

class TokenizedParts(BaseModel):
    parts: List[str] = Field(..., description="The list of segmented substrings from the text.")

class BatchResult(BaseModel):
    results: List[TokenizedParts]