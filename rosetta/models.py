from enum import Enum
from typing import Optional, Union, Literal
from pydantic import BaseModel, Field

class DecimalSeparator(str, Enum):
    DOT = "."
    COMMA = ","

class PolarityCaseA(BaseModel):
    """Case A (Signed): The amount column already contains negative/positive values."""
    type: Literal["signed"] = "signed"

class PolarityCaseB(BaseModel):
    """Case B (Direction Column): A separate column dictates polarity."""
    type: Literal["direction"] = "direction"
    direction_col: str = Field(..., description="The column containing the direction indicator (e.g. 'Debit/Credit')")
    outgoing_value: str = Field(..., description="Value indicating money leaving the account (e.g. 'Debit', 'Expenditure')")
    incoming_value: str = Field(..., description="Value indicating money entering the account (e.g. 'Credit', 'Income')")

class PolarityCaseC(BaseModel):
    """Case C (Credit/Debit Columns): Separate columns for money in and money out."""
    type: Literal["credit_debit"] = "credit_debit"
    credit_col: str = Field(..., description="Column for money entering (Credit)")
    debit_col: str = Field(..., description="Column for money leaving (Debit)")

class ColumnMapping(BaseModel):
    """Mapping from raw file headers to standard schema fields with parsing rules."""
    date_col: str = Field(..., description="The column name in the file that corresponds to the transaction date.")
    amount_col: Optional[str] = Field(None, description="The column name for amount. Required for Signed or Direction cases. Ignored for Credit/Debit case.")
    desc_col: str = Field(..., description="The column name in the file that corresponds to the description or narration.")
    decimal_separator: DecimalSeparator = Field(..., description="The character used as the decimal separator in numbers.")
    polarity: Union[PolarityCaseA, PolarityCaseB, PolarityCaseC] = Field(..., description="The logic to determine if a transaction is income or expense.")
