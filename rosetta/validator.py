import pandas as pd
import pandera as pa
from .config import get_logger

logger = get_logger(__name__)

# Pandera Schema for the Final Target Output
TargetSchema = pa.DataFrameSchema({
    "transaction_id": pa.Column(str, checks=pa.Check(lambda x: len(str(x)) > 0)), # UUID as string
    "date": pa.Column(pa.DateTime),
    "account": pa.Column(str),
    "amount": pa.Column(float), 
    "currency": pa.Column(str, checks=pa.Check.isin(["EUR", "USD", "GBP", "JPY"])),
    "price": pa.Column(float, nullable=True),
    "meta": pa.Column(object), # Store JSON or Dict
})

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates the DataFrame against the TargetSchema.
    """
    logger.info("Stage 3: Validating data...")
    try:
        validated_df = TargetSchema.validate(df)
        logger.info("Validation successful!")
        return validated_df
    except pa.errors.SchemaError as e:
        logger.error(f"Schema Validation Failed: {e}")
        # In a real app, send to 'quarantine' queue
        raise e
