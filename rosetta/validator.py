import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors
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
        validated_df = TargetSchema.validate(df, lazy=True)
        logger.info("Validation successful!")
        return validated_df
    except SchemaErrors as e:
        logger.warning(f"Validation found {len(e.failure_cases)} errors.")
        
        # Separate Clean vs Quarantine
        # e.failure_cases is a DataFrame.
        # Get the index of failure cases.
        bad_indices = e.failure_cases['index'].unique()
        
        clean_df = df.drop(index=bad_indices)
        quarantine_df = df.loc[bad_indices]
        
        logger.info(f"Quarantined {len(quarantine_df)} rows. {len(clean_df)} clean rows proceeding.")
        
        # Save Quarantine
        quarantine_filename = "quarantine_data.csv"
        try:
            quarantine_df.to_csv(quarantine_filename, index=False)
            logger.info(f"Saved quarantined rows to {quarantine_filename}")
        except Exception as io_err:
            logger.error(f"Failed to save quarantine file: {io_err}")
            
        return clean_df
