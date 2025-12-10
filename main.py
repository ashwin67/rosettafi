import pandas as pd
import pandera as pa
import instructor
from pydantic import BaseModel, Field
from openai import OpenAI
import io
import datetime
import uuid
import json
import logging
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Universal Data Model & Schemas ---

class ColumnMapping(BaseModel):
    """Mapping from raw file headers to standard schema fields."""
    date_col: str = Field(..., description="The column name in the file that corresponds to the transaction date.")
    amount_col: str = Field(..., description="The column name in the file that corresponds to the transaction amount.")
    desc_col: str = Field(..., description="The column name in the file that corresponds to the description or narration.")

# Pandera Schema for the Final Target Output
# Note: The prompt asks for 1 row = 1 Split. 
# Columns: transaction_id, date, account, amount, currency, price, meta
TargetSchema = pa.DataFrameSchema({
    "transaction_id": pa.Column(str, checks=pa.Check(lambda x: len(str(x)) > 0)), # UUID as string
    "date": pa.Column(pa.DateTime),
    "account": pa.Column(str),
    "amount": pa.Column(float), # Using float for simplicity in MVP, Decimal preferred for prod
    "currency": pa.Column(str, checks=pa.Check.isin(["EUR", "USD", "GBP", "JPY"]), default="EUR"),
    "price": pa.Column(float, nullable=True),
    "meta": pa.Column(object), # Store JSON or Dict
})

# --- Stage 1: The Sniffer ---

def sniff_header_row(file_path_or_buffer) -> pd.DataFrame:
    """
    Reads the first 20 rows of a file to heuristically identify the valid header row.
    Returns a DataFrame loaded with the correct header.
    """
    logger.info("Stage 1: Sniffing for header...")
    
    # Read first 20 lines without header
    if isinstance(file_path_or_buffer, str):
        # Determine if file path or string content
        try:
            with open(file_path_or_buffer, 'r') as f:
                lines = [next(f) for _ in range(20)]
        except Exception:
             # Treat as string buffer
            lines = file_path_or_buffer.splitlines()[:20]
            file_path_or_buffer = io.StringIO(file_path_or_buffer)
    elif isinstance(file_path_or_buffer, io.StringIO):
         lines = file_path_or_buffer.getvalue().splitlines()[:20]
         file_path_or_buffer.seek(0)
    else:
        # Fallback for other file-like objects
        lines = [line.decode('utf-8') for line in file_path_or_buffer.readlines()[:20]]
        file_path_or_buffer.seek(0)

    keywords = ['date', 'booking', 'transaction', 'amount', 'debit', 'credit', 'description', 'memo', 'payee', 'valuta']
    
    best_row_idx = 0
    max_score = -1

    for idx, line in enumerate(lines):
        score = 0
        line_lower = line.lower()
        for kw in keywords:
            if kw in line_lower:
                score += 1
        
        # Heuristic: Valid headers usually have delimiters (comma/semicolon)
        if ',' in line or ';' in line:
            score += 1
            
        if score > max_score:
            max_score = score
            best_row_idx = idx

    logger.info(f"Identified header at row index: {best_row_idx} (Score: {max_score})")
    
    # Reload dataframe with correct header
    # We manually slice the lines to avoid ambiguities with read_csv's header/skip_blank_lines logic
    clean_content = "\n".join(lines[best_row_idx:] + lines[20:]) # Join header+rest (we only read 20 lines initially? Wait, we need the whole file)
    
    # Re-reading the whole file logic properly
    if isinstance(file_path_or_buffer, (str, io.StringIO)):
        if isinstance(file_path_or_buffer, str) and not file_path_or_buffer.endswith('.csv') and not '\n' in file_path_or_buffer:
             # Actual file path
             with open(file_path_or_buffer, 'r') as f:
                 all_lines = f.readlines()
        else:
             # String content or StringIO
             if isinstance(file_path_or_buffer, str):
                 file_path_or_buffer = io.StringIO(file_path_or_buffer)
             else:
                 file_path_or_buffer.seek(0)
             all_lines = file_path_or_buffer.readlines()
             
    clean_content = "".join(all_lines[best_row_idx:])
    df = pd.read_csv(io.StringIO(clean_content))
    
    return df

# --- Stage 2: The Mapper ---

def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Uses LLM (Ollama) to map raw columns to standard columns.
    Returns a transformed DataFrame matching the universal schema.
    """
    logger.info("Stage 2: Mapping columns...")
    
    raw_headers = list(df.columns)
    logger.info(f"Raw headers found: {raw_headers}")
    
    client = instructor.from_openai(
        OpenAI(
            base_url="http://localhost:11434/v1",
            api_key="ollama",  # required, but unused
        ),
        mode=instructor.Mode.JSON,
    )

    mapping = None
    try:
        mapping = client.chat.completions.create(
            model="deepseek-r1:8b", 
            messages=[
                {
                    "role": "user",
                    "content": f"""
                    You are a data engineering assistant. 
                    Given these file headers from a bank export: {raw_headers}
                    
                    Identify the exact column names that correspond to:
                    1. Transaction Date
                    2. Amount (money)
                    3. Description (narration/details)
                    
                    Return a JSON object mapping.
                    """
                }
            ],
            response_model=ColumnMapping,
            max_retries=1 # Fail fast for demo
        )
        logger.info(f"LLM Mapping result: {mapping}")
    
    except Exception as e:
        logger.warning(f"Could not connect to Ollama or failed generation: {e}.")
    
    if mapping is None:
        logger.warning("!!! USING FALLBACK MOCK (LLM Failed) !!!")
        # Fallback Logic
        try:
            date_c = next((c for c in raw_headers if 'date' in c.lower()), raw_headers[0])
            amount_c = next((c for c in raw_headers if 'amount' in c.lower() or 'betrag' in c.lower() or 'eur' in c.lower()), raw_headers[1] if len(raw_headers)>1 else raw_headers[0])
            desc_c = next((c for c in raw_headers if 'text' in c.lower() or 'desc' in c.lower() or 'memo' in c.lower()), raw_headers[2] if len(raw_headers)>2 else raw_headers[0])
            
            mapping = ColumnMapping(date_col=date_c, amount_col=amount_c, desc_col=desc_c)
            logger.info(f"Fallback Mapping result: {mapping}")
        except Exception as e:
            logger.error(f"Fallback mapping also failed: {e}")
            raise e

    # Standardize DataFrame
    mapped_df = pd.DataFrame()
    
    # 1. Date
    mapped_df['date'] = pd.to_datetime(df[mapping.date_col], errors='coerce')
    
    # 2. Amount (Clean currency symbols if needed, assuming simple float for MVP)
    # Simple cleanup helper
    def clean_amount(x):
        if isinstance(x, str):
            x = x.replace('EUR', '').replace('$', '').replace(',', '.') # Naive cleanup
        return float(x)
    
    mapped_df['amount'] = df[mapping.amount_col].apply(clean_amount)
    
    # 3. Description -> Meta (preserve original) logic or as Account? 
    # Prompt says 'account' is "Assets:Bank:Chase", 'meta' is original description
    # We will map 'desc_col' to 'meta' content primarily, and set 'account' to default for now
    
    mapped_df['account'] = "Assets:Bank:Unknown" # Default for ingestion
    mapped_df['currency'] = "EUR"
    mapped_df['price'] = pd.Series([None] * len(df), dtype="float64")
    
    # Generate UUIDs
    mapped_df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    
    # Meta Field: Store original row as JSON
    # To do this efficienty, we convert the original row to dict
    mapped_df['meta'] = df.apply(lambda row: row.to_json(), axis=1) # Or specific description? Prompt: "Meta: JSON (Original description, raw text)"
    # Let's add the specific description column as well to meta or handle it? 
    # For now, let's keep it simple.
    
    return mapped_df

# --- Stage 3: The Validator ---

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

# --- Main Execution ---

if __name__ == "__main__":
    
    # 1. Create Mock Messy CSV
    mock_csv_data = """
    Bank of Antigravity - Account Statement
    Generated: 2023-10-27
    Account: 123-456-789
    
    Disclaimer: This is not legal advice.
    
    Transaction Date,Valuta Date,Booking Text,Amount EUR,Balance
    2023-10-01,2023-10-01,Supermarket Purchase,-50.20,1000.00
    2023-10-02,2023-10-02,Monthly Salary,3500.00,4500.00
    2023-10-05,2023-10-05,Coffee Shop,-4.50,4495.50
    """
    
    logger.info("Initializing Sniffer & Mapper Engine...")
    
    # Run Stage 1
    clean_df = sniff_header_row(mock_csv_data)
    print("\n--- Stage 1 Output (Sniffed DataFrame) ---")
    print(clean_df.head())
    
    # Run Stage 2
    mapped_df = map_columns(clean_df)
    print("\n--- Stage 2 Output (Mapped DataFrame) ---")
    print(mapped_df[['date', 'amount', 'transaction_id']].head())
    
    # Run Stage 3
    final_df = validate_data(mapped_df)
    print("\n--- Stage 3 Output (Validated & Standardized) ---")
    print(final_df.head())
    print("\nProcess Complete.")
