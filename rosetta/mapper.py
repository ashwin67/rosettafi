import pandas as pd
import instructor
from openai import OpenAI
import json
import hashlib
import os
from .models import ColumnMapping, PolarityCaseA, PolarityCaseB, PolarityCaseC, DecimalSeparator
from .config import get_logger
from .workspace import Workspace

logger = get_logger(__name__)

workspace = Workspace()
CONFIG_FILE = workspace.get_bank_config_path()

def get_column_mapping(df: pd.DataFrame, confirm_mapping: bool = False) -> ColumnMapping:
    """
    Uses LLM (Ollama) to generate a ColumnMapping configuration.
    """
    logger.info("Stage 2: Determining Column Mapping & Logic...")
    
    raw_headers = list(df.columns)
    logger.info(f"Raw headers found: {raw_headers}")

    # 1. Check for Persistent Config
    headers_str = str(raw_headers)
    header_hash = hashlib.md5(headers_str.encode()).hexdigest()

    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                all_configs = json.load(f)
                if header_hash in all_configs:
                    logger.info(f"Found persistent config for hash {header_hash}. Loading...")
                    return ColumnMapping(**all_configs[header_hash])
        except Exception as e:
            logger.warning(f"Failed to load persistent config: {e}")
    
    client = instructor.from_openai(
        OpenAI(
            base_url="http://localhost:11434/v1",
            api_key="ollama", 
        ),
        mode=instructor.Mode.JSON,
    )

    mapping = None
    try:
        mapping = client.chat.completions.create(
            model="llama3.2", 
            messages=[
                {
                    "role": "user",
                    "content": f"""
                    You are a data engineering assistant. 
                    Given these file headers from a bank export: {raw_headers}
                    
                    Analyze the CSV structure to determine:
                    1. The columns for Date, Amount, and Description.
                       - Date keywords: 'Date', 'Datum', 'Transactiedatum'
                       - Amount keywords: 'Amount', 'Bedrag', 'Transactiebedrag', 'Debit', 'Credit'
                       - Description keywords: 'Description', 'Omschrijving', 'Mededelingen', 'Naam'
                    2. The Decimal Separator (Comma ',' or Dot '.'). European formats often use comma.
                    3. The Polarity Logic (How to distinguish income vs expense).
                       - Case A: One 'Amount' column with signed values (e.g. -50.00).
                       - Case B: One 'Amount' column + a 'Direction' column (e.g. Credit/Debit words).
                       - Case C: Separate 'Credit' and 'Debit' value columns.
                    
                    Return a valid JSON object instance matching the ColumnMapping schema.
                    IMPORTANT: Do NOT return the JSON Schema definition. Return the actual mapping data.
                    """
                }
            ],
            response_model=ColumnMapping,
            max_retries=1 
        )
        logger.info(f"LLM Mapping result: {mapping}")
    
    except Exception as e:
        logger.warning(f"Could not connect to Ollama or failed generation: {e}.")
    
    if mapping is None:
        logger.warning("!!! USING FALLBACK MOCK (LLM Failed) !!!")
        mapping = create_fallback_mapping(raw_headers)
        logger.info(f"Fallback Mapping result: {mapping}")
        
    if mapping:
        mapping.date_col = mapping.date_col.strip()
        mapping.desc_col = mapping.desc_col.strip()
        if mapping.amount_col:
            mapping.amount_col = mapping.amount_col.strip()
            
        # Strip whitespace from polarity fields if applicable
        if mapping.polarity.type == 'direction':
            mapping.polarity.direction_col = mapping.polarity.direction_col.strip()
        elif mapping.polarity.type == 'credit_debit':
            mapping.polarity.credit_col = mapping.polarity.credit_col.strip()
            mapping.polarity.debit_col = mapping.polarity.debit_col.strip()
        
        # Validation Logic: Case A Requires amount_col
        if mapping.polarity.type == 'signed' and not mapping.amount_col:
            # Fallback for amount column finding if LLM missed it
            # We re-run heuristic logic just for this
             lower_headers = [h.lower() for h in raw_headers]
             amount_idx = next((i for i, h in enumerate(lower_headers) if any(x in h for x in ['amount', 'betrag', 'eur'])), -1)
             if amount_idx != -1:
                mapping.amount_col = raw_headers[amount_idx]
             else:
                mapping.amount_col = raw_headers[1] if len(raw_headers)>1 else raw_headers[0]

        # Interactive Confirmation & Persistence
        save_decision = True
        if confirm_mapping:
            print("\n--- Proposed Mapping ---")
            print(mapping.model_dump_json(indent=2))
            try:
                user_input = input("Accept this mapping? (Y/n): ").strip().lower()
                if user_input == 'n':
                    save_decision = False
                    logger.info("User rejected mapping. Not saving to persistent config.")
            except EOFError:
                pass

        if save_decision:
             try:
                all_configs = {}
                if os.path.exists(CONFIG_FILE):
                    with open(CONFIG_FILE, 'r') as f:
                        all_configs = json.load(f)
                
                all_configs[header_hash] = mapping.model_dump()
                
                with open(CONFIG_FILE, 'w') as f:
                    json.dump(all_configs, f, indent=4)
                logger.info("Saved mapping to persistent config.")
             except Exception as e:
                logger.warning(f"Failed to save config: {e}")
                
    return mapping

def create_fallback_mapping(headers: list[str]) -> ColumnMapping:
    """
    Heuristics to generate a mapping if LLM fails.
    """
    lower_headers = [h.lower() for h in headers]
    
    # 1. Identify key columns
    date_col = next((h for h in headers if any(x in h.lower() for x in ['date', 'datum'])), headers[0])
    desc_col = next((h for h in headers if any(x in h.lower() for x in ['text', 'desc', 'book', 'narr', 'omschrijving', 'mededelingen', 'naam', 'name'])), headers[-1])
    
    # 2. Check for Credit/Debit columns (Case C)
    credit_idx = next((i for i, h in enumerate(lower_headers) if 'credit' in h and 'card' not in h), -1)
    debit_idx = next((i for i, h in enumerate(lower_headers) if 'debit' in h and 'card' not in h), -1)
    
    polarity = None
    amount_col = None
    
    if credit_idx != -1 and debit_idx != -1:
        # Case C
        polarity = PolarityCaseC(
            credit_col=headers[credit_idx],
            debit_col=headers[debit_idx]
        )
    else:
        # Look for Amount
        amount_idx = next((i for i, h in enumerate(lower_headers) if any(x in h for x in ['amount', 'betrag', 'eur', 'bedrag'])), -1)
        if amount_idx != -1:
            amount_col = headers[amount_idx]
        else:
             # Last resort
             amount_col = headers[1] if len(headers)>1 else headers[0]
        
        # Check for Direction column (Case B)
        dir_idx = next((i for i, h in enumerate(lower_headers) if any(x in h for x in ['cd', 'c/d', 'direction', 'type'])), -1)
        if dir_idx != -1:
            polarity = PolarityCaseB(
                direction_col=headers[dir_idx],
                outgoing_value='Debit', # Safe guesses?
                incoming_value='Credit'
            )
        else:
            # Default to Case A (Signed)
            polarity = PolarityCaseA()
            if amount_col is None:
                # If we somehow missed it, default to 2nd column or fail
                amount_col = headers[1] if len(headers) > 1 else headers[0]

    # 3. Decimal Separator Guess: Default to Dot for fallback, or maybe Comma if german words found?
    # Simple heuristic
    decimal_sep = DecimalSeparator.DOT
    if any(x in "".join(lower_headers) for x in ['betrag', 'valuta', 'buchung', 'bedrag']):
        decimal_sep = DecimalSeparator.COMMA
        
    return ColumnMapping(
        date_col=date_col,
        amount_col=amount_col,
        desc_col=desc_col,
        decimal_separator=decimal_sep,
        polarity=polarity
    )
