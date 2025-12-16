import pandas as pd
import instructor
from openai import OpenAI
import json
import hashlib
import os
from typing import Optional, List

from .models import ColumnMapping, PolarityCaseA, PolarityCaseB, PolarityCaseC, DecimalSeparator
from rosetta.utils import get_logger
from .workspace import Workspace
# Removed external logic import
from .data.constants import (
    LLM_MODEL_NAME, LLM_BASE_URL, LLM_API_KEY,
    MAPPER_SYSTEM_PROMPT, MAPPER_USER_PROMPT_TEMPLATE,
    KEYWORDS_DATE, KEYWORDS_AMOUNT, KEYWORDS_DESC,
    KEYWORDS_CREDIT, KEYWORDS_DEBIT, KEYWORDS_DIRECTION,
    DECIMAL_COMMA_INDICATORS
)

logger = get_logger(__name__)

workspace = Workspace()
CONFIG_FILE = workspace.get_bank_config_path()

def get_column_mapping(df: pd.DataFrame, confirm_mapping: bool = False) -> ColumnMapping:
    """
    Determines the column mapping and logic for the provided DataFrame.
    Uses a 2-step approach:
    1. Check for persistent config based on header hash.
    2. If not found, attempt LLM generation.
    3. If LLM fails, fall back to robust heuristics.
    """
    logger.info("Stage 2: Determining Column Mapping & Logic...")
    
    # Preprocess headers: strip whitespace
    raw_headers = [str(h).strip() for h in df.columns]
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
    
    # 2. LLM Generation
    mapping: Optional[ColumnMapping] = None
    try:
        mapping = _get_llm_mapping(raw_headers)
    except Exception as e:
        logger.error(f"LLM Mapping failed: {e}")

    # 3. Fallback / Validation
    if mapping is None:
        logger.warning("!!! USING FALLBACK MOCK (LLM Failed) !!!")
        # Use extracted logic from rosetta.logic.mapper_logic
        mapping = heuristic_map_columns(raw_headers)
        logger.info(f"Fallback Mapping result: {mapping}")
    
    # Post-processing cleanups on the mapping object
    if mapping:
        mapping.date_col = mapping.date_col.strip()
        mapping.desc_col = mapping.desc_col.strip()
        if mapping.amount_col:
            mapping.amount_col = mapping.amount_col.strip()
            
        # Strip whitespace from polarity fields
        if mapping.polarity.type == 'direction':
            mapping.polarity.direction_col = mapping.polarity.direction_col.strip()
        elif mapping.polarity.type == 'credit_debit':
            mapping.polarity.credit_col = mapping.polarity.credit_col.strip()
            mapping.polarity.debit_col = mapping.polarity.debit_col.strip()

        # Sanity check: Ensure Amount column exists for signed transactions
        if mapping.polarity.type == 'signed' and not mapping.amount_col:
             logger.warning("LLM returned Signed polarity but no Amount column. Fixing...")
             fallback = heuristic_map_columns(raw_headers)
             mapping.amount_col = fallback.amount_col

    # 4. Interactive Confirmation & Persistence
    if _handle_persistence(mapping, header_hash, confirm_mapping):
        logger.info("Mapping saved.")
    else:
        logger.info("Mapping not saved (User rejected or error).")

    return mapping

def _get_llm_mapping(headers: List[str]) -> ColumnMapping:
    """Calls Ollama via Instructor to get the mapping."""
    client = instructor.from_openai(
        OpenAI(base_url=LLM_BASE_URL, api_key=LLM_API_KEY),
        mode=instructor.Mode.JSON,
    )

    user_content = MAPPER_USER_PROMPT_TEMPLATE.format(
        headers=headers,
        date_keywords=KEYWORDS_DATE,
        amount_keywords=KEYWORDS_AMOUNT,
        desc_keywords=KEYWORDS_DESC
    )

    return client.chat.completions.create(
        model=LLM_MODEL_NAME, 
        messages=[
            {"role": "system", "content": MAPPER_SYSTEM_PROMPT},
            {"role": "user", "content": user_content}
        ],
        response_model=ColumnMapping,
        max_retries=1 
    )

def _handle_persistence(mapping: ColumnMapping, header_hash: str, confirm: bool) -> bool:
    """Handles user confirmation and saving to disk."""
    save_decision = True
    if confirm:
        print("\n--- Proposed Mapping ---")
        print(mapping.model_dump_json(indent=2))
        try:
            user_input = input("Accept this mapping? (Y/n): ").strip().lower()
            if user_input == 'n':
                save_decision = False
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
            return True
        except Exception as e:
            logger.warning(f"Failed to save config: {e}")
            return False
    return False

def heuristic_map_columns(headers: List[str]) -> ColumnMapping:
    """
    Robust pure-Python heuristic fallback for column mapping.
    Determines Date, Amount, Description, and Polarity based on keyword matching.
    """
    # Create mapping of "clean lower" -> original
    # We iterate over original headers but match against clean lower
    header_map = {h.strip().lower(): h for h in headers}
    # lower_headers = list(header_map.keys()) # Unused
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
