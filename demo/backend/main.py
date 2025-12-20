# To run this server, use the following command from the root project directory:
# PYTHONPATH=. uvicorn demo.backend.main:app --reload

from fastapi import FastAPI, File, UploadFile, Body
from fastapi.middleware.cors import CORSMiddleware
import shutil
from pathlib import Path
import pandas as pd
import uuid
from typing import List, Optional

# Rosetta Imports
from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
from rosetta.rules import RulesEngine
from rosetta.validator import validate_data
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.logic.ledger import LedgerEngine

app = FastAPI()

# Allow CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory storage for demo purposes
SESSION_STORE = {}
CONFIDENCE_THRESHOLD = 0.9
BATCH_SIZE = 5

from pydantic import BaseModel

import base64

class FileUploadRequest(BaseModel):
    filename: str
    content: str
    encoding: str # 'text' or 'base64'

@app.post("/upload")
async def initialize_session(request: FileUploadRequest):
    """
    Initializes a new categorization session.
    - Creates a session ID.
    - Runs the initial ETL (Sniff, Map, Rules).
    - Stores the normalized DataFrame and a new Categorizer instance.
    - Returns the session_id to the client.
    """
    session_id = str(uuid.uuid4())
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / request.filename
    
    # Write content based on encoding
    if request.encoding == 'base64':
        # The content is a data URL like "data:application/vnd.ms-excel;base64,..."
        # We need to strip the prefix and decode.
        header, encoded = request.content.split(",", 1)
        data = base64.b64decode(encoded)
        with open(file_path, "wb") as f:
            f.write(data)
    else: # text
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(request.content)
    
    # 1. Sniffer, 2. Mapper, 3. Rules Engine
    clean_df = sniff_header_row(str(file_path))
    mapping = get_column_mapping(clean_df)
    engine = RulesEngine(mapping)
    normalized_df = engine.apply(clean_df)

    categorizer = CategorizationEngine()
    normalized_df = categorizer._prepare_df(normalized_df, "description")

    # Initialize session
    SESSION_STORE[session_id] = {
        "categorizer": categorizer,
        "df": normalized_df,
        "index": 0,
        "description_col": "description",
        "noise_words": set(),
    }
    
    return {"session_id": session_id}


import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.post("/interactive-categorize")
async def interactive_categorize_session(payload: dict = Body(...)):
    """
    The main endpoint for the interactive categorization loop.
    - Handles user feedback from the previous step.
    - Finds the next batch of items that need manual categorization.
    - Skips items that can be auto-categorized with high confidence.
    """
    session_id = payload.get('session_id')
    user_feedback = payload.get('feedback', [])
    session = SESSION_STORE.get(session_id)

    if not session:
        return {"status": "error", "message": "Invalid session ID."}

    categorizer = session["categorizer"]
    df = session["df"]
    
    # 1. Learn from user feedback (for both entities and noise)
    if user_feedback:
        logger.info(f"Registering user feedback: {user_feedback}")
        for item in user_feedback:
            # Register the entity mapping
            categorizer.register_entity(item['name'], item['category'], alias=item['raw'])
            
            # Learn noise words
            raw_tokens = set(item['raw'].split())
            name_tokens = set(item['name'].lower().split())
            new_noise = raw_tokens - name_tokens
            session["noise_words"].update(new_noise)
        
        logger.info(f"Updated noise list: {session['noise_words']}")
        # Re-clean the entire dataframe with the new noise list
        df = categorizer._prepare_df(df, session["description_col"], session["noise_words"])
        session["df"] = df

    # 2. Find the next batch that needs categorization
    items_to_categorize = []
    auto_categorized_in_batch = 0
    
    logger.info("--- Starting new categorization batch ---")
    while session["index"] < len(df) and len(items_to_categorize) < BATCH_SIZE:
        current_row = df.iloc[session["index"]]
        # Use the cleaned description for all logic
        desc = current_row['merchant_clean']
        
        logger.info(f"Processing row {session['index']}: '{desc}'")

        # Check if already categorized by a previous run
        existing_entity = categorizer.resolver.resolve(desc)
        if existing_entity and existing_entity.default_category != "Uncategorized":
            logger.info(f"--> Found existing entity: '{existing_entity.canonical_name}'")
            df.at[session["index"], 'Entity'] = existing_entity.canonical_name
            df.at[session["index"], 'Category'] = existing_entity.default_category
            df.at[session["index"], 'confidence'] = 1.0
            session["index"] += 1
            auto_categorized_in_batch += 1
            continue

        # Check for high-confidence matches
        matches = categorizer.resolver.find_similar(desc, top_n=3)
        logger.info(f"--> Similarity matches: {matches}")
        
        suggestion_name = None
        suggestion_category = None
        suggestion_confidence = 0.0

        if matches:
            alias, score = matches[0]
            entity_id = categorizer.phonebook.alias_index.get(alias)
            if entity_id:
                entity = categorizer.phonebook.entities[entity_id]
                
                # Check for high confidence auto-categorization
                if score >= CONFIDENCE_THRESHOLD:
                    logger.info(f"--> High confidence match found: '{desc}' -> '{entity.canonical_name}' (Score: {score})")
                    df.at[session["index"], 'Entity'] = entity.canonical_name
                    df.at[session["index"], 'Category'] = entity.default_category
                    df.at[session["index"], 'confidence'] = score
                    session["index"] += 1
                    auto_categorized_in_batch += 1
                    continue
                
                # Otherwise, prepare suggestion metadata
                suggestion_name = entity.canonical_name
                suggestion_category = entity.default_category
                suggestion_confidence = score
        
        # If we reach here, it's a low-confidence item
        logger.info(f"--> Low confidence. Adding to manual review queue.")
        items_to_categorize.append({
            "raw": desc, # This is the cleaned name, for user feedback
            "original_examples": [current_row[session["description_col"]]], # Show original for context
            "suggested_name": suggestion_name,
            "suggested_category": suggestion_category,
            "confidence": round(suggestion_confidence, 2)
        })
        session["index"] += 1

    # 3. Decide what to return
    if items_to_categorize:
        return {
            "status": "pending_categorization",
            "unknowns": items_to_categorize,
            "categories": list(categorizer.phonebook.get_all_categories()),
            "total_rows": len(df),
            "processed_rows": session["index"],
            "auto_categorized_in_batch": auto_categorized_in_batch,
        }
    else:
        # No more items to categorize, finalize the process
        df['account'] = df['Category']
        ledger_engine = LedgerEngine()
        ledger_df = ledger_engine.generate_splits(df)
        final_df = validate_data(ledger_df)

        # Clean up session
        del SESSION_STORE[session_id]
        
        # Replace NaN with None for valid JSON output
        final_df = final_df.where(pd.notnull(final_df), None)

        return {
            "status": "completed",
            "data": final_df.to_dict(orient='records')
        }

