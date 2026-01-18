from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
import uuid
import base64
import os
import shutil
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import pandas as pd

# Rosetta V2 Imports
from rosetta.pipeline import RosettaPipeline
from rosetta.data.constants import UNKNOWN_CATEGORY
from rosetta.workspace import Workspace

app = FastAPI()

# Allow CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Persistent Pipeline & Session Store
DB_FILENAME = "rosetta_v2.db"
PIPELINE = RosettaPipeline(DB_FILENAME)
SESSION_STORE = {}

class FileUploadRequest(BaseModel):
    filename: str
    content: str
    encoding: str # 'text' or 'base64'

@app.post("/upload")
async def upload_file(request: FileUploadRequest):
    """
    Initializes a new session by processing the uploaded file through the V2 pipeline.
    """
    session_id = str(uuid.uuid4())
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    file_path = temp_dir / f"{session_id}_{request.filename}"
    
    # 1. Save file
    if request.encoding == 'base64':
        if "," in request.content:
            _, encoded = request.content.split(",", 1)
        else:
            encoded = request.content
        data = base64.b64decode(encoded)
        with open(file_path, "wb") as f:
            f.write(data)
    else:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(request.content)
            
    # 2. Run Pipeline (Vector Search Phase)
    results = PIPELINE.process_file(str(file_path))
    
    if results.get("status") == "error":
        return results

    # 3. Store in Session
    SESSION_STORE[session_id] = {
        "processed": results['processed'],
        "needs_review": results['needs_review'],
        "mapping": results['mapping'],
        "file_path": str(file_path)
    }
    
    return {
        "session_id": session_id,
        "summary": {
            "total": len(results['processed']) + len(results['needs_review']),
            "auto_processed": len(results['processed']),
            "needs_review": len(results['needs_review'])
        },
        "debug_mapping": results['mapping'] # Send mapping back for UI Logging
    }

@app.post("/interactive-categorize")
async def interactive_loop(payload: Dict = Body(...)):
    """
    The main interactive loop:
    - Receives user labels.
    - Updates knowledge (Vector DB + SetFit).
    - Returns the next batch of uncertain items.
    """
    session_id = payload.get('session_id')
    feedback = payload.get('feedback', []) # List of labeled items
    
    print(f"DEBUG: interactive-categorize for session {session_id}, feedback items: {len(feedback)}")
    
    session = SESSION_STORE.get(session_id)
    if not session:
        return {"status": "error", "message": "Session not found or expired."}
        
    auto_categorized_count = 0

    # 1. Active Learning: Update model with user feedback
    if feedback:
        # Map frontend keys to pipeline keys
        mapped_feedback = []
        feedback_map = {} # Quick lookup for moving manual items to processed
        
        for f in feedback:
            # For pipeline training
            mapped_feedback.append({
                "entity": f.get('name'),
                "category": f.get('category'),
                "cleaned_description": f.get('raw')
            })
            # For session update
            if f.get('raw'):
                feedback_map[f.get('raw')] = f
            
        # Update knowledge & retrain SetFit immediately
        PIPELINE.update_knowledge(mapped_feedback)
        
        # Split 'needs_review' into:
        # A. Items the user just labeled (move to processed)
        # B. Items still unknown (re-evaluate with new model)
        remaining = session["needs_review"]
        manual_processed = []
        still_needs_review = []
        
        for item in remaining:
            raw_desc = item.get('cleaned_description')
            if raw_desc in feedback_map:
                # Apply user label
                fb = feedback_map[raw_desc]
                item['entity'] = fb.get('name')
                item['account'] = fb.get('category')
                item['confidence'] = 1.0
                item['method'] = 'manual_user'
                manual_processed.append(item)
            else:
                still_needs_review.append(item)
        
        # Move manual items to processed immediately
        session["processed"].extend(manual_processed)
        
        # Re-evaluate the rest
        if still_needs_review:
            # Re-predict using updated SetFit model
            texts = [item['cleaned_description'] for item in still_needs_review]
            
            # TUNING: Threshold raised to 0.8 as requested for high precision
            predictions = PIPELINE.categorizer.predict(texts, threshold=0.8)
            
            new_processed = []
            new_review = []
            for item, pred in zip(still_needs_review, predictions):
                if pred['category']:
                    item['account'] = pred['category']
                    item['confidence'] = pred['confidence']
                    item['method'] = 'setfit_v2'
                    new_processed.append(item)
                else:
                    new_review.append(item)
            
            # Count how many were effectively auto-resolved this round
            auto_categorized_count = len(new_processed)
            
            session["processed"].extend(new_processed)
            session["needs_review"] = new_review
    
    # 2. Return state
    total_rows = len(session["processed"]) + len(session["needs_review"])
    processed_rows = len(session["processed"])
    
    if session["needs_review"]:
        # Return next batch of 5 items
        batch = session["needs_review"][:5]
        
        # Identify extraction columns
        mapping = session['mapping']
        date_col = mapping.get('date_col')
        amount_col = mapping.get('amount_col')
        desc_col = mapping.get('desc_col')

        # Format for frontend expectations
        formatted_unknowns = []
        for item in batch:
            formatted_unknowns.append({
                "raw": item.get('cleaned_description'),
                "original_examples": [item.get(desc_col)],
                "date": item.get(date_col),
                "amount": item.get(amount_col),
                "suggested_name": item.get('entity'),
                "suggested_category": item.get('account'),
                "confidence": item.get('confidence', 0.0)
            })
            
        return {
            "status": "pending_categorization",
            "unknowns": formatted_unknowns,
            "categories": [r[0] for r in PIPELINE.db.conn.execute("SELECT DISTINCT default_category FROM merchants").fetchall() if r[0]],
            "total_rows": total_rows,
            "processed_rows": processed_rows,
            "auto_categorized_in_batch": auto_categorized_count # Explicitly return count
        }
    else:
        # 3. Finalize: Generate Ledger Splits
        ledger_df = PIPELINE.finalize_ledger(session["processed"], session["mapping"])
        
        # Cleanup session
        if session_id in SESSION_STORE:
            del SESSION_STORE[session_id]
        
        # Replace NaN with None
        ledger_df = ledger_df.where(pd.notnull(ledger_df), None)
        ledger_data = ledger_df.to_dict(orient='records')
        
        # 4. ENRICHMENT: Inject 'short_description' (Entity) back into ledger data
        id_to_entity = {}
        for item in session["processed"]:
            if item.get('id') and item.get('entity'):
                id_to_entity[item['id']] = item['entity']
        
        for row in ledger_data:
            tid = row.get('transaction_id')
            if tid and tid in id_to_entity:
                row['short_description'] = id_to_entity[tid]
            else:
                row['short_description'] = ""
        
        return {
            "status": "completed",
            "data": ledger_data,
            "total_rows": total_rows,
            "processed_rows": processed_rows,
            "auto_categorized_in_batch": auto_categorized_count
        }

@app.post("/reset")
async def reset_state():
    """
    Clears all sessions, deletes the database, and wipes the cache.
    """
    global SESSION_STORE, PIPELINE
    
    print("WARNING: System Reset Requested.")
    
    # 1. Clear Memory
    SESSION_STORE.clear()
    
    # 2. Close and Remove DB
    try:
        if hasattr(PIPELINE, 'db') and hasattr(PIPELINE.db, 'close'):
            PIPELINE.db.close()
    except Exception as e:
        print(f"Warning closing DB: {e}")

    if os.path.exists(DB_FILENAME):
        try:
            os.remove(DB_FILENAME)
        except PermissionError:
             print("Warning: Could not delete DB file (locked?).")

    # 3. Wipe ~/.rosetta_cache (Configs and Memory)
    ws = Workspace()
    if os.path.exists(ws.base_path):
        try:
             shutil.rmtree(ws.base_path)
             # Re-create structure immediately so next calls don't fail
             ws._ensure_structure() 
        except Exception as e:
            print(f"Warning: Could not wipe cache: {e}")

    # 4. Re-initialize Pipeline (creates new empty DB)
    PIPELINE = RosettaPipeline(DB_FILENAME)

    return {"status": "reset_complete", "message": "System reset to clean state."}