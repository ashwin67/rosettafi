from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
import uuid
import base64
from pathlib import Path
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import pandas as pd

# Rosetta V2 Imports
from rosetta.pipeline import RosettaPipeline
from rosetta.data.constants import UNKNOWN_CATEGORY

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
PIPELINE = RosettaPipeline("rosetta_v2.db")
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
        }
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
        
    # 1. Active Learning: Update model with user feedback
    if feedback:
        # Map frontend keys to pipeline keys
        mapped_feedback = []
        for f in feedback:
            mapped_feedback.append({
                "entity": f.get('name'),
                "category": f.get('category'),
                "cleaned_description": f.get('raw')
            })
            
        # Update knowledge & retrain SetFit immediately
        PIPELINE.update_knowledge(mapped_feedback)
        
        # Re-evaluate remaining review items with updated model
        remaining = session["needs_review"]
        # Skip items that were just labeled (using cleaned_description as key)
        labeled_keys = {f.get('raw') for f in feedback if f.get('raw')}
        still_needs_review = [item for item in remaining if item.get('cleaned_description') not in labeled_keys]
        
        if still_needs_review:
            # Re-predict using updated SetFit model
            texts = [item['cleaned_description'] for item in still_needs_review]
            predictions = PIPELINE.categorizer.predict(texts)
            
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
            
            session["processed"].extend(new_processed)
            session["needs_review"] = new_review
            
    # 2. Return state
    total_rows = len(session["processed"]) + len(session["needs_review"])
    processed_rows = len(session["processed"])
    
    if session["needs_review"]:
        # Return next batch of 5 items
        batch = session["needs_review"][:5]
        
        # Format for frontend expectations
        formatted_unknowns = []
        for item in batch:
            formatted_unknowns.append({
                "raw": item.get('cleaned_description'),
                "original_examples": [item.get(session['mapping']['desc_col'])],
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
            "auto_categorized_in_batch": 0 # For now, as we re-process everything at once
        }
    else:
        # 3. Finalize: Generate Ledger Splits
        ledger_df = PIPELINE.finalize_ledger(session["processed"], session["mapping"])
        
        # Cleanup session
        if session_id in SESSION_STORE:
            del SESSION_STORE[session_id]
        
        # Replace NaN with None for valid JSON output
        ledger_df = ledger_df.where(pd.notnull(ledger_df), None)
        ledger_data = ledger_df.to_dict(orient='records')
        
        return {
            "status": "completed",
            "data": ledger_data,
            "total_rows": total_rows,
            "processed_rows": processed_rows
        }
