import os
import json
import logging
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
import instructor
from openai import OpenAI

logger = logging.getLogger(__name__)

class SegmentedTransaction(BaseModel):
    id: int = Field(..., description="The ID of the input transaction.")
    keywords: List[str] = Field(..., description="List of technical tags, codes, numbers, and noise.")
    descriptions: List[str] = Field(..., description="List of meaningful proper nouns and descriptive text.")

class SegmentationBatch(BaseModel):
    items: List[SegmentedTransaction] = Field(..., description="List of segmented results corresponding to the input batch.")

class LLMSegmenter:
    """
    Handles Pass 1: Batch Segmentation.
    Splits transaction strings into 'keywords' (noise) and 'descriptions' (signal).
    """

    def __init__(self, client=None, model: str = "llama3.2"):
        self.client = client
        self.model = model
        # Ensure client is patched
        if self.client and not hasattr(self.client, "chat"):
             self.client = instructor.patch(self.client, mode=instructor.Mode.JSON)

    def segment_batch(self, texts: List[str], system_prompt: str) -> List[dict]:
        """
        Segments a batch of descriptions.
        Returns a list of dicts: [{'keywords': [], 'descriptions': []}, ...]
        Guarantees validation and alignment with input texts.
        """
        if not texts:
            return []

        # Filter out non-strings or empty but KEEP INDEX alignment
        # Actually, simpler to assign IDs to all inputs, let LLM skip nulls if it wants, 
        # but we demand checking logic.
        
        # Prepare inputs with explicit IDs
        inputs_with_ids = [{"id": i, "text": t} for i, t in enumerate(texts) if isinstance(t, str) and t.strip()]
        
        if not inputs_with_ids:
            return [{"keywords": [], "descriptions": []} for _ in texts]

        try:
            # We explicitly ask for a wrapper object to ensure list parsing works well
            resp = self.client.chat.completions.create(
                model=self.model,
                response_model=SegmentationBatch,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Here is the batch of {len(inputs_with_ids)} transactions:\n" + json.dumps(inputs_with_ids)}
                ]
            )
            
            # Map results by ID
            results_map = {item.id: item.model_dump() for item in resp.items}
            
            # Reconstruct ordered list matching original 'texts'
            final_results = []
            for i, text in enumerate(texts):
                if i in results_map:
                    # Found explicit result
                    # Remove ID from dict before returning to keep interface clean
                    res = results_map[i]
                    res.pop('id', None)
                    final_results.append(res)
                else:
                    # Missing or originally empty - fallback to raw text as description?
                    # If it was empty string, empty desc.
                    if not isinstance(text, str) or not text.strip():
                        final_results.append({"keywords": [], "descriptions": []})
                    else:
                        # LLM missed a valid item -> Fallback to raw
                        logger.warning(f"LLM missed item ID {i}: '{text}'. Using fallback.")
                        final_results.append({"keywords": [], "descriptions": [text]})
            
            return final_results

        except Exception as e:
            logger.error(f"Error during batch segmentation: {e}")
            # Fallback for ENTIRE batch
            return [{"keywords": [], "descriptions": [t] if isinstance(t, str) and t.strip() else []} for t in texts]
