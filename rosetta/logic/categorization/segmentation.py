import instructor
from pydantic import BaseModel, Field
from typing import List
from rosetta.utils import get_logger
from rosetta.models import TokenizedParts, BatchResult

logger = get_logger(__name__)

class LLMSegmenter:
    """
    Handles Pass 1: Batch Tokenization.
    Splits transaction strings into a flat list of logical parts.
    """

    def __init__(self, client, model):
        self.client = client
        self.model = model
        # Ensure client is patched
        if self.client and not hasattr(self.client, "chat"):
             self.client = instructor.patch(self.client, mode=instructor.Mode.JSON)

    def tokenize(self, text: str, system_prompt: str) -> List[str]:
        """
        Splits a single string into smart tokens.
        """
        if not text or not text.strip():
            return []
            
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                response_model=TokenizedParts,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text}
                ],
                max_retries=2
            )
            return resp.parts
        except Exception as e:
            logger.error(f"Tokenization failed for text '{text[:20]}...': {e}")
            return text.split()

    def tokenize_batch(self, texts: List[str], system_prompt: str) -> List[List[str]]:
        """
        Process a batch.
        """
        valid_items = [(i, t) for i, t in enumerate(texts) if t and t.strip()]
        if not valid_items:
            return [[] for _ in texts]
            
        # We wrap inputs to ensure the model knows which ID corresponds to which text
        formatted_input = "\n".join([f"ID {i}: {t}" for _, (i, t) in enumerate(valid_items)])
        
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                response_model=BatchResult,
                messages=[
                    {"role": "system", "content": system_prompt + "\nReturn a list of results matching the input IDs."},
                    {"role": "user", "content": formatted_input}
                ]
            )
            
            results = resp.results
            
            if len(results) != len(valid_items):
                 logger.warning(f"Batch tokenization count mismatch. Expected {len(valid_items)}, got {len(results)}. Fallback to single processing.")
                 return [self.tokenize(t, system_prompt) for t in texts]
                 
            final_output = [[] for _ in texts]
            for idx, (original_idx, _) in enumerate(valid_items):
                final_output[original_idx] = results[idx].parts
                
            return final_output
            
        except Exception as e:
            logger.error(f"Batch failed: {e}. Falling back to single processing.")
            return [self.tokenize(t, system_prompt) for t in texts]