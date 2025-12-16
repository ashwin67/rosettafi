import pandas as pd
from typing import Dict, List
import json
import instructor

class BatchCategorizer:
    """
    Handles Pass 3: Contextual Batch Categorization.
    """
    
    def __init__(self, client=None, model: str = "llama3.2"):
        self.client = client
        self.model = model
        # Ensure client is patched if using instructor for structured output directly, 
        # though the requirements say "Return a JSON Dictionary". 
        # We can strictly type the response as well or just parse dict.
        # Let's use instructor with a simple Dict wrapper for safety if possible,
        # or just ask for a dict via the prompt and rely on the JSON mode if the model supports it.
        # Given "llama3.2", structured output via instructor is best practice.

    def categorize_batch(self, merchants: List[str], system_prompt: str) -> Dict[str, str]:
        """
        Pass 3: Send unique merchants to LLM and get categories.
        """
        unique_merchants = list(set([m for m in merchants if isinstance(m, str) and m.strip()]))
        
        if not unique_merchants:
            return {}

        # Chunking: If list is too huge, we might need to chunk.
        # For this task, assuming it fits in context or roughly < 100 items.
        # If > 100, we might want to split, but requirements imply "one context window" for cross-pollination.
        # We will proceed with sending all.
        
        # We define a helper model for the response to ensure we get a Dict[str, str]
        # Pydantic doesn't trivially support Dict[str,str] as root in all instructor versions easily 
        # without a wrapper class.
        from pydantic import BaseModel, Field
        class CategoryMapping(BaseModel):
            mapping: Dict[str, str] = Field(..., description="Dictionary mapping merchant name to category")

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                response_model=CategoryMapping,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Categorize these merchants:\n{unique_merchants}"}
                ]
            )
            return resp.mapping
        except Exception as e:
            print(f"Error during batch categorization: {e}")
            return {}

    def map_categories(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Map the finding back to the DataFrame.
        """
        df['Category'] = df['merchant_clean'].map(mapping).fillna('Uncategorized')
        return df
