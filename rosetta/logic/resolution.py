from sentence_transformers import SentenceTransformer
from rosetta.database import RosettaDB
from rosetta.logic.cleaning import TextCleaner
from typing import Optional, Dict, Any

class EntityResolver:
    def __init__(self, db: RosettaDB, model_name: str = 'all-MiniLM-L6-v2'):
        self.db = db
        # Lazy load model to avoid overhead if not used immediately
        self._model = None
        self.model_name = model_name
        self.cleaner = TextCleaner()

    @property
    def model(self):
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def resolve(self, description: str, threshold: float = 0.85) -> Optional[Dict[str, Any]]:
        """
        Clean the description, generate embeddings, and query DuckDB for the nearest match.
        """
        cleaned_text = self.cleaner.clean(description)
        if not cleaned_text:
            return None
            
        # Generate embedding (O(1) with respect to DB size, O(L) with respect to text length)
        embedding = self.model.encode(cleaned_text).tolist()
        
        # Vector Similarity Search in DuckDB
        result = self.db.find_nearest_merchant(embedding, threshold)
        if result:
            return {
                "canonical_name": result[0],
                "default_category": result[1],
                "similarity": float(result[2])
            }
        return None

    def add_merchant(self, canonical_name: str, category: str, description: str = None):
        """
        Add or update a merchant in the database with its vector embedding.
        """
        text_to_embed = description if description else canonical_name
        cleaned_text = self.cleaner.clean(text_to_embed)
        embedding = self.model.encode(cleaned_text).tolist()
        self.db.upsert_merchant(canonical_name, category, embedding)
