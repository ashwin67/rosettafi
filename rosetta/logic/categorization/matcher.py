import json
import os
import shutil
import ollama
from typing import List, Dict, Optional, Tuple
from scipy.spatial.distance import cosine
from rosetta.config import get_logger
from rosetta.workspace import Workspace
from rosetta.data.constants import (
    SIMILARITY_THRESHOLD, 
    DEFAULT_CATEGORIES, 
    CATEGORIZER_EMBEDDING_MODEL
)

logger = get_logger(__name__)
workspace = Workspace()

class VectorMatcherLayer:
    """
    Layer 3: Vector RAG Matcher.
    Manages Memory (JSON) and performs Cosine Similarity search.
    """
    
    def __init__(self):
        self.memory_path = workspace.get_memory_path()
        self.memory: List[Dict] = []
        self._load_memory()

    def _load_memory(self):
        """Loads semantic memory from JSON. Seeds if missing. Handles corruption."""
        if not os.path.exists(self.memory_path):
            self._seed_memory()
            return

        try:
            with open(self.memory_path, 'r') as f:
                self.memory = json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Memory corruption detected: {e}. Backing up and resetting.")
            # Backup
            if os.path.exists(self.memory_path):
                shutil.copy(self.memory_path, f"{self.memory_path}.bak")
            self._seed_memory()

    def _seed_memory(self):
        """Seeds memory with defaults."""
        logger.info("Seeding Vector Memory with Default Categories...")
        self.memory = []
        for cat in DEFAULT_CATEGORIES:
            emb = self.get_embedding(cat) # Using category name as description for seed
            if emb:
                self.memory.append({
                    "category": cat,
                    "description": cat,
                    "embedding": emb
                })
        self._save_memory()

    def _save_memory(self):
        """Persists memory to disk."""
        try:
            with open(self.memory_path, 'w') as f:
                json.dump(self.memory, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save memory: {e}")

    def get_embedding(self, text: str) -> Optional[List[float]]:
        """Generates embedding using Ollama."""
        try:
            response = ollama.embeddings(model=CATEGORIZER_EMBEDDING_MODEL, prompt=text)
            return response['embedding']
        except Exception as e:
            logger.error(f"Embedding failed for '{text}': {e}")
            return None

    def find_best_match(self, description: str) -> Optional[str]:
        """
        Generates embedding for input and finds best match in memory.
        Returns Category if similarity > Threshold.
        """
        if not description:
            return None
            
        emb = self.get_embedding(description)
        if not emb:
            return None
            
        best_score = -1.0
        best_category = None
        
        for entry in self.memory:
            try:
                # Cosine distance: 0=identical, 1=orthogonal, 2=opposite
                # Similarity = 1 - distance
                score = 1 - cosine(emb, entry['embedding'])
                if score > best_score:
                    best_score = score
                    best_category = entry['category']
            except ValueError:
                continue
        
        if best_category and best_score >= SIMILARITY_THRESHOLD:
            logger.info(f"VECTOR MATCH: '{description}' -> '{best_category}' (Score: {best_score:.2f})")
            return best_category
            
        logger.info(f"VECTOR MISS: '{description}' (Best: {best_category} @ {best_score:.2f})")
        return None

    def update_memory(self, description: str, category: str):
        """Adds a new learned association to memory."""
        emb = self.get_embedding(description)
        if emb:
            self.memory.append({
                "category": category,
                "description": description,
                "embedding": emb
            })
            self._save_memory()
            logger.info(f"LEARNED: '{description}' -> '{category}'")

    def get_known_categories(self) -> List[str]:
        return list(set(m['category'] for m in self.memory))
