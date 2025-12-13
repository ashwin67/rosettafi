import json
import os
import shutil
import ollama
from typing import List, Dict, Optional, Tuple
from scipy.spatial.distance import cosine
from .config import get_logger
from .workspace import Workspace
from .data.constants import (
    SIMILARITY_THRESHOLD, UNKNOWN_CATEGORY, DEFAULT_CATEGORIES,
    CATEGORIZER_EMBEDDING_MODEL, CATEGORIZER_SYSTEM_PROMPT, LLM_MODEL_NAME
)

logger = get_logger(__name__)
workspace = Workspace()

class HybridCategorizer:
    def __init__(self):
        self.memory_path = workspace.get_memory_path()
        self.memory: List[Dict] = []
        self._load_memory()

    def _load_memory(self):
        """Loads semantic memory from JSON. Seeds if missing. Handles corruption."""
        if not os.path.exists(self.memory_path):
            logger.info("Memory file not found. Seeding default categories...")
            self._seed_memory()
            return
            
        try:
            with open(self.memory_path, 'r') as f:
                self.memory = json.load(f)
            logger.info(f"Loaded {len(self.memory)} categories into memory.")
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Memory corruption detected: {e}. Backing up and resetting.")
            shutil.copy(self.memory_path, f"{self.memory_path}.bak")
            self._seed_memory()

    def _seed_memory(self):
        """Seeds memory with defaults and generates embeddings."""
        self.memory = []
        for cat in DEFAULT_CATEGORIES:
            emb = self._get_embedding(cat)
            if emb:
                self.memory.append({
                    "category": cat,
                    "description": cat, # Self-referential for seed
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

    def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Generates embedding using Ollama."""
        try:
            response = ollama.embeddings(model=CATEGORIZER_EMBEDDING_MODEL, prompt=text)
            return response['embedding']
        except Exception as e:
            logger.error(f"Embedding failed for '{text}': {e}")
            return None

    def _find_best_match(self, target_embedding: List[float]) -> Tuple[Optional[str], float]:
        """Finds best semantic match in memory using Cosine Similarity."""
        best_score = -1.0
        best_category = None
        
        for entry in self.memory:
            # Cosine distance is 0 for identical. Similarity = 1 - distance.
            # Scipy returns distance.
            try:
                score = 1 - cosine(target_embedding, entry['embedding'])
                if score > best_score:
                    best_score = score
                    best_category = entry['category']
            except ValueError:
                continue
                
        return best_category, best_score

    def categorize(self, description: str) -> str:
        """
        Hyper-Categorization Flow:
        1. Fast Path: Vector Search
        2. Slow Path: LLM Classification
        3. Self-Healing: Update Memory
        """
        if not description or not description.strip():
             return UNKNOWN_CATEGORY

        # 1. Embed Input
        logger.debug(f"Categorizing: {description}")
        emb = self._get_embedding(description)
        if not emb:
            return UNKNOWN_CATEGORY

        # 2. Fast Path
        match_cat, score = self._find_best_match(emb)
        if match_cat and score >= SIMILARITY_THRESHOLD:
            logger.info(f"FAST PATH: '{description}' -> '{match_cat}' (Score: {score:.2f})")
            return match_cat

        # 3. Slow Path (LLM)
        logger.info(f"SLOW PATH: '{description}' (Best: {match_cat} @ {score:.2f} < {SIMILARITY_THRESHOLD})")
        new_category = self._ask_llm(description)
        
        # 4. Self-Healing (Memorize decision)
        if new_category != UNKNOWN_CATEGORY:
            self.memory.append({
                "category": new_category,
                "description": description,
                "embedding": emb
            })
            self._save_memory()
            
        return new_category

    def _ask_llm(self, description: str) -> str:
        """Queries LLM to classify description given current categories context."""
        try:
            # Get unique existing categories for context
            unique_cats = list(set(m['category'] for m in self.memory))
            
            prompt = CATEGORIZER_SYSTEM_PROMPT.format(existing_categories=", ".join(unique_cats))
            
            response = ollama.chat(model=LLM_MODEL_NAME, messages=[
                {'role': 'system', 'content': prompt},
                {'role': 'user', 'content': description}
            ])
            
            category = response['message']['content'].strip()
            # Basic cleanup (remove quotes output by some models)
            category = category.replace('"', '').replace("'", "").replace(".", "")
            return category
            
        except Exception as e:
            logger.error(f"LLM Classification failed: {e}")
            return UNKNOWN_CATEGORY
