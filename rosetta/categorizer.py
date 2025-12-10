import asyncio
import json
import os
import pandas as pd
import instructor
import ollama
import nest_asyncio
import numpy as np
from scipy.spatial.distance import cosine
from openai import AsyncOpenAI
from pydantic import BaseModel
from enum import Enum
from typing import List, Dict, Optional
from .config import get_logger

# Enable nested event loops for notebook/script compatibility
nest_asyncio.apply()

logger = get_logger(__name__)

class CategoryEnum(str, Enum):
    GROCERIES = "Expenses:Groceries"
    TRAVEL = "Expenses:Travel"
    UTILITIES = "Expenses:Utilities"
    INCOME = "Income:Standard"
    TRANSFER = "Transfers"
    UNKNOWN = "Expenses:Unknown"

class TransactionCategory(BaseModel):
    category: CategoryEnum

class Categorizer:
    def __init__(self, memory_file: str = "category_memory.json"):
        # Initialize Async Instructor Client for Batch Processing (Slow Path)
        self.llm_client = instructor.from_openai(
            AsyncOpenAI(
                base_url="http://localhost:11434/v1",
                api_key="ollama", 
            ),
            mode=instructor.Mode.JSON,
        )
        
        # Async Ollama Client for Embeddings (Fast Path)
        self.ollama_client = ollama.AsyncClient()
        
        # Lightweight Vector Memory (In-Memory List of Dicts)
        # Structure: [{"embedding": [float], "category": str, "description": str}]
        self.memory_file = memory_file
        self.memory: List[Dict] = []
        self.load_memory()
        
        # Concurrency Control (Batch Size)
        self.sem = asyncio.Semaphore(10)

    def load_memory(self):
        if os.path.exists(self.memory_file):
            logger.info(f"Loading Vector Memory from {self.memory_file}...")
            try:
                with open(self.memory_file, "r") as f:
                    self.memory = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load memory: {e}")
                self.memory = []
        else:
            logger.info("No existing memory found. Starting fresh.")
            self.memory = []

    def save_memory(self):
        try:
            with open(self.memory_file, "w") as f:
                json.dump(self.memory, f)
            # logger.debug("Memory saved.")
        except Exception as e:
            logger.error(f"Failed to save memory: {e}")

    async def get_embedding(self, text: str) -> List[float]:
        try:
            logger.debug(f"Generating embedding for: '{text}'")
            response = await self.ollama_client.embeddings(model='all-minilm', prompt=text)
            emb = response['embedding']
            logger.debug(f"Embedding result (first 5): {emb[:5]}")
            return emb
        except Exception as e:
            logger.error(f"Embedding failed for '{text}': {e}")
            return []

    def find_best_match(self, target_embedding: List[float]) -> Optional[str]:
        """
        Finds the closest match in memory using Cosine Similarity.
        Returns category if similarity > 0.9, else None.
        """
        if not self.memory or not target_embedding:
            return None
            
        best_sim = -1.0
        best_cat = None
        
        target_vec = np.array(target_embedding)
        
        for item in self.memory:
            mem_vec = np.array(item['embedding'])
            # Cosine Distance = 1 - Cosine Similarity
            # We use scipy.spatial.distance.cosine which returns DISTANCE
            # Distance 0 = Identical
            # Similarity > 0.9  => Distance < 0.1
            
            # Avoid zero vector errors
            if np.all(mem_vec == 0) or np.all(target_vec == 0):
                continue
                
            dist = cosine(target_vec, mem_vec)
            sim = 1.0 - dist
            
            if sim > best_sim:
                best_sim = sim
                best_cat = item['category']
        
        # Threshold Check
        if best_sim > 0.9:
            logger.debug(f"Cache Hit (Sim: {best_sim:.4f}): {best_cat}")
            return best_cat
            
        return None

    async def classify_transaction(self, description: str, amount: float) -> str:
        async with self.sem:
            try:
                # 1. Fast Path: Generate Embedding & Check Memory
                embedding = await self.get_embedding(description)
                
                if embedding:
                    cached_category = self.find_best_match(embedding)
                    if cached_category:
                        return cached_category
                
                # 2. Slow Path: LLM Classification
                # logger.debug(f"Cache Miss for '{description}'. Calling LLM...")
                resp = await self.llm_client.chat.completions.create(
                    model="llama3.2",
                    messages=[
                        {
                            "role": "user",
                            "content": f"""
                            Classify this financial transaction into a category.
                            Description: {description}
                            Amount: {amount}
                            
                            Categories:
                            - Expenses:Groceries (Supermarkets, Food)
                            - Expenses:Travel (Trains, Flights, Hotels)
                            - Expenses:Utilities (Bills, Internet, Phone)
                            - Income:Standard (Salary, Refunds)
                            - Transfers (Internal transfers)
                            - Expenses:Unknown (If unsure)
                            """
                        }
                    ],
                    response_model=TransactionCategory,
                    max_retries=2
                )
                category = resp.category.value
                
                # 3. Save to Memory (if embedding succeeded)
                if embedding:
                    self.memory.append({
                        "description": description,
                        "embedding": embedding,
                        "category": category
                    })
                    # Save periodically or at end? 
                    # For safety in this MVP, we save implicitly. 
                    # To avoid excessive I/O, maybe verify if we should save here.
                    # Ideally, batch save. But for now, let's allow it or rely on a final save.
                    pass 
                
                return category

            except Exception as e:
                logger.warning(f"Categorization failed for '{description}': {e}")
                return CategoryEnum.UNKNOWN.value

    async def classify_batch(self, descriptions: List[str], amounts: List[float]) -> List[str]:
        tasks = [
            self.classify_transaction(d, a) 
            for d, a in zip(descriptions, amounts)
        ]
        results = await asyncio.gather(*tasks)
        
        # Save memory after batch processing
        self.save_memory()
        
        return results

    def run_categorization(self, df: pd.DataFrame, mapping) -> pd.DataFrame:
        """
        Main entry point for Stage 5.
        df: Normalized DataFrame (must contain 'description' column)
        """
        logger.info("Stage 5: Categorizer - Classifying Transactions (Lightweight Hybrid)...")
        
        # Fallback if description missing
        if 'description' not in df.columns:
            logger.warning("'description' column missing, extracting from meta/mapping...")
            extracted = []
            for _, row in df.iterrows():
                try:
                    import json
                    meta = json.loads(row['meta'])
                    extracted.append(str(meta.get(mapping.desc_col, "")))
                except:
                    extracted.append("Unknown")
            df['description'] = extracted

        descriptions = df['description'].tolist()
        amounts = df['amount'].tolist()

        # Run Async Batch Processing
        loop = asyncio.get_event_loop()
        categories = loop.run_until_complete(self.classify_batch(descriptions, amounts))
        
        df['account'] = categories
        return df
