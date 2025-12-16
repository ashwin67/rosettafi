import pandas as pd
from openai import OpenAI
import instructor
import os
from rosetta.logic.categorization.batch import BatchCategorizer
from rosetta.logic.categorization.segmentation import LLMSegmenter
from rosetta.data.constants import UNKNOWN_CATEGORY, ENTITY_SEGMENTATION_PROMPT, BATCH_CATEGORIZATION_PROMPT
from rosetta.utils import get_logger
import pandas as pd
import os
import instructor
from openai import OpenAI
from typing import List, Dict
from pathlib import Path
import csv
import json

logger = get_logger(__name__)

class CategorizationEngine:
    def __init__(self, model_name="llama3.2"):
        # Setup LLM client
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        api_key = os.getenv("OLLAMA_API_KEY", "ollama")
        
        self.client = instructor.from_openai(
            OpenAI(base_url=base_url, api_key=api_key),
            mode=instructor.Mode.JSON,
        )
        self.model_name = model_name
        self.segmenter = LLMSegmenter(client=self.client, model=model_name)
        self.batcher = BatchCategorizer(client=self.client, model=model_name)
        
        # Workspace Paths
        self.cache_dir = Path.home() / ".rosetta_cache"
        self.cache_dir.mkdir(exist_ok=True, parents=True)
        self.corrections_file = self.cache_dir / "corrections.csv"
        self.review_file = self.cache_dir / "extraction_review.csv"
        
        self.corrections = self._load_corrections()

    def _load_corrections(self) -> dict:
        """Loads manual corrections from CSV (Original -> Corrected)"""
        corrections = {}
        if self.corrections_file.exists():
            try:
                # Read CSV: Assume header "description,merchant_clean"
                df = pd.read_csv(self.corrections_file)
                # Map Description -> Merchant
                if 'description' in df.columns and 'merchant_clean' in df.columns:
                     corrections = pd.Series(df.merchant_clean.values, index=df.description).to_dict()
                logger.info(f"Loaded {len(corrections)} manual corrections.")
            except Exception as e:
                logger.error(f"Failed to load corrections: {e}")
        return corrections

    def _save_review_file(self, df: pd.DataFrame, description_col: str):
        """Saves suspicious extractions for user review."""
        # For segmentation, "Suspicious" might mean empty description or description == raw
        # We assume if merchant_clean == description, it failed or was just one word.
        # Let's save items where merchant_clean is empty OR same as description (if desc > 20 chars)
        
        failed_mask = (df['merchant_clean'] == "") | \
                      ((df['merchant_clean'] == df[description_col]) & (df[description_col].str.len() > 30))
        
        review_df = df[failed_mask][[description_col, 'merchant_clean']].copy()
        review_df = review_df.drop_duplicates(subset=[description_col])
        review_df['suggested_fix'] = "" 
        
        if not review_df.empty:
            review_df.to_csv(self.review_file, index=False)
            logger.warning(f"Saved {len(review_df)} items to {self.review_file} for review.")
        else:
            logger.info("No suspicious extractions to review!")

    def run(self, df: pd.DataFrame, description_col: str = "Description") -> pd.DataFrame:
        """
        Orchestrates the 2-Pass Segmentation & Categorization Pipeline.
        """
        logger.info(f"Starting Categorization Pipeline on {len(df)} rows...")

        # Pass 1: Batch LLM Segmentation
        logger.info("Pass 1: Segmenting Entities (Batch LLM)...")
        
        # Initialize column
        df['merchant_clean'] = df[description_col] # Default to raw
        
        # Filter rows that are NOT in corrections (we don't need to waste LLM calls on corrected items)
        unknown_mask = ~df[description_col].isin(self.corrections.keys())
        to_process_df = df[unknown_mask]
        
        if not to_process_df.empty:
            # Chunking logic
            import numpy as np
            BATCH_SIZE = 5 # Small batch for robust JSON
            chunks = [to_process_df[i:i + BATCH_SIZE] for i in range(0, to_process_df.shape[0], BATCH_SIZE)]
            
            cleaned_merchants_map = {} # desc -> clean
            
            for i, chunk in enumerate(chunks):
                texts = chunk[description_col].tolist()
                logger.debug(f"Segmenting Batch {i+1}/{len(chunks)}...")
                
                results = self.segmenter.segment_batch(texts, ENTITY_SEGMENTATION_PROMPT)
                
                # Align results with texts
                # Just assuming order preservation. 
                # If len match, great. If not, safe fallback for this batch?
                if len(results) == len(texts):
                    for text, res in zip(texts, results):
                        # Join descriptions
                        raw_descs = res.get("descriptions", [])
                        clean_descs = [d.strip() for d in raw_descs if d.strip()]
                        clean_name = " ".join(clean_descs).strip()
                        if not clean_name:
                            clean_name = text # Fallback
                        cleaned_merchants_map[text] = clean_name
                else:
                    logger.error(f"Batch {i+1} mismatch: Sent {len(texts)}, Got {len(results)}. Skipping batch.")
            
            # Apply results to dataframe
            df.loc[unknown_mask, 'merchant_clean'] = df.loc[unknown_mask, description_col].map(cleaned_merchants_map).fillna(df[description_col])

        # APPLY CORRECTIONS (Override)
        if self.corrections:
            logger.info("Applying manual corrections...")
            mask = df[description_col].isin(self.corrections.keys())
            df.loc[mask, 'merchant_clean'] = df.loc[mask, description_col].map(self.corrections)

        # GENERATE REVIEW FILE
        self._save_review_file(df, description_col)

        # Pass 2: Batch Categorizing
        logger.info("Pass 2: Batch Categorizing...")
        
        unique_merchants = df['merchant_clean'].dropna().unique().tolist()
        logger.info(f"Found {len(unique_merchants)} unique merchants to categorize.")
        
        category_map = self.batcher.categorize_batch(
            unique_merchants, 
            system_prompt=BATCH_CATEGORIZATION_PROMPT
        )
        
        # Map back
        df = self.batcher.map_categories(df, category_map)
        
        logger.info("Categorization Complete.")
        return df
