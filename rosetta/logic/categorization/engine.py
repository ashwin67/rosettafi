import pandas as pd
import re
import os
from openai import OpenAI
import instructor
from typing import List, Dict

from rosetta.logic.categorization.segmentation import LLMSegmenter
from rosetta.logic.categorization.phonebook import Phonebook
from rosetta.logic.categorization.resolver import EntityResolver
from rosetta.data.constants import TOKENIZATION_PROMPT, BANNED_TAGS_SET, UNKNOWN_CATEGORY
from rosetta.utils import get_logger

logger = get_logger(__name__)

class CategorizationEngine:
    """
    The Orchestrator for the Entity-Centric Pipeline.
    Flow: Tokenize (Pass 1) -> Filter -> Resolve (Pass 2) -> Categorize (Pass 3).
    """
    
    def __init__(self, model_name="qwen2.5:7b"):
        # Setup LLM
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        api_key = os.getenv("OLLAMA_API_KEY", "ollama")
        
        self.client = instructor.from_openai(
            OpenAI(base_url=base_url, api_key=api_key),
            mode=instructor.Mode.JSON,
        )
        self.model_name = model_name
        
        # Sub-Components
        self.segmenter = LLMSegmenter(client=self.client, model=model_name)
        self.phonebook = Phonebook()
        self.resolver = EntityResolver(self.phonebook)

    def _prepare_df(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Initializes the necessary columns for categorization."""
        if 'Entity' not in df.columns:
            df['Entity'] = None
        if 'Category' not in df.columns:
            df['Category'] = UNKNOWN_CATEGORY
        if 'merchant_clean' not in df.columns:
            df['merchant_clean'] = df[description_col]
        return df

    def _tokenize_df(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Runs the LLM tokenizer on the description column."""
        logger.info(f"Pass 1: Tokenizing and Cleaning {len(df)} rows...")
        texts = df[description_col].fillna("").tolist()
        
        BATCH_SIZE = 5
        cleaned_names = []
        for i in range(0, len(texts), BATCH_SIZE):
            chunk_texts = texts[i:i + BATCH_SIZE]
            token_lists = self.segmenter.tokenize_batch(chunk_texts, TOKENIZATION_PROMPT)
            for tokens in token_lists:
                clean = self._post_process_tokens(tokens)
                reconstructed = " ".join(clean)
                cleaned_names.append(reconstructed)
        
        df['merchant_clean'] = cleaned_names
        return df

    def _resolve_df(self, df: pd.DataFrame, description_col: str) -> pd.DataFrame:
        """Runs the entity resolver on the tokenized dataframe."""
        logger.info(f"Pass 2: Entity Resolution on {len(df)} rows...")

        def resolve_row(row):
            # Only resolve if not already resolved
            if pd.notna(row.get('Entity')) and row.get('Category') != UNKNOWN_CATEGORY:
                return pd.Series([row['Entity'], row['Category']])

            candidate = row['merchant_clean']
            full_desc = row[description_col]
            
            entity = self.resolver.resolve(candidate)
            if not entity:
                entity = self.resolver.resolve(full_desc)
            
            if entity:
                category = self.resolver.determine_category(entity, full_desc)
                return pd.Series([entity.canonical_name, category])
            else:
                return pd.Series([None, UNKNOWN_CATEGORY])

        df[['Entity', 'Category']] = df.apply(resolve_row, axis=1)
        return df

    def run(self, df: pd.DataFrame, description_col: str = "Description") -> pd.DataFrame:
        """
        Full, non-interactive categorization run.
        """
        if df.empty:
            return self._prepare_df(df, description_col)

        df = self._prepare_df(df, description_col)
        df = self._tokenize_df(df, description_col)
        df = self._resolve_df(df, description_col)
        
        resolved_count = df['Entity'].notna().sum()
        logger.info(f"Run complete. Resolved {resolved_count}/{len(df)} transactions.")
        return df

    def run_interactive(self, df: pd.DataFrame, description_col: str, batch_size: int = 100):
        """
        Interactive, generator-based categorization. Processes the DataFrame in
        batches and yields newly discovered unknown entities for user feedback.
        """
        logger.info("Starting Interactive Categorization...")
        df = self._prepare_df(df, description_col)
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size].copy()
            
            # Process the batch
            logger.info(f"\n--- Processing batch {i // batch_size + 1} ({len(batch_df)} rows) ---")
            batch_df = self._tokenize_df(batch_df, description_col)
            batch_df = self._resolve_df(batch_df, description_col)
            
            # Update the main dataframe with the results from the batch
            df.update(batch_df)
            
            # Discover and yield unknowns found in this batch
            unknowns = self.discover_entities(batch_df)
            if unknowns:
                logger.info(f"Discovered {len(unknowns)} unknowns in this batch.")
                yield unknowns
                
                # After yielding, re-resolve the current batch to apply new knowledge
                # before moving to the next one.
                logger.info("Re-applying knowledge to current batch...")
                batch_df = self._resolve_df(batch_df, description_col)
                df.update(batch_df)

        logger.info("Interactive categorization complete.")

    def _post_process_tokens(self, token_list: List[str]) -> List[str]:
        """
        Deterministic Python Filter to remove BANNED_TAGS.
        """
        clean_tokens = []
        for token in token_list:
            if not token: continue
            
            t = token.strip()
            
            # 1. Strict Number Filter
            if re.fullmatch(r'[\d.,\-]+', t): continue

            # 2. ID Filter (>15 chars mixed)
            if len(t) > 15 and any(c.isdigit() for c in t) and any(c.isalpha() for c in t):
                continue

            # 3. Blocklist
            clean_tag = t.replace('/', '').replace(':', '').upper()
            if clean_tag in BANNED_TAGS_SET:
                continue
            
            # 4. Short noise
            if len(t) < 2: continue

            clean_tokens.append(t)
            
        return clean_tokens

    # --- API Methods for Web App ---
    
    def discover_entities(self, df: pd.DataFrame, description_col: str = "description") -> List[Dict]:
        """
        Returns a list of unique 'merchant_clean' strings that failed resolution,
        along with Suggested Matches and original description examples.
        """
        # Filter for rows where Entity is None (failed strict resolution)
        unknown_df = df[df['Entity'].isna()].copy()
        unique_unknown_merchants = unknown_df['merchant_clean'].dropna().unique()
        
        results = []
        for name in unique_unknown_merchants:
            if not name.strip(): continue
            
            # Find original descriptions for this unknown merchant, if the column exists
            original_examples = []
            if description_col in unknown_df.columns:
                original_examples = unknown_df[unknown_df['merchant_clean'] == name][description_col].unique().tolist()

            # Find best suggestion
            matches = self.resolver.find_similar(name, top_n=1)
            suggestion = None
            confidence = 0.0
            
            if matches:
                alias, score = matches[0]
                # Look up the Canonical Entity for this alias
                entity_id = self.phonebook.alias_index.get(alias)
                if entity_id:
                    entity = self.phonebook.entities[entity_id]
                    suggestion = entity.canonical_name
                    confidence = score

            results.append({
                "raw": name,
                "suggested_name": suggestion,
                "confidence": round(confidence, 2),
                "original_examples": original_examples[:3] # Return up to 3 unique examples
            })
            
        return results

    def register_entity(self, name: str, category: str = None, alias: str = None):
        """
        API to add a new entity to the Phonebook.
        """
        aliases = [alias] if alias else []
        self.phonebook.register_entity(name, category, aliases)
        logger.info(f"Registered new entity: {name} ({category})")