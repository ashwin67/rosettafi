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

    def run(self, df: pd.DataFrame, description_col: str = "Description") -> pd.DataFrame:
        logger.info(f"Starting Categorization Pipeline on {len(df)} rows...")
        
        if df.empty:
            df['Entity'] = None
            df['Category'] = UNKNOWN_CATEGORY
            df['merchant_clean'] = None
            return df

        # Initialize Output Columns
        df['Entity'] = None # The Resolved Canonical Name
        df['Category'] = UNKNOWN_CATEGORY
        df['merchant_clean'] = df[description_col] # Raw Fallback

        # Filter: Only process rows that aren't already categorized/resolved?
        # For now, process all.

        # --- PASS 1: TOKENIZATION & FILTERING ---
        logger.info("Pass 1: Tokenizing and Cleaning...")
        
        # 1a. Batch Tokenize via LLM
        texts = df[description_col].fillna("").tolist()
        
        # Chunking for LLM stability
        BATCH_SIZE = 5
        cleaned_names = []
        
        for i in range(0, len(texts), BATCH_SIZE):
            chunk_texts = texts[i:i+BATCH_SIZE]
            # LLM Call
            token_lists = self.segmenter.tokenize_batch(chunk_texts, TOKENIZATION_PROMPT)
            
            # Python Filter Call
            for tokens in token_lists:
                clean = self._post_process_tokens(tokens)
                # Reconstruct Name (e.g., "Albert Heijn")
                reconstructed = " ".join(clean)
                cleaned_names.append(reconstructed)

        df['merchant_clean'] = cleaned_names
        
        # --- PASS 2: RESOLUTION & CATEGORIZATION ---
        logger.info("Pass 2: Entity Resolution (Phonebook Lookup)...")
        
        def resolve_row(row):
            candidate = row['merchant_clean']
            full_desc = row[description_col]
            
            # Try Resolve
            entity = self.resolver.resolve(candidate)
            
            if entity:
                # Found in Phonebook!
                category = self.resolver.determine_category(entity, full_desc)
                return pd.Series([entity.canonical_name, category])
            else:
                # Unknown Entity
                return pd.Series([None, UNKNOWN_CATEGORY])

        # Apply Resolution
        df[['Entity', 'Category']] = df.apply(resolve_row, axis=1)
        
        # Metrics
        resolved_count = df['Entity'].notna().sum()
        logger.info(f"Resolved {resolved_count}/{len(df)} transactions via Phonebook.")

        return df

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
    
    def discover_entities(self, df: pd.DataFrame) -> List[Dict]:
        """
        Returns a list of unique 'merchant_clean' strings that failed resolution,
        along with Suggested Matches from the Phonebook.
        """
        # Filter for rows where Entity is None (failed strict resolution)
        unknowns = df[df['Entity'].isna()]['merchant_clean'].dropna().unique()
        
        results = []
        for name in unknowns:
            if not name.strip(): continue
            
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
                "confidence": round(confidence, 2)
            })
            
        return results

    def register_entity(self, name: str, category: str = None, alias: str = None):
        """
        API to add a new entity to the Phonebook.
        """
        aliases = [alias] if alias else []
        self.phonebook.register_entity(name, category, aliases)
        logger.info(f"Registered new entity: {name} ({category})")