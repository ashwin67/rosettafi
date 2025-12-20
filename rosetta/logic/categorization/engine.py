import pandas as pd
import re
from typing import List, Dict

from rosetta.logic.categorization.phonebook import Phonebook
from rosetta.logic.categorization.resolver import EntityResolver
from rosetta.data.constants import UNKNOWN_CATEGORY
from rosetta.utils import get_logger

logger = get_logger(__name__)

class CategorizationEngine:
    """
    The Orchestrator for the Entity-Centric Pipeline.
    Manages the Phonebook and Resolver.
    """
    
    def __init__(self):
        # Sub-Components are now lightweight and don't use LLMs for tokenization
        self.phonebook = Phonebook()
        self.resolver = EntityResolver(self.phonebook)

    def _clean_description_text(self, text: str, dynamic_noise_list: set = None) -> str:
        """
        A non-LLM based cleaner to remove noise from transaction descriptions.
        """
        if not isinstance(text, str):
            return ""
        
        # 1. Lowercase
        text = text.lower()
        
        # 2. Remove DYNAMIC noise words if provided
        if dynamic_noise_list:
            for n in dynamic_noise_list:
                text = re.sub(r'\b' + re.escape(n) + r'\b', '', text)
            
        # 3. Remove universal noise like dates, times, and long numbers/codes
        text = re.sub(r'\b\d{2}[./-]\d{2}[./-]\d{2,4}\b', '', text) # Dates
        text = re.sub(r'\b\d{2}:\d{2}\b', '', text) # Times
        text = re.sub(r'\b[a-z0-9]{16,}\b', '', text) # Long transaction IDs
        text = re.sub(r'\b[0-9]{5,}\b', '', text) # Long numbers
        
        # 4. Remove special characters and extra whitespace
        text = re.sub(r'[/,*]', ' ', text) # Replace slashes, commas, asterisks with space
        text = re.sub(r'[^a-z0-9\s]', '', text) # Remove remaining non-alphanumeric
        text = ' '.join(text.split()) # Consolidate whitespace
        
        return text.strip()

    def _prepare_df(self, df: pd.DataFrame, description_col: str, dynamic_noise_list: set = None) -> pd.DataFrame:
        """Initializes the necessary columns for categorization."""
        if 'Entity' not in df.columns:
            df['Entity'] = None
        if 'Category' not in df.columns:
            df['Category'] = UNKNOWN_CATEGORY
        if 'confidence' not in df.columns:
            df['confidence'] = 0.0
        
        if description_col in df.columns:
            df['merchant_clean'] = df[description_col].fillna('').apply(
                self._clean_description_text, 
                dynamic_noise_list=dynamic_noise_list
            )
        else:
            df['merchant_clean'] = ''
        return df

    # --- API Methods for Web App ---
    
    def discover_entities(self, df: pd.DataFrame, description_col: str = "description") -> List[Dict]:
        """
        Returns a list of unique 'merchant_clean' strings that failed resolution,
        along with Suggested Matches and original description examples.
        """
        # Ensure the df is prepared
        df = self._prepare_df(df, description_col)
        
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
        aliases = [alias] if alias and alias != name else []
        self.phonebook.register_entity(name, category, aliases)
        logger.info(f"Registered new entity: {name} ({category}) with alias '{alias}'")

    def resolve_and_categorize(self, df: pd.DataFrame, description_col: str = "description"):
        """
        A non-interactive method to resolve entities and categories for a given DataFrame.
        This is useful for re-applying knowledge after registering new entities.
        """
        df = self._prepare_df(df, description_col)
        if df.empty:
            return df
        
        def resolve_row(row):
            # Use the cleaned merchant name for resolution
            candidate = row['merchant_clean']
            
            entity = self.resolver.resolve(candidate)
            # We don't fallback to the original description anymore, as it's too noisy
            
            if entity:
                category = self.resolver.determine_category(entity, row[description_col])
                return pd.Series([entity.canonical_name, category])
            else:
                return pd.Series([None, UNKNOWN_CATEGORY])

        df[['Entity', 'Category']] = df.apply(resolve_row, axis=1)
        return df
