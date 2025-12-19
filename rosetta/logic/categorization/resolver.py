import re
import difflib
from typing import Optional, List, Tuple
from rosetta.models import MerchantEntity
from rosetta.logic.categorization.phonebook import Phonebook
from rosetta.utils import get_logger

logger = get_logger(__name__)

class EntityResolver:
    """
    Pass 2: The Resolver.
    Takes a list of clean tokens (candidate names) and finds the matching Entity in the Phonebook.
    Uses Fuzzy Matching to handle slight variations.
    """
    
    def __init__(self, phonebook: Phonebook):
        self.phonebook = phonebook
        # We define a similarity threshold (0.0 - 1.0)
        self.SIMILARITY_THRESHOLD = 0.85

    def resolve(self, candidate_name: str) -> Optional[MerchantEntity]:
        """
        Resolves a string to an Entity.
        1. Exact Match (O(1))
        2. Fuzzy Match (Levenshtein)
        """
        if not isinstance(candidate_name, str) or not candidate_name.strip():
            return None

        candidate_lower = candidate_name.lower().strip().replace('_', ' ')
        if not candidate_lower:
            return None

        # 1. Exact Match
        # We check the original candidate name for exact match
        entity = self.phonebook.find_entity_by_alias(candidate_name)
        if entity:
            return entity

        # 2. Substring Match
        # Find all aliases that are substrings and select the most specific (longest) one.
        found_matches = []
        for alias in self.phonebook.alias_index.keys():
            try:
                if re.search(r'\b' + re.escape(alias) + r'\b', candidate_lower):
                    found_matches.append(alias)
            except re.error as e:
                logger.warning(f"Regex error for alias '{alias}': {e}")
                continue
        
        if found_matches:
            best_match = max(found_matches, key=len)
            entity_id = self.phonebook.alias_index[best_match]
            logger.debug(f"Substring Match (longest): '{candidate_name}' -> '{best_match}' ({entity_id})")
            return self.phonebook.entities[entity_id]

        # 3. Fuzzy Match
        # We compare candidate against all known aliases in the index
        # NOTE: For massive DBs, this needs optimization (SimHash or vector search).
        
        all_aliases = list(self.phonebook.alias_index.keys())
        matches = difflib.get_close_matches(candidate_lower, all_aliases, n=1, cutoff=self.SIMILARITY_THRESHOLD)
        
        if matches:
            best_match_alias = matches[0]
            entity_id = self.phonebook.alias_index[best_match_alias]
            logger.debug(f"Fuzzy Match: '{candidate_name}' -> '{best_match_alias}' ({entity_id})")
            return self.phonebook.entities[entity_id]

        return None

    def find_similar(self, candidate_name: str, top_n: int = 3, threshold: float = 0.6) -> List[Tuple[str, float]]:
        """
        Returns a list of (alias, score) tuples for the best matches.
        Used for the 'Suggestion' phase (Who is this?).
        """
        candidate_lower = candidate_name.lower().strip()
        if not candidate_lower:
            return []

        all_aliases = list(self.phonebook.alias_index.keys())
        
        # Get matches with scores
        # difflib.get_close_matches only returns strings, not scores. 
        # So we calculate ratios manually for the top candidates or use a different approach.
        # For simplicity and consistency with get_close_matches logic:
        
        matches = difflib.get_close_matches(candidate_lower, all_aliases, n=top_n, cutoff=threshold)
        
        results = []
        for match in matches:
            score = difflib.SequenceMatcher(None, candidate_lower, match).ratio()
            results.append((match, score))
            
        return results

    def determine_category(self, entity: MerchantEntity, full_description_text: str) -> str:
        """
        Determines category based on Entity Default + Context Rules.
        """
        # 1. Check Context Rules
        for rule in entity.rules:
            if rule.contains_keyword.lower() in full_description_text.lower():
                logger.debug(f"Context Rule Applied: '{rule.contains_keyword}' -> {rule.assign_category}")
                return rule.assign_category
        
        # 2. Default
        return entity.default_category