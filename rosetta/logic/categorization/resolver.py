import re
from thefuzz import process, fuzz
from typing import Optional, List, Tuple
from rosetta.models import MerchantEntity
from rosetta.logic.categorization.phonebook import Phonebook
from rosetta.utils import get_logger

logger = get_logger(__name__)

class EntityResolver:
    """
    Pass 2: The Resolver.
    Takes a candidate name and finds the matching Entity in the Phonebook.
    Uses a hybrid approach: Exact Match -> Substring Match -> Fuzzy Match.
    """
    
    def __init__(self, phonebook: Phonebook):
        self.phonebook = phonebook
        # We define a similarity threshold (0-100 for thefuzz)
        self.SIMILARITY_THRESHOLD = 88

    def resolve(self, candidate_name: str) -> Optional[MerchantEntity]:
        """
        Resolves a string to an Entity using a hybrid approach.
        """
        if not isinstance(candidate_name, str) or not candidate_name.strip():
            return None

        candidate_lower = candidate_name.lower().strip()
        
        # 1. Exact Match (Fastest)
        entity = self.phonebook.find_entity_by_alias(candidate_lower)
        if entity:
            return entity

        # 2. Substring Match (Prefer longest match)
        found_matches = []
        for alias in self.phonebook.alias_index.keys():
            if re.search(r'\b' + re.escape(alias) + r'\b', candidate_lower):
                found_matches.append(alias)
        
        if found_matches:
            # Prioritize the longest matching alias (e.g., "amazon web services" over "amazon")
            best_match = max(found_matches, key=len)
            entity_id = self.phonebook.alias_index[best_match]
            logger.debug(f"Substring Match: '{candidate_name}' -> '{best_match}'")
            return self.phonebook.entities[entity_id]

        # 3. Fuzzy Match (Slower Fallback)
        all_aliases = list(self.phonebook.alias_index.keys())
        if not all_aliases:
            return None
            
        best_match = process.extractOne(candidate_lower, all_aliases, score_cutoff=self.SIMILARITY_THRESHOLD)
        
        if best_match:
            best_match_alias, score = best_match
            entity_id = self.phonebook.alias_index[best_match_alias]
            logger.debug(f"Fuzzy Match: '{candidate_name}' -> '{best_match_alias}' (Score: {score})")
            return self.phonebook.entities[entity_id]

        return None

    def find_similar(self, candidate_name: str, top_n: int = 3, threshold: float = 60.0) -> List[Tuple[str, float]]:
        """
        Returns a list of (alias, score) tuples for the best matches using thefuzz.
        The score is converted from 0-100 to 0.0-1.0.
        """
        candidate_lower = candidate_name.lower().strip()
        if not candidate_lower:
            return []

        all_aliases = list(self.phonebook.alias_index.keys())
        if not all_aliases:
            return []

        # process.extractBests returns a list of (match, score)
        matches = process.extractBests(candidate_lower, all_aliases, scorer=fuzz.token_set_ratio, score_cutoff=threshold, limit=top_n)
        
        # Convert score to 0-1.0 float
        results = [(match, score / 100.0) for match, score in matches]
            
        return results

    def determine_category(self, entity: MerchantEntity, full_description_text: str) -> str:
        """
        Determines category based on Entity Default + Context Rules.
        """
        # 1. Check Context Rules
        if entity.rules and full_description_text:
            for rule in entity.rules:
                if rule.contains_keyword.lower() in full_description_text.lower():
                    logger.debug(f"Context Rule Applied: '{rule.contains_keyword}' -> {rule.assign_category}")
                    return rule.assign_category
        
        # 2. Default
        return entity.default_category