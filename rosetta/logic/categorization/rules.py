from typing import Optional
from rosetta.data.constants import HARD_CODED_RULES
from rosetta.config import get_logger

logger = get_logger(__name__)

class RulesLayer:
    """
    Layer 2: Deterministic Rules.
    Checks the cleaned description against a dictionary of known mappings.
    Fastest path.
    """
    
    @staticmethod
    def apply_hard_rules(cleaned_description: str) -> Optional[str]:
        if not cleaned_description:
            return None
            
        desc_lower = cleaned_description.lower()
        
        # O(1) Dictionary Lookup? Or substring search?
        # The constants defined "hypotheek" -> ...
        # If I have Cleaned Description: "ABN AMRO Hypotheek"
        # We should check if any KEY in RULES is IN description.
        
        # Optimization: Sort keys by length desc to match longest first? 
        # For now, simple iteration.
        
        for keyword, category in HARD_CODED_RULES.items():
            if keyword in desc_lower:
                logger.debug(f"RULE MATCH: '{keyword}' in '{cleaned_description}' -> {category}")
                return category
                
        return None
