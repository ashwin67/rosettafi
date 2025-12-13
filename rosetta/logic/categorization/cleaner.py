import re
from rosetta.data.constants import CLEANER_REGEX_PATTERNS
from rosetta.config import get_logger

logger = get_logger(__name__)

class CleanerLayer:
    """
    Layer 1: Heuristic Cleaner.
    Strips banking noise (SEPA, TRTP, IDs) to extract the core Merchant Name.
    """
    
    @staticmethod
    def clean(description: str) -> str:
        if not description:
            return ""
            
        cleaned = description
        
        # Apply all regex patterns
        for pattern in CLEANER_REGEX_PATTERNS:
            cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
            
        # Strip remaining whitespace
        cleaned = cleaned.strip()
        
        # Edge case: If we stripped everything, revert to original (better safe than sorry)
        if not cleaned:
            logger.warning(f"Cleaner stripped everything from '{description}'. Reverting.")
            return description.strip()
            
        return cleaned
