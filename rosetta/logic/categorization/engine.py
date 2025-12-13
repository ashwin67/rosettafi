from rosetta.logic.categorization.cleaner import CleanerLayer
from rosetta.logic.categorization.rules import RulesLayer
from rosetta.logic.categorization.matcher import VectorMatcherLayer
from rosetta.logic.categorization.agent import AgentLayer
from rosetta.data.constants import UNKNOWN_CATEGORY
from rosetta.config import get_logger

logger = get_logger(__name__)

class CategorizationEngine:
    """
    Orchestrator for the 4-Layer Categorization Pipeline.
    1. Cleaner (Regex)
    2. Rules (Dictionary)
    3. Matcher (Vector Cache)
    4. Agent (LLM)
    """
    
    def __init__(self):
        self.cleaner = CleanerLayer()
        self.rules = RulesLayer()
        self.matcher = VectorMatcherLayer()
        self.agent = AgentLayer()

    def run(self, description: str) -> str:
        """
        Executes the pipeline for a single transaction description.
        Returns the category.
        """
        if not description or not description.strip():
            return UNKNOWN_CATEGORY
            
        # 1. Cleaner Layer
        cleaned_desc = self.cleaner.clean(description)
        logger.debug(f"Pipeline Input: '{description}' -> Cleaned: '{cleaned_desc}'")
        
        # 2. Rules Layer (Fast deterministic)
        rule_hit = self.rules.apply_hard_rules(cleaned_desc)
        if rule_hit:
            return rule_hit
            
        # 3. Matcher Layer (Vector RAG)
        # Note: We match against the CLEANED description for better semantic grouping
        match_hit = self.matcher.find_best_match(cleaned_desc)
        if match_hit:
            return match_hit
            
        # 4. Agent Layer (LLM)
        # Get context from Matcher (it owns the memory/categories)
        context = self.matcher.get_known_categories()
        new_category = self.agent.ask_agent(cleaned_desc, context)
        
        # 5. Self-Healing (Update Memory)
        if new_category != UNKNOWN_CATEGORY:
            # We store the CLEANED description to prevent garbage polluting the vector space
            self.matcher.update_memory(cleaned_desc, new_category)
            
        return new_category
