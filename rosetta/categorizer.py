from rosetta.logic.categorization.engine import CategorizationEngine as HybridCategorizer
from rosetta.logic.categorization.engine import CategorizationEngine

# Backwards compatibility if needed, or strictly expose the new Engine.
# The previous prompt asked to "re-export CategorizationEngine"
# But main.py was updated to use HybridCategorizer.
# So we alias it here to avoid breaking changes in main.py again.

__all__ = ["HybridCategorizer", "CategorizationEngine"]
