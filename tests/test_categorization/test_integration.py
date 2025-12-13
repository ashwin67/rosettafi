import pytest
from unittest.mock import patch, MagicMock
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.data.constants import UNKNOWN_CATEGORY

# Mocks
MOCK_EMB_GAS = [0.9, 0.1, 0.0] 
MOCK_EMB_UNK = [0.0, 0.0, 0.9] 

@pytest.fixture
def engine(tmp_path):
    # Mock workspace memory path
    with patch('rosetta.workspace.Workspace.get_memory_path', return_value=str(tmp_path / "mem.json")):
        # Mock initial seeding to avoid real Ollama call
        with patch('ollama.embeddings', return_value={'embedding': [0.1, 0.1, 0.1]}): 
            return CategorizationEngine()

def test_integration_rules_hit(engine):
    """Pipeline should exit early at Rules Layer."""
    # "Netflix" is in HARD_CODED_RULES
    res = engine.run("Betaling aan Netflix BV")
    assert res == "Expenses:Subscriptions"
    # Matcher and Agent should NOT be called ideally, or at least result matches rule.

def test_integration_matcher_hit(engine):
    """Pipeline should exit at Matcher Layer if high similarity."""
    # Seed local memory
    engine.matcher.memory = [
        {"category": "Expenses:Transport", "description": "Shell", "embedding": MOCK_EMB_GAS}
    ]
    
    # Input "BP Fuel" -> Cleaned -> Matcher
    # Reuse Cleaner logic (no changes needed for this string)
    
    with patch('ollama.embeddings', return_value={'embedding': MOCK_EMB_GAS}): # Simulate high sim
        res = engine.run("BP Fuel Service")
        assert res == "Expenses:Transport"

def test_integration_agent_fallback(engine):
    """Pipeline should fall through to Agent if no Rule/Match."""
    engine.matcher.memory = [] # Emptyish
    
    # Input "UniqueStartUp" -> No Rule, No Match
    with patch('ollama.embeddings', return_value={'embedding': MOCK_EMB_UNK}):
        # Agent return
        mock_decision = MagicMock()
        mock_decision.category = "Expenses:Business:Services"
        mock_decision.reasoning = "It sounds like a business."
        
        with patch.object(engine.agent.client.chat.completions, 'create', return_value=mock_decision) as mock_llm:
            res = engine.run("UniqueStartUp Invoice 123")
            
            assert res == "Expenses:Business:Services"
            mock_llm.assert_called_once()
            
            # Verify Self-Healing (Matcher memory update)
            assert len(engine.matcher.memory) == 1
            assert engine.matcher.memory[0]['category'] == "Expenses:Business:Services"
            assert engine.matcher.memory[0]['description'] == "UniqueStartUp Invoice 123"

def test_integration_empty_input(engine):
    assert engine.run(None) == UNKNOWN_CATEGORY
    assert engine.run("   ") == UNKNOWN_CATEGORY
