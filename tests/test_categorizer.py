import pytest
import os
import json
from unittest.mock import patch, MagicMock
from rosetta.categorizer import HybridCategorizer
from rosetta.data.constants import UNKNOWN_CATEGORY, SIMILARITY_THRESHOLD

# Mock Vectors (Length 3 for simplicity)
MOCK_EMBEDDING_GAS = [0.9, 0.1, 0.0] 
MOCK_EMBEDDING_FOOD = [0.1, 0.9, 0.0]
MOCK_EMBEDDING_UNK = [0.0, 0.0, 0.9] # Orthogonal to others

@pytest.fixture
def clean_workspace(tmp_path):
    """Mocks workspace paths to use a temp dir for memory.json"""
    memory_file = tmp_path / "category_memory.json"
    
    with patch('rosetta.workspace.Workspace.get_memory_path', return_value=str(memory_file)):
        yield str(memory_file)

@pytest.fixture
def categorizer(clean_workspace):
    # Mock seeding embeddings during init
    with patch('ollama.embeddings') as mock_seed:
        # Return generic vector for seeds
        mock_seed.return_value = {'embedding': [0.1, 0.1, 0.1]} 
        cat = HybridCategorizer()
        return cat

def test_initialization_seeds_memory(clean_workspace):
    """Test that memory file is created and seeded on init."""
    assert not os.path.exists(clean_workspace)
    
    with patch('ollama.embeddings', return_value={'embedding': [0.1, 0.1, 0.1]}):
        HybridCategorizer()
        
    assert os.path.exists(clean_workspace)
    with open(clean_workspace) as f:
        data = json.load(f)
        assert len(data) > 0
        assert data[0]['category'] in ["Groceries", "Rent", "Salary"] # Check defaults

def test_fast_path_exact_match(categorizer):
    """Test exact match logic (High similarity)."""
    # Pre-load memory with a specific known vector
    categorizer.memory = [
        {"category": "Gas", "description": "Shell", "embedding": MOCK_EMBEDDING_GAS}
    ]
    
    # Mock embedding for input "Shell Gas" -> Close to Gas vector
    with patch('ollama.embeddings', return_value={'embedding': MOCK_EMBEDDING_GAS}):
        cat = categorizer.categorize("Shell Gas")
        assert cat == "Gas"

def test_fast_path_semantic_match(categorizer):
    """Test semantic match (BP -> Gas)."""
    categorizer.memory = [
        {"category": "Gas", "description": "Shell", "embedding": MOCK_EMBEDDING_GAS}
    ]
    
    # Input vector is slightly different but highly similar
    # Cosine Distance ~ 0.
    input_vec = [0.89, 0.11, 0.0] 
    
    with patch('ollama.embeddings', return_value={'embedding': input_vec}):
        cat = categorizer.categorize("BP Fuel Station")
        assert cat == "Gas"

def test_slow_path_new_concept(categorizer):
    """Test low similarity triggers LLM."""
    categorizer.memory = [
        {"category": "Gas", "description": "Shell", "embedding": MOCK_EMBEDDING_GAS}
    ]
    
    # Input "Netflix" -> Orthogonal to Gas
    with patch('ollama.embeddings', return_value={'embedding': MOCK_EMBEDDING_UNK}) as mock_emb:
        with patch('ollama.chat') as mock_chat:
            # LLM returns new category
            mock_chat.return_value = {'message': {'content': 'Subscriptions'}}
            
            cat = categorizer.categorize("Netflix")
            
            # Assertions
            assert cat == "Subscriptions"
            mock_chat.assert_called_once()
            
            # Verify memory updated
            assert len(categorizer.memory) == 2
            assert categorizer.memory[-1]['category'] == "Subscriptions"

def test_resilience_ollama_down(categorizer):
    """Test graceful failure if Ollama explodes."""
    with patch('ollama.embeddings', side_effect=Exception("Connection Refused")):
        cat = categorizer.categorize("Anything")
        assert cat == UNKNOWN_CATEGORY

def test_empty_input(categorizer):
    assert categorizer.categorize(None) == UNKNOWN_CATEGORY
    assert categorizer.categorize("   ") == UNKNOWN_CATEGORY
