import pytest
import os
import json
from unittest.mock import patch
from rosetta.logic.categorization.matcher import VectorMatcherLayer
from rosetta.data.constants import DEFAULT_CATEGORIES

@pytest.fixture
def clean_memory(tmp_path):
    mem_file = tmp_path / "mem.json"
    with patch('rosetta.workspace.Workspace.get_memory_path', return_value=str(mem_file)):
        yield str(mem_file)

def test_matcher_seeds_memory_on_init(clean_memory):
    """Test that memory file is created and seeded if missing."""
    assert not os.path.exists(clean_memory)
    
    with patch('ollama.embeddings', return_value={'embedding': [0.1, 0.1, 0.1]}):
        matcher = VectorMatcherLayer()
        
    assert os.path.exists(clean_memory)
    with open(clean_memory) as f:
        data = json.load(f)
        assert len(data) == len(DEFAULT_CATEGORIES)
        # Check one random default
        cats = [d['category'] for d in data]
        assert "Expenses:Groceries" in cats

def test_matcher_loads_existing_memory(clean_memory):
    """Test loading from existing JSON."""
    existing = [{"category": "Test", "description": "Test", "embedding": [0.9]}]
    with open(clean_memory, 'w') as f:
        json.dump(existing, f)
        
    matcher = VectorMatcherLayer()
    assert len(matcher.memory) == 1
    assert matcher.memory[0]['category'] == "Test"

def test_matcher_handles_corruption(clean_memory):
    """Test that corrupt JSON triggers backup and re-seed."""
    with open(clean_memory, 'w') as f:
        f.write("{corrupt_json")
        
    with patch('ollama.embeddings', return_value={'embedding': [0.1]}):
        matcher = VectorMatcherLayer()
        
    # Memory should be re-seeded
    assert len(matcher.memory) == len(DEFAULT_CATEGORIES)
    # Backup should exist
    assert os.path.exists(clean_memory + ".bak")
