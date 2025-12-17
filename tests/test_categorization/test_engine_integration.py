
import pytest
import pandas as pd
import json
from unittest.mock import MagicMock
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.models import TokenizedParts, BatchResult

@pytest.fixture
def mock_segmenter():
    """Mock the LLM Segmenter to avoid API costs."""
    segmenter = MagicMock()
    # default behavior
    segmenter.process_batch.return_value = BatchResult(results=[])
    return segmenter

@pytest.fixture
def engine(tmp_path, mock_segmenter):
    """
    Creates an engine with a temp phonebook and mock segmenter.
    """
    # Create temp phonebook path and init file
    pb_path = tmp_path / "merchants.json"
    with open(pb_path, 'w') as f:
        json.dump({"entities": {}}, f)
    
    # Init engine (uses default path first)
    ce = CategorizationEngine()
    
    # Patch the phonebook to use our temp path
    ce.phonebook.db_path = str(pb_path)
    ce.phonebook.entities = {}
    ce.phonebook.alias_index = {}
    
    # Patch Segmenter
    ce.segmenter = mock_segmenter
    
    return ce

def test_full_pipeline(engine):
    """
    Test the full run() method:
    Input DataFrame -> Tokenize (Mock) -> Resolve (Phonebook) -> Assign Category
    """
    # 1. Setup Data
    df = pd.DataFrame([
        {"description": "Albert Heijn Amsterdam", "amount": -10.0},
        {"description": "Unknown Shop", "amount": -5.0}
    ])
    
    # 2. Setup Phonebook Knowledge
    engine.register_entity("Albert Heijn", "Groceries", alias="Albert Heijn")
    
    # 3. Setup Mock Segmenter Response
    # The engine calls tokenize_batch which returns List[List[str]]
    mock_token_lists = [
        ["Albert Hijn"],  # Typo matches "Albert Heijn" > 0.85
        ["Unknown Shop"]               # Matches "Unknown Shop"
    ]
    engine.segmenter.tokenize_batch.return_value = mock_token_lists

    # 4. Run Pipeline
    result_df = engine.run(df, description_col="description")
    
    # 5. Verify Results
    
    # Row 0: "Albert Heijn Amsterdam" -> "Albert Heijn" (Groceries)
    row0 = result_df.iloc[0]
    assert row0["Entity"] == "Albert Heijn"
    assert row0["Category"] == "Groceries"
    
    # Row 1: "Unknown Shop" -> None (Uncategorized)
    row1 = result_df.iloc[1]
    assert pd.isna(row1["Entity"])
    assert row1["Category"] == "Uncategorized"

def test_pipeline_empty(engine):
    """Test resilience against empty input."""
    df = pd.DataFrame(columns=["description", "amount"])
    result_df = engine.run(df, description_col="description")
    assert result_df.empty
    assert "Entity" in result_df.columns
    assert "Category" in result_df.columns

def test_discover_entities(engine):
    """Test the discovery API (Suggestion Engine integration)."""
    # 1. Setup
    engine.register_entity("Picnic", "Groceries")
    
    # 2. DataFrame with a typo
    df = pd.DataFrame([
        {"merchant_clean": "Pcnic", "Entity": None}, # Typo
        {"merchant_clean": "Random", "Entity": None} # No match
    ])
    
    # 3. Discover
    unknowns = engine.discover_entities(df)
    
    # 4. Assertions
    # Should find 'Pcnic' matches 'Picnic'
    picnic_match = next((u for u in unknowns if u['raw'] == 'Pcnic'), None)
    assert picnic_match is not None
    assert picnic_match['suggested_name'] == "Picnic"
    assert picnic_match['confidence'] > 0.6
    
    # 'Random' should have no suggestion (below default threshold) or self-suggestion depending on logic
    # Our updated logic returns [] if no match above threshold.
    # engine.py logic: if default threshold is 0.6, 'Random' vs 'Picnic' is likely < 0.6.
    
    random_match = next((u for u in unknowns if u['raw'] == 'Random'), None)
    # Depending on threshold, clean random match might be empty
    if random_match:
         assert random_match['confidence'] <= 0.6
