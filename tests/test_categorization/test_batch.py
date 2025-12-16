import pytest
from unittest.mock import MagicMock
import pandas as pd
from rosetta.logic.categorization.batch import BatchCategorizer

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.chat.completions.create = MagicMock()
    return client

def test_batch_categorizer_categorize(mock_client):
    batcher = BatchCategorizer(client=mock_client)
    
    # Mock response object 
    # Needs to match the structure expected by batcher (CategoryMapping)
    # Since batcher uses response_model, the return value of create must act like that model
    mock_mapping = MagicMock()
    mock_mapping.mapping = {"Uber": "Transport", "Spar": "Groceries"}
    mock_client.chat.completions.create.return_value = mock_mapping
    
    merchants = ["Uber", "Spar", "Uber"]
    mapping = batcher.categorize_batch(merchants, "prompt")
    
    assert mapping == {"Uber": "Transport", "Spar": "Groceries"}
    # Verify we sent unique list
    args, kwargs = mock_client.chat.completions.create.call_args
    # Check messages content roughly
    assert "Uber" in kwargs['messages'][1]['content']
    assert "Spar" in kwargs['messages'][1]['content']

def test_batch_categorizer_map():
    batcher = BatchCategorizer(client=None)
    mapping = {"Uber": "Transport", "Spar": "Groceries"}
    
    data = {"merchant_clean": ["Uber", "Spar", "Unknown", "Uber"]}
    df = pd.DataFrame(data)
    
    df_result = batcher.map_categories(df, mapping)
    
    assert "Category" in df_result.columns
    assert df_result.iloc[0]["Category"] == "Transport"
    assert df_result.iloc[1]["Category"] == "Groceries"
    assert df_result.iloc[2]["Category"] == "Uncategorized"
    assert df_result.iloc[3]["Category"] == "Transport"
