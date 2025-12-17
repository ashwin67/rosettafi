import pytest
from unittest.mock import MagicMock
from rosetta.logic.categorization.segmentation import LLMSegmenter
from rosetta.models import TokenizedParts, BatchResult

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.chat.completions.create = MagicMock()
    return client

def test_segmentation_batch(mock_client):
    segmenter = LLMSegmenter(client=mock_client, model="mock")
    
    # Mock Response
    mock_batch = BatchResult(results=[
        TokenizedParts(parts=["TRTP", "iDEAL", "bol.com"]),
        TokenizedParts(parts=["BEA", "UBER", "EATS", "NR:123"])
    ])
    mock_client.chat.completions.create.return_value = mock_batch
    
    texts = [
        "/TRTP/iDEAL/NAME/bol.com/REMI/",
        "BEA UBER EATS NR:123"
    ]
    
    results = segmenter.tokenize_batch(texts, "prompt")
    
    assert len(results) == 2
    assert results[0] == ["TRTP", "iDEAL", "bol.com"]
    assert results[1] == ["BEA", "UBER", "EATS", "NR:123"]
    assert "bol.com" in results[0]

def test_segmentation_empty(mock_client):
    segmenter = LLMSegmenter(client=mock_client, model="mock")
    results = segmenter.tokenize_batch([], "prompt")
    assert results == []
    mock_client.chat.completions.create.assert_not_called()
