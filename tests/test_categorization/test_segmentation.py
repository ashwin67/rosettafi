import pytest
from unittest.mock import MagicMock
from rosetta.logic.categorization.segmentation import LLMSegmenter, SegmentationBatch, SegmentedTransaction

@pytest.fixture
def mock_client():
    client = MagicMock()
    client.chat.completions.create = MagicMock()
    return client

def test_segmentation_batch(mock_client):
    segmenter = LLMSegmenter(client=mock_client)
    
    # Mock Response
    mock_batch = SegmentationBatch(items=[
        SegmentedTransaction(id=0, keywords=["/TRTP/", "iDEAL"], descriptions=["bol.com"]),
        SegmentedTransaction(id=1, keywords=["BEA", "NR:123"], descriptions=["UBER EATS"])
    ])
    mock_client.chat.completions.create.return_value = mock_batch
    
    texts = [
        "/TRTP/iDEAL/NAME/bol.com/REMI/",
        "BEA UBER EATS NR:123"
    ]
    
    results = segmenter.segment_batch(texts, "prompt")
    
    assert len(results) == 2
    assert results[0]['descriptions'] == ["bol.com"]
    assert results[1]['descriptions'] == ["UBER EATS"]
    assert "/TRTP/" in results[0]['keywords']

def test_segmentation_empty(mock_client):
    segmenter = LLMSegmenter(client=mock_client)
    results = segmenter.segment_batch([], "prompt")
    assert results == []
    mock_client.chat.completions.create.assert_not_called()
