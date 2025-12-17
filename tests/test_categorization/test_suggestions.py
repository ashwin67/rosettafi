import pytest
import pandas as pd
from unittest.mock import MagicMock
from rosetta.logic.categorization.engine import CategorizationEngine, Phonebook
from rosetta.models import MerchantEntity

@pytest.fixture
def categorization_engine():
    # Mock the LLM Segmenter to avoid API calls
    engine = CategorizationEngine(model_name="dummy")
    engine.segmenter = MagicMock()
    # Mock Tokenization to just split by space
    engine.segmenter.tokenize_batch.side_effect = lambda texts, prompt: [t.split() for t in texts]
    
    # Setup a clean Phonebook with known entities
    engine.phonebook.entities = {}
    engine.phonebook.alias_index = {}
    
    # Register "Albert Heijn"
    engine.register_entity(
        name="Albert Heijn", 
        category="Groceries", 
        alias="AH to go"
    )
    
    # Register "Google" (no aliases initially)
    engine.register_entity(
        name="Google", 
        category="Software"
    )
    
    return engine

def test_discovery_unknown_with_suggestion(categorization_engine):
    """
    Test that 'AH Amsterdam' (unknown) is suggested as 'Albert Heijn' 
    because it mimics the known alias 'AH to go' or canonical 'Albert Heijn'.
    """
    # Create a DataFrame with unknown merchants
    df = pd.DataFrame({
        "Description": ["AH Amsterdam 1234", "Unknown Bakery"],
        # Mocking what the segmenter would produce in 'merchant_clean'
        "merchant_clean": ["AH to go", "De Bakker"] 
    })
    
    # Force 'Entity' column to be NaN so discover_entities picks them up
    df['Entity'] = None
    
    results = categorization_engine.discover_entities(df)
    
    # Check Result 1: "AH to go" -> Should be EXACT match or very high suggestion
    # Wait, if it was an Exact Match, 'resolve_row' would have caught it.
    # But here we are testing 'discover_entities' on rows that *failed* resolution.
    # So let's use a variation that matches fuzzily.
    
    # Scenario: The pipeline ran, "AH Amsterdam" was cleaned to "AH Amsterdam".
    # "AH Amsterdam" is NOT in the phonebook.
    # But "Albert Heijn" IS in the phonebook.
    
    df = pd.DataFrame({
        "Description": ["Misc Trans"],
        "Entity": [None],
        "merchant_clean": ["Albert Hijn"] # Typo
    })
    
    results = categorization_engine.discover_entities(df)
    
    assert len(results) == 1
    res = results[0]
    
    # "Albert Hijn" should match "Albert Heijn"
    assert res['raw'] == "Albert Hijn"
    assert res['suggested_name'] == "Albert Heijn"
    assert res['confidence'] > 0.8
    
def test_discovery_no_suggestion(categorization_engine):
    """
    Test that a completely random string gets no high confidence suggestion.
    """
    df = pd.DataFrame({
        "Description": ["Misc"],
        "Entity": [None],
        "merchant_clean": ["Xylophone 999"]
    })
    
    results = categorization_engine.discover_entities(df)
    
    assert len(results) == 1
    res = results[0]
    
    assert res['raw'] == "Xylophone 999"
    # It might match something with very low score, or None if threshold is strict.
    # Our implementation returns the top 1 regardless of score, but score should be low.
    
    if res['suggested_name']:
        assert res['confidence'] < 0.4

def test_register_creates_lookup(categorization_engine):
    """
    Test that registering an entity actually makes it resolve-able.
    """
    categorization_engine.register_entity("Netflix", "Entertainment", "Netflix.com")
    
    entity = categorization_engine.resolver.resolve("Netflix.com")
    assert entity is not None
    assert entity.canonical_name == "Netflix"
    assert entity.default_category == "Entertainment"
