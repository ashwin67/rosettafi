import pytest
import pandas as pd
from unittest.mock import MagicMock
from rosetta.logic.categorization.engine import CategorizationEngine, Phonebook
from rosetta.models import MerchantEntity

@pytest.fixture
def categorization_engine():
    engine = CategorizationEngine()
    
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
    Tests that a description with a typo will generate a suggestion.
    """
    # Scenario: The description "Albert Hijn" has a typo but should be
    # suggested as "Albert Heijn".
    df = pd.DataFrame({
        "description": ["Albert Hijn"],
    })
    
    results = categorization_engine.discover_entities(df, description_col="description")
    
    assert len(results) == 1
    res = results[0]
    
    # The 'raw' value is the cleaned, lowercased version of the description
    assert res['raw'] == "albert hijn"
    assert res['suggested_name'] == "Albert Heijn"
    assert res['confidence'] > 0.8
    
def test_discovery_no_suggestion(categorization_engine):
    """
    Test that a completely random string gets no high confidence suggestion.
    """
    df = pd.DataFrame({
        "description": ["Xylophone 999"],
    })
    
    results = categorization_engine.discover_entities(df, description_col="description")
    
    assert len(results) == 1
    res = results[0]
    
    assert res['raw'] == "xylophone 999"
    # A suggestion might be found, but its confidence should be very low.
    # Or no suggestion might be found if it's below the resolver's threshold.
    if res['suggested_name']:
        assert res['confidence'] < 0.4
    else:
        assert res['suggested_name'] is None

def test_register_creates_lookup(categorization_engine):
    """
    Test that registering an entity actually makes it resolve-able.
    """
    categorization_engine.register_entity("Netflix", "Entertainment", alias="netflix.com")
    
    # The resolver works with lowercase
    entity = categorization_engine.resolver.resolve("netflix.com")
    assert entity is not None
    assert entity.canonical_name == "Netflix"
    assert entity.default_category == "Entertainment"
