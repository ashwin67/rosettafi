import pytest
import pandas as pd
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.data.constants import UNKNOWN_CATEGORY

@pytest.fixture
def engine():
    """
    Creates a clean engine for each test.
    """
    ce = CategorizationEngine()
    # Clear the phonebook for test isolation
    ce.phonebook.entities = {}
    ce.phonebook.alias_index = {}
    return ce

def test_full_pipeline(engine):
    """
    Test the full resolve_and_categorize() method.
    """
    # 1. Setup Data
    df = pd.DataFrame([
        {"description": "Albert Heijn Amsterdam", "amount": -10.0},
        {"description": "Unknown Shop", "amount": -5.0}
    ])
    
    # 2. Setup Phonebook Knowledge
    engine.register_entity("Albert Heijn", "Groceries", alias="albert heijn")
    
    # 3. Run Pipeline
    result_df = engine.resolve_and_categorize(df, description_col="description")
    
    # 4. Verify Results
    # Row 0: "Albert Heijn Amsterdam" should resolve to "Albert Heijn" (Groceries)
    row0 = result_df.iloc[0]
    assert row0["Entity"] == "Albert Heijn"
    assert row0["Category"] == "Groceries"
    
    # Row 1: "Unknown Shop" -> None (Uncategorized)
    row1 = result_df.iloc[1]
    assert pd.isna(row1["Entity"])
    assert row1["Category"] == UNKNOWN_CATEGORY

def test_pipeline_empty(engine):
    """Test resilience against empty input."""
    df = pd.DataFrame(columns=["description", "amount"])
    result_df = engine.resolve_and_categorize(df, description_col="description")
    assert result_df.empty
    assert "Entity" in result_df.columns
    assert "Category" in result_df.columns

def test_discover_entities_with_typo(engine):
    """Test the discovery API with a typo."""
    # 1. Setup
    engine.register_entity("Picnic", "Groceries")
    
    # 2. DataFrame with a typo in the description
    # The 'merchant_clean' will be a lowercase version of this.
    df = pd.DataFrame([
        {"description": "picnic deliver", "Entity": None},
    ])
    
    # 3. Discover
    unknowns = engine.discover_entities(df, description_col="description")
    
    # 4. Assertions
    assert len(unknowns) == 1
    picnic_match = unknowns[0]
    
    assert picnic_match['raw'] == 'picnic deliver' # from merchant_clean
    assert picnic_match['suggested_name'] == "Picnic"
    assert picnic_match['confidence'] >= 0.6
