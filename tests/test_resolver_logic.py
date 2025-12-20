import pytest
from rosetta.logic.categorization.engine import CategorizationEngine

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

def test_substring_match_should_behave(engine):
    """
    Tests that a known entity name is found inside a longer string.
    This is the key test for the 'RAG' feature. If this fails, the
    resolver's substring logic is flawed.
    """
    engine.phonebook.register_entity("Albert Heijn", "Groceries", aliases=["albert heijn"])
    
    candidate = "bea, betaalpas albert heijn 1657,pas142 nr:lx035n"
    
    # The `resolve` method should find this via the substring match
    resolved_entity = engine.resolver.resolve(candidate)
    
    assert resolved_entity is not None
    assert resolved_entity.canonical_name == "Albert Heijn"

def test_fuzzy_match_negative_cases(engine):
    """
    Tests that a clearly different string does not get a high-confidence
    fuzzy match to an existing entity.
    """
    engine.phonebook.register_entity("Gebroeders van Hex", "Groceries", aliases=["gebroeders van hex"])
    
    candidate = "bea, betaalpas dbanyan,pas142 nr:c080vs"
    
    # We use find_similar because resolve() would just return None.
    # We want to see what score the bad suggestion is getting.
    matches = engine.resolver.find_similar(candidate)
    
    if matches:
        # If a match is found, its confidence should be very low.
        best_match_name, best_match_score = matches[0]
        assert best_match_score < 0.5 # 50% confidence is way too high for this

