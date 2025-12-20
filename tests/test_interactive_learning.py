
import pytest
import pandas as pd
import os
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.workspace import Workspace

def test_online_learning_simulation():
    """
    Simulates an interactive session to test if the engine can learn and
    immediately apply new knowledge.
    """
    # Ensure a clean state by deleting the phonebook before the test
    workspace = Workspace()
    phonebook_path = workspace.get_phonebook_path()
    if os.path.exists(phonebook_path):
        os.remove(phonebook_path)

    categorizer = CategorizationEngine()
    
    data = {
        "description": [
            "New Cool Cafe",
            "New Cool Cafe",
            "Some Other Place",
            "New Cool Cafe",
        ],
        "amount": [-10, -15, -20, -5],
        "date": pd.to_datetime("2024-01-01"),
        "transaction_id": [f"t{i}" for i in range(4)],
        "currency": ["EUR"] * 4,
        "account": ["Assets:Bank"] * 4,
    }
    df = pd.DataFrame(data)
    
    # --- First Pass ---
    # The system runs and discovers "New Cool Cafe" is unknown.
    categorized_df_pass1 = categorizer.resolve_and_categorize(df.copy(), description_col="description")
    
    unknowns = categorizer.discover_entities(categorized_df_pass1, "description")
    
    # Assert that "New Cool Cafe" was discovered as an unknown entity
    unknown_names = [u['raw'] for u in unknowns]
    assert "new cool cafe" in unknown_names
    assert unknowns[0]['original_examples'] is not None
    
    # --- User Interaction Simulation ---
    # The user now categorizes "New Cool Cafe".
    print("\nSimulating user categorizing 'New Cool Cafe' as 'Restaurants'")
    categorizer.register_entity("New Cool Cafe", "Restaurants", alias="new cool cafe")
    
    # --- Second Pass ---
    # The system re-runs categorization. The new knowledge should be applied.
    print("Re-running categorization to apply new knowledge...")
    categorized_df_pass2 = categorizer.resolve_and_categorize(df.copy(), description_col="description")
    
    pd.set_option('display.max_colwidth', None)
    print("\n--- Categorization Results after Learning ---")
    print(categorized_df_pass2[['description', 'Entity', 'Category']])

    # --- Assertions ---
    # Check that all "New Cool Cafe" transactions are now correctly categorized
    cafe_transactions = categorized_df_pass2[categorized_df_pass2['description'].str.contains("New Cool Cafe")]
    assert (cafe_transactions['Category'] == 'Restaurants').all(), \
        "Not all 'New Cool Cafe' transactions were categorized as 'Restaurants' after learning."
        
    # Check that the other transaction remains uncategorized
    other_transaction = categorized_df_pass2[categorized_df_pass2['description'].str.contains("Some Other Place", case=False, na=False)]
    assert other_transaction['Category'].iloc[0] == 'Uncategorized', \
        "The other transaction should have remained uncategorized."

    assert len(cafe_transactions) == 3, "Should have found 3 cafe transactions."


def test_learning_with_skip_scenario():
    """
    Tests a more complex interactive flow where the user categorizes one
    entity and skips another.
    """
    # Ensure a clean state
    workspace = Workspace()
    phonebook_path = workspace.get_phonebook_path()
    if os.path.exists(phonebook_path):
        os.remove(phonebook_path)
        
    categorizer = CategorizationEngine()
    
    data = {
        "description": [
            "Cafe Alpha", "Store Beta", "Cafe Alpha", "Store Beta", "Some other place"
        ],
        "amount": [-10, -50, -11, -55, -100],
        "date": pd.to_datetime("2024-01-01"),
        "transaction_id": [f"t{i}" for i in range(5)],
        "currency": ["EUR"] * 5,
        "account": ["Assets:Bank"] * 5,
    }
    df = pd.DataFrame(data)
    
    # --- Pass 1: Initial Discovery ---
    categorized_df = categorizer.resolve_and_categorize(df.copy(), description_col="description")
    unknowns = categorizer.discover_entities(categorized_df, "description")
    unknown_names = sorted([u['raw'] for u in unknowns])
    assert unknown_names == ["cafe alpha", "some other place", "store beta"]
    
    # --- Pass 2: User categorizes 'Cafe Alpha' ---
    print("\nSimulating user categorizing 'Cafe Alpha'")
    categorizer.register_entity("Cafe Alpha", "Restaurants", alias="cafe alpha")
    categorized_df = categorizer.resolve_and_categorize(df.copy(), description_col="description")
    
    # Assert 'Cafe Alpha' is learned
    assert (categorized_df[categorized_df['description'] == "Cafe Alpha"]['Category'] == 'Restaurants').all()

    # Discover remaining unknowns
    unknowns = categorizer.discover_entities(categorized_df, "description")
    unknown_names = sorted([u['raw'] for u in unknowns])
    assert unknown_names == ["some other place", "store beta"]

    # --- Pass 3: User skips 'Store Beta' ---
    print("\nSimulating user skipping 'Store Beta'")
    categorizer.register_entity("Store Beta", "Skipped", alias="store beta")
    categorized_df = categorizer.resolve_and_categorize(df.copy(), description_col="description")
    
    # Assert 'Store Beta' is now 'Skipped'
    assert (categorized_df[categorized_df['description'] == "Store Beta"]['Category'] == 'Skipped').all()
    
    # --- Final Check ---
    # Discover remaining unknowns. Only "Some other place" should be left.
    unknowns = categorizer.discover_entities(categorized_df, "description")
    unknown_names = sorted([u['raw'] for u in unknowns])
    assert unknown_names == ["some other place"]

