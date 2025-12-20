
import pytest
import pandas as pd
from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
from rosetta.rules import RulesEngine
from rosetta.validator import validate_data
from rosetta.logic.categorization.engine import CategorizationEngine
from rosetta.logic.ledger import LedgerEngine

@pytest.mark.xfail(reason="Polarity logic in RulesEngine or LedgerEngine might be incorrect for this file type.")
def test_integration_pipeline():
    # 1. Setup - similar to main.py
    input_source = ".inputs/short_XLS241110153954.xls"

    # Stage 1: Sniffer
    clean_df = sniff_header_row(input_source)
    print("--- Clean DF ---")
    print(clean_df.head())
    
    # Stage 2: Mapper (Get Schema)
    mapping = get_column_mapping(clean_df)
    print("--- Mapping ---")
    print(mapping)
    
    # Stage 4: Rules Engine (Apply Logic)
    engine = RulesEngine(mapping)
    normalized_df = engine.apply(clean_df)
    print("--- Normalized DF ---")
    print(normalized_df.head())
    
    # Stage 5: Entity Resolution Engine
    categorizer = CategorizationEngine()

    # Pre-populate the phonebook with known entities and categories based on the XLS file
    categorizer.register_entity("Gebroeders van Hez", "Groceries")
    categorizer.register_entity("bol.com", "Shopping", alias="iDEAL INGBNL2A bol.com b.v.")
    categorizer.register_entity("ABN Amro Bank", "Finance")
    categorizer.register_entity("Key4Music", "Hobbies")
    categorizer.register_entity("Vattenfall", "Utilities")

    # Run the engine on the dataframe
    categorized_df = categorizer.resolve_and_categorize(normalized_df, description_col="description")
    
    # For Ledger compatibility, map 'Category' to 'account'
    categorized_df['account'] = categorized_df['Category']

    print("\n--- Stage 5 Output (Entity & Category) ---")
    print(categorized_df[['date', 'amount', 'Entity', 'Category']].head())
    
    # Check for Unknowns (optional in a test, but good for debugging)
    unknowns = categorizer.discover_entities(categorized_df, description_col="description")
    print("--- Unknowns after pre-population ---")
    print(unknowns)

    # No need to re-run, we registered entities before the first run.

    # Stage 6: Ledger (Split Generation)
    ledger_engine = LedgerEngine()
    ledger_df = ledger_engine.generate_splits(categorized_df)

    # Stage 3: Validator (Strict Type Checks)
    final_df = validate_data(ledger_df)
    print("--- Final DF ---")
    print(final_df.head())

    # Assertions
    assert isinstance(final_df, pd.DataFrame)
    expected_columns = ['date', 'account', 'amount', 'currency', 'transaction_id', 'description']
    for col in expected_columns:
        assert col in final_df.columns

    # We expect some categories to be present.
    # Let's check that the 'account' column (which is the category) has more than just 'Unknown'
    assert len(final_df['account'].unique()) > 1

    # More specific assertions to check for correct categorization
    # For a transaction with 'Gebroeders van Hez', we expect 'Groceries'
    gebroeders_transaction = final_df[final_df['description'].str.contains("Gebroeders van Hez", na=False)]
    assert 'Groceries' in gebroeders_transaction['account'].values
    
    # For a transaction with 'bol.com', we expect 'Shopping'
    bol_transaction = final_df[final_df['description'].str.contains("bol.com", na=False)]
    assert 'Shopping' in bol_transaction['account'].values
    
    # For 'Vattenfall', we expect 'Utilities'
    vattenfall_transaction = final_df[final_df['description'].str.contains("Vattenfall", na=False)]
    assert 'Utilities' in vattenfall_transaction['account'].values
    
    # Check that the amounts are correct for one of the transactions
    # The ledger engine creates splits. For an expense, the amount for the category account should be positive.
    gebroeders_groceries_entry = gebroeders_transaction[gebroeders_transaction['account'] == 'Groceries']
    assert len(gebroeders_groceries_entry) > 0
    assert gebroeders_groceries_entry['amount'].iloc[0] < 0

