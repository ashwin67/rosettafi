from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
from rosetta.rules import RulesEngine
from rosetta.validator import validate_data
from rosetta.utils import get_logger
from rosetta.data.constants import UNKNOWN_CATEGORY

logger = get_logger(__name__)

if __name__ == "__main__":
    
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description='RosettaFi - Financial Data Ingestion Pipeline')
    parser.add_argument('file', nargs='?', help='Path to the input bank export file (CSV, TXT, Excel)')
    args = parser.parse_args()

    input_source = None
    
    if args.file:
        if not os.path.exists(args.file):
            logger.error(f"File not found: {args.file}")
            sys.exit(1)
        logger.info(f"Processing input file: {args.file}")
        input_source = args.file
    else:
        logger.info("No input file provided. Using built-in Mock Data.")
        # 1. Create Mock Messy CSV (European Format: Semicolons, Comma Decimals)
        mock_csv_data = """
        Bank of Antigravity - Account Statement
        Generated: 2023-10-27
        Account: 123-456-789
        
        Disclaimer: This is not legal advice.
        
        Transaction Date;Valuta Date;Booking Text;Betrag EUR;Balance
        01.10.2023;01.10.2023;Supermarket Purchase;-50,20;1.000,00
        02.10.2023;02.10.2023;Monthly Salary;3.500,00;4.500,00
        05.10.2023;05.10.2023;Coffee Shop;-4,50;4.495,50
        """
        input_source = mock_csv_data
    
    logger.info("Initializing Sniffer & Mapper Engine (Refactored)...")
    
    # Stage 1: Sniffer
    clean_df = sniff_header_row(input_source)
    print("\n--- Stage 1 Output (Sniffed DataFrame) ---")
    print(clean_df.head())
    
    # Stage 2: Mapper (Get Schema)
    mapping = get_column_mapping(clean_df)
    print("\n--- Stage 2 Output (Column Mapping Configuration) ---")
    print(mapping.model_dump_json(indent=2))
    
    # Stage 4: Rules Engine (Apply Logic)
    engine = RulesEngine(mapping)
    normalized_df = engine.apply(clean_df)
    print("\n--- Stage 4 Output (Normalized DataFrame) ---")
    print(normalized_df[['date', 'amount', 'transaction_id']].head())
    
    # Stage 5: Entity Resolution Engine
    from rosetta.logic.categorization.engine import CategorizationEngine
    categorizer = CategorizationEngine()
    
    logger.info("Entity Resolution Engine started...")
    
    # Run the engine on the dataframe
    # This now adds 'Entity', 'Category', and 'merchant_clean' columns
    normalized_df = categorizer.run(normalized_df, description_col="description")
    
    # For Ledger compatibility, map 'Category' to 'account'
    normalized_df['account'] = normalized_df['Category']
    
    categorized_df = normalized_df.copy()
    
    print("\n--- Stage 5 Output (Entity & Category) ---")
    print(categorized_df[['date', 'amount', 'Entity', 'Category']].head())
    
    # Check for Unknowns
    unknowns = categorizer.discover_entities(categorized_df)
    if unknowns:
        print(f"\n[!] Discovered {len(unknowns)} Unknown Entities.")
        print("--- Entitiy Resolution Mode ---")
        
        try:
            # Interactive Loop
            for item in unknowns:
                raw_name = item['raw']
                suggestion = item['suggested_name']
                confidence = item['confidence']
                
                print(f"\nEntity: '{raw_name}'")
                
                # Step 1: Identity
                default_identity = suggestion if suggestion else raw_name
                prompt_identity = f"Identify Entity [Enter for '{default_identity}']: "
                identity_input = input(prompt_identity).strip()
                
                if identity_input.lower() == 'skip':
                    continue
                
                final_name = identity_input if identity_input else default_identity
                
                # Step 2: Category
                # In a real app, we might check if 'final_name' is already a known entity to auto-fetch category.
                # For now, we ask for category unless it's a known suggestion we accepted.
                
                final_category = None
                
                # If we accepted a suggestion, we assume the user might be happy with existing category logic, 
                # but technically register_entity needs a category if it's new.
                # If we are just linking an alias to an existing entity (Suggestion accepted), 
                # we don't strictly need to re-enter category, but we can allow override.
                
                is_suggestion_accepted = (not identity_input) and suggestion
                
                if is_suggestion_accepted:
                    # User accepted suggestion. We can skip category prompt or offer override.
                    # Let's keep it simple: Link alias.
                    print(f" -> Linked to '{final_name}'")
                    categorizer.register_entity(final_name, category=None, alias=raw_name)
                    continue
                else:
                    # User entered a new name OR accepted raw_name as new entity.
                    # We need a category.
                    prompt_cat = f"Category [Enter for '{UNKNOWN_CATEGORY}']: "
                    cat_input = input(prompt_cat).strip()
                    final_category = cat_input if cat_input else UNKNOWN_CATEGORY
                    
                    categorizer.register_entity(final_name, final_category, alias=raw_name)
                    print(f" -> Registered '{final_name}' ({final_category})")

        except EOFError:
            print("\n[!] Non-interactive mode detected. Skipping remaining entities.")


        # Re-Run Categorization to apply new knowledge
        print("\nRe-running categorization with updated Phonebook...")
        normalized_df = categorizer.run(normalized_df, description_col="description")
        
        # Update categorized_df
        categorized_df = normalized_df.copy()
    
    # Stage 6: Ledger (Split Generation)
    from rosetta.logic.ledger import LedgerEngine
    ledger_engine = LedgerEngine()
    ledger_df = ledger_engine.generate_splits(categorized_df)
    
    print("\n--- Stage 6 Output (Double-Entry Splits) ---")
    print(ledger_df[['date', 'account', 'amount', 'currency']].head(10))
    
    # Stage 3: Validator (Strict Type Checks)
    final_df = validate_data(ledger_df)
    print("\n--- Stage 3 Output (Validated & Standardized Ledger) ---")
    print(final_df.head(10))
    
    # Save to Workspace Temp
    from rosetta.workspace import Workspace
    workspace = Workspace()
    output_path = workspace.temp_dir / "output.csv"
    final_df.to_csv(output_path, index=False)
    logger.info(f"Final output saved to: {output_path}")

    print("\nProcess Complete.")