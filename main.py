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
    
    logger.info("Starting Interactive Entity Resolution...")
    
    # Use the interactive, generator-based runner
    # This processes the dataframe in batches and yields unknowns for feedback
    interactive_runner = categorizer.run_interactive(normalized_df, "description", batch_size=50)
    
    for unknowns_batch in interactive_runner:
        # Pass the description column name to discover_entities
        unknown_details = categorizer.discover_entities(normalized_df[normalized_df['Category'] == UNKNOWN_CATEGORY], "description")

        if not unknown_details:
            continue

        print(f"\n[!] Discovered {len(unknown_details)} new unique unknown entities.")
        print("--- Entity Resolution Mode ---")
        
        try:
            # Loop through the unknowns found in the current batch
            for item in unknown_details:
                raw_name = item['raw']
                suggestion = item['suggested_name']
                original_examples = item['original_examples']
                
                print(f"\nNext Entity: '{raw_name}'")
                if original_examples:
                    print(f"  e.g. Original Description: \"{original_examples[0]}\"")
                
                # Step 1: Identity
                default_identity = suggestion if suggestion else raw_name
                prompt_identity = f"Identify Entity [Enter for '{default_identity}', 'skip' to ignore]: "
                identity_input = input(prompt_identity).strip()
                
                if identity_input.lower() == 'skip':
                    print(f" -> Skipping '{raw_name}' for this session.")
                    categorizer.register_entity(raw_name, category="Skipped", alias=raw_name)
                    continue # Move to the next item in the batch
                
                final_name = identity_input if identity_input else default_identity
                
                # Step 2: Category
                existing_entity = categorizer.phonebook.find_entity_by_alias(final_name)
                final_category = None
                if existing_entity and existing_entity.default_category != UNKNOWN_CATEGORY:
                    prompt_cat = f"Category [Enter for '{existing_entity.default_category}']: "
                else:
                    prompt_cat = f"Category [Enter for '{UNKNOWN_CATEGORY}']: "
                
                cat_input = input(prompt_cat).strip()
                
                if cat_input:
                    final_category = cat_input
                elif existing_entity:
                    final_category = existing_entity.default_category
                else:
                    final_category = UNKNOWN_CATEGORY
                    
                categorizer.register_entity(final_name, final_category, alias=raw_name)
                print(f" -> Registered '{final_name}' ({final_category})")

        except (EOFError, KeyboardInterrupt):
            print("\n[!] Exiting interactive mode. Continuing with current categorizations.")
            break # Exit the for loop over the generator
            
    print("\n[+] Interactive resolution complete.")
    
    # The normalized_df is now updated with the learned categories
    # Map 'Category' to 'account' for the ledger
    categorized_df = normalized_df
    categorized_df['account'] = categorized_df['Category']
    
    # Filter out any rows that were explicitly skipped
    categorized_df = categorized_df[categorized_df['account'] != 'Skipped']
    
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