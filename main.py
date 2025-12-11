from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
from rosetta.rules import RulesEngine
from rosetta.validator import validate_data
from rosetta.config import get_logger

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
    # Note: Rules engine handles parsing and normalization before validation
    engine = RulesEngine(mapping)
    normalized_df = engine.apply(clean_df)
    print("\n--- Stage 4 Output (Normalized DataFrame) ---")
    print(normalized_df[['date', 'amount', 'transaction_id']].head())
    
    # Stage 5: Categorizer (Classify Transactions)
    from rosetta.categorizer import Categorizer
    categorizer = Categorizer()
    categorized_df = categorizer.run_categorization(normalized_df, mapping)
    print("\n--- Stage 5 Output (Categorized DataFrame) ---")
    print(categorized_df[['date', 'amount', 'account']].head())
    
    # Stage 6: Ledger (Split Generation)
    from rosetta.ledger import LedgerEngine
    ledger_engine = LedgerEngine()
    ledger_df = ledger_engine.generate_splits(categorized_df)
    
    print("\n--- Stage 6 Output (Double-Entry Splits) ---")
    print(ledger_df[['date', 'account', 'amount', 'currency']].head(10))
    
    # Stage 3: Validator (Strict Type Checks)
    # We validate the final ledger splits
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
