from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
from rosetta.rules import RulesEngine
from rosetta.validator import validate_data
from rosetta.config import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    
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
    
    logger.info("Initializing Sniffer & Mapper Engine (Refactored)...")
    
    # Stage 1: Sniffer
    clean_df = sniff_header_row(mock_csv_data)
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
    
    # Stage 3: Validator (Strict Type Checks)
    final_df = validate_data(normalized_df)
    print("\n--- Stage 3 Output (Validated & Standardized) ---")
    print(final_df.head())
    print("\nProcess Complete.")
