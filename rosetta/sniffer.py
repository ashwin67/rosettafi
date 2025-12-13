import pandas as pd
import io
from .config import get_logger
from rosetta.logic.sniffer_logic import detect_header_by_density, detect_header_by_keywords
from rosetta.data.sniffer_constants import SNIFF_WINDOW_SIZE

logger = get_logger(__name__)

def sniff_header_row(file_path_or_buffer) -> pd.DataFrame:
    """
    Reads the first 20 rows of a file to heuristically identify the valid header row.
    Returns a DataFrame loaded with the correct header.
    """
    logger.info("Stage 1: Sniffing for header...")
    
    # Detect Excel files
    is_excel = False
    if isinstance(file_path_or_buffer, str) and (file_path_or_buffer.endswith('.xlsx') or file_path_or_buffer.endswith('.xls')):
        is_excel = True
        logger.info("Detected Excel file. Converting to CSV buffer for analysis...")
        # Read Excel w/o header initially to capture everything
        temp_df = pd.read_excel(file_path_or_buffer, header=None)
        # Convert to CSV string buffer for the rest of the existing logic
        csv_buffer = io.StringIO()
        temp_df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)
        
        # Override input to be this new CSV buffer
        file_path_or_buffer = csv_buffer

    # Read lines for analysis
    lines = []
    
    # Logic to populate 'lines' based on input type
    # We need to be careful to not consume the buffer permanently if possible, or reset it.
    
    if isinstance(file_path_or_buffer, str):
        # File path (CSV/TXT)
        if not '\n' in file_path_or_buffer and (file_path_or_buffer.endswith('.csv') or file_path_or_buffer.endswith('.txt')):
             with open(file_path_or_buffer, 'r') as f:
                lines = [next(f) for _ in range(SNIFF_WINDOW_SIZE)]
        else:
             # String content
             lines = file_path_or_buffer.splitlines()[:SNIFF_WINDOW_SIZE]
             # If it was a string content, we need to wrap it for later reading if it's not a file path
             if not isinstance(file_path_or_buffer, io.StringIO):
                  file_path_or_buffer = io.StringIO(file_path_or_buffer)

    elif isinstance(file_path_or_buffer, io.StringIO):
         lines = file_path_or_buffer.getvalue().splitlines()[:SNIFF_WINDOW_SIZE]
         file_path_or_buffer.seek(0)
    else:
        # Fallback for other file-like objects (e.g. valid bytes buffer if we supported it, but mainly text io)
        # Assuming text mode for now based on existing code
        try:
            lines = [line for line in file_path_or_buffer.readlines()[:SNIFF_WINDOW_SIZE]]
            file_path_or_buffer.seek(0)
        except Exception:
             # If readlines fails (e.g. bytes), try decoding? 
             # For now adhering to existing logic which seemed to assume text.
             pass

    # Strategy 1: Data Density Heuristic
    best_row_idx = detect_header_by_density(lines)
    
    # Strategy 2: Keyword Fallback
    if best_row_idx is None:
        best_row_idx = detect_header_by_keywords(lines)

    logger.info(f"Final Header Decision: Row {best_row_idx}")
    
    # Load Dataframe
    # We need to read the FULL content now, starting from best_row_idx
    
    all_lines = []
    if is_excel:
        file_path_or_buffer.seek(0)
        all_lines = file_path_or_buffer.readlines()
    elif isinstance(file_path_or_buffer, io.StringIO):
        file_path_or_buffer.seek(0)
        all_lines = file_path_or_buffer.readlines()
    elif isinstance(file_path_or_buffer, str):
         # If it's a file path
         if not '\n' in file_path_or_buffer and (file_path_or_buffer.endswith('.csv') or file_path_or_buffer.endswith('.txt')):
            with open(file_path_or_buffer, 'r') as f:
                all_lines = f.readlines()
         else:
            # It's content, but we wrapped it in StringIO earlier if it was passed as string logic? 
            # Actually, let's just split the string again if it is a string.
             all_lines = file_path_or_buffer.splitlines(keepends=True)
    else:
         # File object
         file_path_or_buffer.seek(0)
         all_lines = file_path_or_buffer.readlines()

    if not all_lines:
        logger.warning("No content found to create DataFrame.")
        return pd.DataFrame()

    clean_content = "".join(all_lines[best_row_idx:])
    
    try:
        # Use on_bad_lines='skip' to handle rows with extra/missing separators gracefully
        df = pd.read_csv(io.StringIO(clean_content), sep=None, engine='python', on_bad_lines='skip')
    except pd.errors.EmptyDataError:
        logger.warning("Empty data after header slice.")
        return pd.DataFrame()
        
    df.columns = df.columns.str.strip()
    
    return df
