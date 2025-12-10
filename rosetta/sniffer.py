import pandas as pd
import io
from .config import get_logger

logger = get_logger(__name__)

def sniff_header_row(file_path_or_buffer) -> pd.DataFrame:
    """
    Reads the first 20 rows of a file to heuristically identify the valid header row.
    Returns a DataFrame loaded with the correct header.
    """
    logger.info("Stage 1: Sniffing for header...")
    
    # Read first 20 lines without header
    if isinstance(file_path_or_buffer, str):
        # Determine if file path or string content
        try:
            if file_path_or_buffer.endswith('.csv') or file_path_or_buffer.endswith('.txt'):
                 # It's a file path
                 with open(file_path_or_buffer, 'r') as f:
                    lines = [next(f) for _ in range(20)]
            else:
                 # It's content
                 raise Exception("Not a file path")
        except Exception:
             # Treat as string buffer
            lines = file_path_or_buffer.splitlines()[:20]
            file_path_or_buffer = io.StringIO(file_path_or_buffer)
    elif isinstance(file_path_or_buffer, io.StringIO):
         lines = file_path_or_buffer.getvalue().splitlines()[:20]
         file_path_or_buffer.seek(0)
    else:
        # Fallback for other file-like objects
        lines = [line.decode('utf-8') for line in file_path_or_buffer.readlines()[:20]]
        file_path_or_buffer.seek(0)

    keywords = ['date', 'booking', 'transaction', 'amount', 'debit', 'credit', 'description', 'memo', 'payee', 'valuta']
    
    best_row_idx = 0
    max_score = -1

    for idx, line in enumerate(lines):
        score = 0
        line_lower = line.lower()
        for kw in keywords:
            if kw in line_lower:
                score += 1
        
        # Heuristic: Valid headers usually have delimiters (comma/semicolon)
        if ',' in line or ';' in line:
            score += 1
            
        if score > max_score:
            max_score = score
            best_row_idx = idx

    logger.info(f"Identified header at row index: {best_row_idx} (Score: {max_score})")
    
    # Reload dataframe with correct header
    # We manually slice the lines to avoid ambiguities with read_csv's header/skip_blank_lines logic
    # Re-reading the whole file log properly
    
    all_lines = []
    if isinstance(file_path_or_buffer, str) and not '\n' in file_path_or_buffer and (file_path_or_buffer.endswith('.csv') or file_path_or_buffer.endswith('.txt')):
        # File path
        with open(file_path_or_buffer, 'r') as f:
            all_lines = f.readlines()
    elif isinstance(file_path_or_buffer, io.StringIO):
        file_path_or_buffer.seek(0)
        all_lines = file_path_or_buffer.readlines()
    elif isinstance(file_path_or_buffer, str):
        all_lines = file_path_or_buffer.splitlines(keepends=True)
             
    clean_content = "".join(all_lines[best_row_idx:])
    
    # Use io.StringIO to create a buffer
    df = pd.read_csv(io.StringIO(clean_content), sep=None, engine='python')
    df.columns = df.columns.str.strip()
    
    return df
