import pandas as pd
import io
from rosetta.utils import get_logger
from typing import List, Optional
from rosetta.data.constants import SNIFF_WINDOW_SIZE, SNIFFER_HEADER_KEYWORDS as HEADER_KEYWORDS, DATA_DENSITY_THRESHOLD, DATA_SEPARATORS
# Removed logic import

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
                lines = []
                for i, line in enumerate(f):
                    if i >= SNIFF_WINDOW_SIZE:
                        break
                    lines.append(line)
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

def calculate_data_density(line: str) -> float:
    """
    Calculates the 'Data Density' score of a line.
    Strategy:
    1. Try to split by common separators (;, , \t).
    2. If splitting yields > 1 token, calculate ratio of tokens containing digits.
    3. Fallback to character-based density if no separators found (single column).
    """
    clean_line = line.strip()
    if not clean_line:
        return 0.0
    
    # Token-based Strategy
    # We check a few candidate separators.
    best_token_score = 0.0
    found_structure = False
    
    for sep in [';', ',', '\t', '|']:
        if sep in clean_line:
            tokens = clean_line.split(sep)
            if len(tokens) > 1:
                found_structure = True
                # Count tokens that look "numeric". 
                # Heuristic: Digit count >= Alpha count.
                # This avoids flagging "Column1" as numeric just because of '1'.
                numeric_tokens = 0
                for t in tokens:
                    digits = sum(c.isdigit() for c in t)
                    alphas = sum(c.isalpha() for c in t)
                    if digits > 0 and digits >= alphas:
                        numeric_tokens += 1
                
                score = numeric_tokens / len(tokens)
                if score > best_token_score:
                    best_token_score = score
    
    if found_structure:
        return best_token_score

    # Fallback: Character-based Strategy (Legacy)
    # Count digits
    digit_count = sum(c.isdigit() for c in clean_line)
    
    # Count separators
    separator_count = sum(1 for c in clean_line if c in DATA_SEPARATORS)
    
    total_len = len(clean_line)
    if total_len == 0:
        return 0.0
        
    density = (digit_count + separator_count) / total_len
    return density

def calculate_keyword_score(line: str) -> int:
    """
    Calculates a score based on presence of known header keywords.
    """
    line_lower = line.lower()
    score = 0
    for kw in HEADER_KEYWORDS:
        if kw in line_lower:
            score += 1
            
    # Bonus for common CSV delimiters in typical header rows
    if ',' in line or ';' in line:
        score += 1
        
    return score

def detect_header_by_density(lines: List[str]) -> Optional[int]:
    """
    Identifies the header row using the Data Density heuristic.
    Algorithm:
    1. Scan lines.
    2. Identify the first block of 'Data Rows' (high numeric density).
    3. The line immediately preceding this block is the candidate header.
    
    Returns index of header row, or None if pattern not found.
    """
    logger.info("Attempting matching via Data Density Heuristic...")
    
    # Calculate densities
    densities = [calculate_data_density(line) for line in lines]
    
    # Find first "Data Row"
    # Find first "Data Row"
    first_data_row_idx = -1
    for idx, density in enumerate(densities):
        logger.info(f"debug: Row {idx} density: {density:.3f}, Content: {lines[idx][:50]}...")
        if density >= DATA_DENSITY_THRESHOLD:
            # Check if likely real data (simple check: usually has some length)
            # AND must contain at least one digit (avoids separator lines like '-----')
            line_stripped = lines[idx].strip()
            has_digits = any(c.isdigit() for c in line_stripped)
            
            if len(line_stripped) > 5 and has_digits: 
                first_data_row_idx = idx
                break
    
    if first_data_row_idx > 0:
        # Candidate is the line before
        candidate_idx = first_data_row_idx - 1
        logger.info(f"Density Transition Found. Data starts at {first_data_row_idx}. Candidate Header at {candidate_idx}.")
        return candidate_idx
    elif first_data_row_idx == 0:
        # Data starts at 0, meaning no header or header is lost?
        # Or maybe the first row IS the header but it looks like data (unlikely for headers to be numeric)
        # We assume no separate header found before data.
        logger.warning("Data density high on first row. Assuming no metadata section or file starts with data.")
        return 0 # Optimistic: maybe file has no metadata.
        
    logger.info("Density heuristic inconclusive. No clear data block found.")
    return None

def detect_header_by_keywords(lines: List[str]) -> int:
    """
    Fallback: Finds the row with the most header-like keywords.
    """
    logger.info("Fallback: Matching via Keywords...")
    max_score = -1
    best_idx = 0
    
    for idx, line in enumerate(lines):
        score = calculate_keyword_score(line)
        if score > max_score:
            max_score = score
            best_idx = idx
            
    logger.info(f"Keyword match found at index {best_idx} with score {max_score}.")
    return best_idx
