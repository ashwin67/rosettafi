# The Sniffer & Mapper

**Project Status:** Implemented Stages 1-6 (Ingestion, Mapping, Rules, Categorizer, Ledger, Validation)

## Architecture Summary
This project implements a financial data ingestion engine designed to handle messy bank exports and standardize them into a "Split-Based Ledger" format. The pipeline follows a strict architecture, optimized for local execution using **Llama 3.2**:

### 1. Stage 1: The Sniffer (Heuristics)
*Located in `rosetta/sniffer.py` (facade), `rosetta/logic/`, `rosetta/data/`*
- Ingests raw files (CSV/TXT) and **Excel files** (.xls, .xlsx) via conversion.
- Uses a robust **Token-Based Data Density Heuristic** to identify the header row, distinguishing it from metadata and long-text descriptions.
- Fallback to keyword matching if density is inconclusive.
- Gracefully handles messy data, skipping malformed rows.

### 2. Stage 2: The Mapper (MVC + Strict Logic)
*Located in `rosetta/mapper.py` and `rosetta/constants.py`*
- **MVC Architecture**: Strict separation of Configuration (prompts/keywords in `constants.py`) and Logic (`mapper.py`).
- **Two-Step Logic**:
    1. **LLM Analysis**: Uses `ollama` + `instructor` to generate a `ColumnMapping` object.
    2. **Heuristic Fallback**: If the LLM fails, a robust standalone heuristic (`_heuristic_map_columns`) determines the mapping based on keyword density and localized patterns.
- **Persistence**: Caches mapping decisions in `bank_configs.json` based on header hashes.

### 3. Stage 3: The Rules Engine (Locale-Aware Logic)
*Located in `rosetta/rules.py`*
- **Strategy Pattern**: Uses `USParsingStrategy` (Dot decimal) or `EUParsingStrategy` (Comma decimal) based on the configuration.
- **Polarity Handler**: Applies localized logic to determine transaction direction:
    - *Signed*: Standard negative/positive amounts.
    - *Direction Column*: Uses keywords (e.g. "Debit", "Af") in a separate column.
    - *Split Columns*: Calculates `Credit - Debit`.
- **Normalization**: Cleans dirty inputs (e.g. "€ 1.200,50"), handles unicode characters, and standardizes dates.

### 4. Stage 4: The Categorizer (Phonebook Strategy)
*Located in `rosetta/logic/categorization/`*
A "Phonebook" based approach that treats Merchants as first-class citizens.
1. **Pass 1: Tokenization (The Shredder)**: Uses `qwen2.5:7b` to shred raw strings into tokens (Keywords vs Descriptions).
2. **Pass 2: Resolution (The Phonebook)**:
   - **Exact Match**: O(1) lookup of known aliases.
   - **Fuzzy Suggestion**: If no exact match, finds the closest known entity (Threshold 0.6) to suggest to the user.
3. **Pass 3: Categorization**: Deterministic mapping based on the Resolved Entity's default category + Context Rules.

### 5. Stage 5: The Ledger (Double-Entry Engine)
*Located in `rosetta/logic/ledger.py`*
- **Standard**: Converts single-row expenses into 2-row Double-Entry splits (Asset Credit / Expense Debit).
- **Hybrid Investments**:
    - **Fast Path**: Regex extraction for deterministic patterns ("Buy 10 AAPL @ 150").
    - **Slow Path**: LLM extraction for complex notes ("Purchase of 50 units...").
- **Configurable**: Accounts and Currency defined in `rosetta/data/constants.py`.

### 6. Stage 6: The Validator (Pandera)
*Located in `rosetta/validator.py`*
- Enforces strict type safety using `pandera`.
- Ensures dates are valid datetimes, amounts are floats, and required fields are present.

### 7. Global Workspace
*Located in `rosetta/workspace.py`*
- Implements a Singleton `Workspace` class managing all file I/O paths.
- Automatically ensures the existence of the `~/.rosetta_cache/` directory structure:
    - `configs/`: Stores `bank_configs.json`
    - `phonebook/`: Stores `merchants.json` (The Master Database)
    - `logs/`: Application logs
    - `quarantine/`: Invalid rows dumped here
    - `temp/`: Temporary processing files

## Project Structure
```
rosettafi/
├── main.py             # Entry point
├── requirements.txt    # Minimal dependencies
├── rosetta/            # Core Package
│   ├── models.py       # Pydantic Schemas
│   ├── workspace.py    # Workspace Singleton
│   ├── data/           # Configuration
│   │   └── constants.py
│   ├── logic/          # Core Business Logic
│   │   ├── ledger.py   # Ledger Engine
│   │   ├── categorization/
│   │   │   ├── engine.py       # Pipeline Orchestrator
│   │   │   ├── segmentation.py # LLM Tokenizer
│   │   │   ├── resolver.py     # Fuzzy Matcher
│   │   │   └── phonebook.py    # Persistence Layer
│   ├── sniffer.py      # Facades / Wrappers
│   ├── mapper.py       
│   ├── rules.py        
│   └── validator.py    
```

## Universal Data Model
| Column | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | UUID | Unique ID linking splits |
| `date` | datetime | Transaction date |
| `account` | string | Normalized account name |
| `amount` | float | Transaction amount (Signed: -Expense, +Income) |
| `currency` | string | Currency code (EUR default) |
| `meta` | JSON | Original row data |

## Usage
### Prerequisites
Ensure you have [Ollama](https://ollama.com) installed and running.
```bash
ollama pull qwen2.5:7b
```

### Running the Project
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py [optional_file_path]
```

### Interactive Entity Resolution
If the system encounters unknown merchants, it will enter **Resolution Mode**:
```text
Entity: 'Unknown Bakery'
Identify Entity [Enter for 'Best Bakery']: 
 -> Linked to 'Best Bakery'
```
- **Hit Enter** to accept the suggestion.
- **Type a Name** to create/link a new entity.
- **Type 'skip'** to ignore.

## Future Work
- **Golden Master Regression Testing**: Implement a verified `golden_master.csv` test suite to prevent 'Poisoned Cache' issues in the Categorizer and ensure logic stability across updates.
- **Auto-retry**: If the pandera validation fails, the system must "Auto-retry Stage 2 with the error message as feedback to the LLM"