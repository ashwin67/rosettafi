# The Sniffer & Mapper

**Project Status:** Implemented Stages 1-6 (Ingestion, Mapping, Rules, Categorizer, Ledger, Validation)

## Architecture Summary
This project implements a financial data ingestion engine designed to handle messy bank exports and standardize them into a "Split-Based Ledger" format.

### 1. Stage 1: The Sniffer (Heuristics)
*Located in `rosetta/sniffer.py`*
- Ingests raw files (CSV/TXT) and **Excel files** (.xls, .xlsx) via conversion.
- Uses a robust **Token-Based Data Density Heuristic** to identify the header row, distinguishing it from metadata and long-text descriptions.
- Fallback to keyword matching if density is inconclusive.
- Gracefully handles messy data, skipping malformed rows.

### 2. Stage 2: The Mapper (LLM + Heuristics)
*Located in `rosetta/mapper.py`*
- **Hybrid Approach**:
    1. **LLM Analysis**: Attempts to use a local LLM (via Ollama) to generate a `ColumnMapping` object for maximum accuracy.
    2. **Heuristic Fallback**: If the LLM fails or is unavailable, a robust standalone heuristic (`heuristic_map_columns`) determines the mapping based on keyword density and localized patterns.
- **Persistence**: Caches mapping decisions in `~/.rosetta_cache/configs/bank_configs.json` based on header hashes.

### 3. Stage 3: The Rules Engine (Locale-Aware Logic)
*Located in `rosetta/rules.py`*
- **Strategy Pattern**: Uses `USParsingStrategy` (Dot decimal) or `EUParsingStrategy` (Comma decimal) based on the configuration.
- **Polarity Handler**: Applies localized logic to determine transaction direction.
- **Normalization**: Cleans dirty inputs, handles unicode characters, and standardizes dates.

### 4. Stage 4: The Categorizer (Interactive Phonebook)
*Located in `rosetta/logic/categorization/`*
A "Phonebook" based approach that treats Merchants as first-class citizens.
1. **Pass 1: Pre-processing (LLM Tokenizer)**: Uses an LLM (`qwen2.5:7b`) to clean and tokenize raw transaction descriptions, removing irrelevant noise.
2. **Pass 2: Resolution (Interactive)**:
   - For each transaction, it attempts to find a matching entity in the "Phonebook" (a local database of merchants).
   - If an unknown entity is found, the system enters an **interactive mode** in the terminal.
   - It may suggest a similar, existing entity using fuzzy string matching.
   - The user provides the correct entity name and category, which are saved for future use.

### 5. Stage 5: The Ledger (Double-Entry Engine)
*Located in `rosetta/logic/ledger.py`*
- Converts single-row transactions into 2-row Double-Entry splits (e.g., Asset Credit / Expense Debit).
- Configurable accounts and currency.

### 6. Stage 6: The Validator (Pandera)
*Located in `rosetta/validator.py`*
- Enforces strict type safety using `pandera`.
- Ensures dates are valid datetimes, amounts are floats, and required fields are present.

### 7. Global Workspace
*Located in `rosetta/workspace.py`*
- Implements a Singleton `Workspace` class managing all file I/O paths.
- Automatically ensures the existence of the `~/.rosetta_cache/` directory structure:
    - `configs/`: Stores `bank_configs.json`
    - `memory/`: Stores `merchants.json` (The Master Database)
    - `logs/`: Application logs
    - `quarantine/`: Invalid rows dumped here
    - `temp/`: Temporary processing files

## Project Structure
```
rosettafi/
├── main.py             # Entry point
├── requirements.txt    # Dependencies
├── rosetta/            # Core Package
│   ├── models.py       # Pydantic Schemas
│   ├── workspace.py    # Workspace Singleton
│   ├── data/           # Configuration
│   │   └── constants.py
│   ├── logic/          # Core Business Logic
│   │   ├── ledger.py   # Ledger Engine
│   │   └── categorization/
│   │       ├── engine.py       # Pipeline Orchestrator
│   │       ├── segmentation.py # LLM Tokenizer
│   │       ├── resolver.py     # Fuzzy Matcher
│   │       └── phonebook.py    # Persistence Layer
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
Ensure you have [Ollama](https://ollama.com) installed and running. The categorization engine uses a local LLM for pre-processing.
```bash
ollama pull qwen2.5:7b
```

### Running the Project
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py [optional_file_path]

PYTHONPATH=. uvicorn demo.backend.main:app --reload
```

### Interactive Entity Resolution
If the system encounters unknown merchants, it will enter **Resolution Mode**:
```text
Next Entity: 'Coffee Shop'
  e.g. Original Description: "Coffee Shop Purchase"
Identify Entity [Enter for 'Coffee Shop', 'skip' to ignore]: 
Category [Enter for 'UNKNOWN']: Food & Drink
 -> Registered 'Coffee Shop' (Food & Drink)
```
- **Identify Entity**: Hit Enter to accept the suggested name, or type a new one.
- **Category**: Provide a category for the entity.

## Future Work
- **Golden Master Regression Testing**: Implement a verified `golden_master.csv` test suite to prevent 'Poisoned Cache' issues in the Categorizer and ensure logic stability across updates.
- **Auto-retry**: If the pandera validation fails, the system could "Auto-retry Stage 2 with the error message as feedback to the LLM".
