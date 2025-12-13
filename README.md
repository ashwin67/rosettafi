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

### 3. Stage 4: The Logic (Locale-Aware Rules Engine)
*Located in `rosetta/rules.py`*
- **Strategy Pattern**: Uses `USParsingStrategy` (Dot decimal) or `EUParsingStrategy` (Comma decimal) based on the configuration.
- **Polarity Handler**: Applies localized logic to determine transaction direction:
    - *Signed*: Standard negative/positive amounts.
    - *Direction Column*: Uses keywords (e.g. "Debit", "Af") in a separate column.
    - *Split Columns*: Calculates `Credit - Debit`.
- **Normalization**: Cleans dirty inputs (e.g. "€ 1.200,50"), handles unicode characters, and standardizes dates.

### 4. Stage 5: The Categorizer (Hybrid Engine)
*Located in `rosetta/categorizer.py`*
- **Architecture**: A "Hybrid" engine combining a **Vector Cache (Fast Path)** and an **LLM (Slow Path)**.
- **Fast Path**: 
    - Uses `ollama` to generate standard embeddings (`all-minilm`).
    - Stores vectors in a specialized, lightweight JSON file (`category_memory.json`).
    - Performs cosine similarity checks (using `scipy`) to instantly categorize known transactions.
- **Slow Path**:
    - If a transaction is new, it is sent to `llama3.2` asynchronously.
    - The result is then embedded and cached for future speed.
- **Benefits**: Zero heavyweight dependencies. No `torch` or `chromadb` required.

### 5. Stage 6: The Ledger (Split Expansion)
*Located in `rosetta/ledger.py`*
- Transforms single-row transactions into a Balanced Double-Entry Ledger.
- **Normal Transactions**: Generates 2 split rows (Source + Category) summing to zero.
- **Investment Logic**: Detects "Buy/Sell" intent via Regex, splitting into `Currency` flow (Bank) and `Asset` flow (Investments).

### 6. Stage 3: The Validator (Pandera)
*Located in `rosetta/validator.py`*
- Enforces strict type safety using `pandera`.
- Ensures dates are valid datetimes, amounts are floats, and required fields are present.

### 7. Global Workspace
*Located in `rosetta/workspace.py`*
- Implements a Singleton `Workspace` class managing all file I/O paths.
- Automatically ensures the existence of the `~/.rosetta_cache/` directory structure:
    - `configs/`: Stores `bank_configs.json`
    - `memory/`: Stores `category_memory.json`
    - `logs/`: Application logs
    - `quarantine/`: Invalid rows dumped here
    - `temp/`: Temporary processing files

## Project Structure
```
rosettafi/
├── main.py             # Entry point
├── requirements.txt    # Minimal dependencies (numpy, scipy, ollama, instructor)
├── rosetta/            # Core Package
│   ├── models.py       # Pydantic Schemas
│   ├── sniffer.py      # Stage 1 (Facade)
│   ├── data/
│   │   └── sniffer_constants.py
│   ├── logic/
│   │   └── sniffer_logic.py
│   ├── mapper.py       # Stage 2
│   ├── rules.py        # Stage 4 (Logic)
│   ├── categorizer.py  # Stage 5 (Hybrid Classifer)
│   ├── ledger.py       # Stage 6 (Splits)
│   ├── validator.py    # Stage 3
│   ├── workspace.py    # Workspace & Cache Management
│   └── config.py       # Configuration
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
Ensure you have [Ollama](https://ollama.com) installed and running. You will need to pull the specific models used by this pipeline:
```bash
ollama pull llama3.2
ollama pull all-minilm
```

### Running the Project
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

## Future Work
- **Golden Master Regression Testing**: Implement a verified `golden_master.csv` test suite to prevent 'Poisoned Cache' issues in the Categorizer and ensure logic stability across updates.
- **Auto-retry**: If the pandera validation fails, the system must "Auto-retry Stage 2 with the error message as feedback to the LLM"