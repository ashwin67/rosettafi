# The Sniffer & Mapper

**Project Status:** Implemented Stages 1-5 (Ingestion, Mapping, Rules Engine, Categorizer, Validation)

## Architecture Summary
This project implements a financial data ingestion engine designed to handle messy bank exports and standardize them into a "Split-Based Ledger" format. The pipeline follows a strict architecture:

### 1. Stage 1: The Sniffer (Heuristics)
*Located in `rosetta/sniffer.py`*
- Ingests raw files (CSV/TXT).
- Scans the first 20 rows to intelligently identify the actual header row, bypassing metadata and garbage lines.
- Handles various delimiters (comma, semicolon).

### 2. Stage 2: The Mapper (LLM + Instructor)
*Located in `rosetta/mapper.py`*
- Uses `ollama` (local `deepseek-r1:8b`) and `instructor` to analyze the cleaned headers.
- Generates a rich `ColumnMapping` configuration, identifying:
    - Target columns (`date`, `amount`, `description`).
    - Decimal Separator (`.` or `,`).
    - Polarity Logic (Signed, Direction Column, or Debit/Credit columns).
- Includes robust heuristics fallback if the LLM is unavailable.

### 3. Stage 4: The Logic (Rules Engine)
*Located in `rosetta/rules.py`*
- Accepts the `ColumnMapping` configuration.
- Performs **Locale-Aware Parsing** to correctly parse numbers like `1.000,00` (European) or `1,000.00` (US).
- Applies **Polarity Logic** to ensure expenses are negative and income is positive, handling multiple bank styles (Direction columns, Credit/Debit splits).

### 4. Stage 5: The Categorizer (LLM)
*Located in `rosetta/categorizer.py`*
- Replaces the default `Assets:Bank:Unknown` account with specific categories (e.g., `Expenses:Groceries`).
- Uses `deepseek-r1:8b` via `instructor` to classify transaction descriptions based on predefined categories.
- Upgradable simplified architecture ready for vector database integration.

### 5. Stage 3: The Validator (Pandera)
*Located in `rosetta/validator.py`*
- Enforces strict type safety using `pandera`.
- Ensures dates are valid datetimes, amounts are floats, and required fields are present.

## Project Structure
```
rosettafi/
├── main.py             # Entry point
├── requirements.txt
├── rosetta/            # Core Package
│   ├── models.py       # Pydantic Schemas
│   ├── sniffer.py      # Stage 1
│   ├── mapper.py       # Stage 2
│   ├── rules.py        # Stage 4 (Logic)
│   ├── categorizer.py  # Stage 5
│   ├── validator.py    # Stage 3
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
Ensure you have [Ollama](https://ollama.com) installed and running. You will need to pull the specific model used by this pipeline:
```bash
ollama pull deepseek-r1:8b
```

### Running the Project
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```
