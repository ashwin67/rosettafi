# The Sniffer & Mapper

**Project Status:** Implemented Stages 1-6 (Ingestion, Mapping, Rules, Categorizer, Ledger, Validation)

## Architecture Summary
This project implements a financial data ingestion engine designed to handle messy bank exports and standardize them into a "Split-Based Ledger" format. The pipeline follows a strict architecture, optimized for local execution using **Llama 3.2**:

### 1. Stage 1: The Sniffer (Heuristics)
*Located in `rosetta/sniffer.py`*
- Ingests raw files (CSV/TXT).
- Scans the first 20 rows to intelligently identify the actual header row, bypassing metadata and garbage lines.
- Handles various delimiters (comma, semicolon).

### 2. Stage 2: The Mapper (LLM + Instructor)
*Located in `rosetta/mapper.py`*
- Uses `ollama` (local `llama3.2`) and `instructor` to analyze the cleaned headers.
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

## Project Structure
```
rosettafi/
├── main.py             # Entry point
├── requirements.txt    # Minimal dependencies (numpy, scipy, ollama, instructor)
├── rosetta/            # Core Package
│   ├── models.py       # Pydantic Schemas
│   ├── sniffer.py      # Stage 1
│   ├── mapper.py       # Stage 2
│   ├── rules.py        # Stage 4 (Logic)
│   ├── categorizer.py  # Stage 5 (Hybrid Classifer)
│   ├── ledger.py       # Stage 6 (Splits)
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