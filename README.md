# The Sniffer & Mapper

**Project Status:** Implemented Stages 1-3 (Ingestion, Mapping, Validation)

## Architecture Summary
This project implements a financial data ingestion engine designed to handle messy bank exports and standardize them into a "Split-Based Ledger" format. The pipeline follows a strict architecture:

1.  **Stage 1: The Sniffer (Heuristics)**
    -   Ingests raw files (CSV).
    -   Scans the first 20 rows to intelligently identify the actual header row, bypassing metadata and garbage lines.
    
2.  **Stage 2: The Mapper (LLM + Instructor)**
    -   Uses `ollama` (running locally) and `instructor` to analyze the cleaned headers.
    -   Generates a `ColumnMapping` configuration to map specific CSV columns (e.g., "Buchungstext", "Betrag") to the Universal Data Model (`description`, `amount`).
    
3.  **Stage 3: The Validator (Pandera)**
    -   Enforces type safety and business logic using `pandera` schemas.
    -   Ensures dates are valid datetimes, amounts are floats/decimals, and identifying splits.

## Libraries Used
-   **pandas**: Data manipulation.
-   **instructor**: Structured output for LLMs.
-   **ollama**: Local LLM inference.
-   **pandera**: Runtime data validation.
-   **pydantic**: Data modeling.

## Current Capabilities
-   Can ingest messy CSV strings/files.
-   Identify headers via keyword scoring heuristics.
-   Map columns using DeepSeek R1 (deepseek-r1:8b) (or fallback logic if offline).
-   Validate and output a clean "Split-Based Ledger" DataFrame.

## Universal Data Model
| Column | Type | Description |
| :--- | :--- | :--- |
| `transaction_id` | UUID | Unique ID linking splits |
| `date` | datetime | Transaction date |
| `account` | string | Normalized account name |
| `amount` | float | Transaction amount |
| `currency` | string | Currency code (EUR default) |
| `meta` | JSON | Original row data |

## Next Steps
-   **Stage 4:** Logic/Polarity flipping (Handling debit/credit column logic).
-   **Stage 5:** Hybrid Categorization with Vector DB (Auto-categorizing based on description using embeddings).

## Usage
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```
