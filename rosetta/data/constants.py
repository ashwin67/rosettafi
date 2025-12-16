"""
Centralized constants for the Rosetta application.
Organized by module/stage.
"""

# ==============================================================================
# GLOBAL / LLM CONFIGURATION
# ==============================================================================
LLM_MODEL_NAME = "llama3.2"
LLM_BASE_URL = "http://localhost:11434/v1"
LLM_API_KEY = "ollama"

# ==============================================================================
# STAGE 1: SNIFFER CONFIGURATION
# ==============================================================================
# Keywords for fallback heuristic (English, Dutch, German, Spanish, etc)
# Used to determine if a row is likely a header when density check fails.
SNIFFER_HEADER_KEYWORDS = [
    'date', 'booking', 'transaction', 'amount', 'debit', 'credit', 
    'description', 'memo', 'payee', 'valuta', 'bedrag', 'datum', 
    'omschrijving', 'tegenrekening', 'naam', 'code', 'fecha', 'importe', 
    'concepto', 'saldo', 'verwendungszweck'
]

# Characters that count as "data" structure separators or content in numeric fields
DATA_SEPARATORS = {'.', ',', ';', '-', '/'}

# Minimum ratio of (Digits + Separators) / Length to consider a line "Data"
DATA_DENSITY_THRESHOLD = 0.5 

# How many lines to analyze from the top of the file
SNIFF_WINDOW_SIZE = 20

# ==============================================================================
# STAGE 2: MAPPER CONFIGURATION
# ==============================================================================

# Prompts ----------------------------------------------------------------------
MAPPER_SYSTEM_PROMPT = """You are a data engineering assistant.
Analyze the provided CSV headers and return a JSON object matching the ColumnMapping schema.
Do NOT return the JSON Schema definition. Return the actual mapping data.
"""

MAPPER_USER_PROMPT_TEMPLATE = """
Given these file headers: {headers}

1. Identify Date, Amount, and Description columns.
   - Date keywords: {date_keywords}
   - Amount keywords: {amount_keywords}
   - Description keywords: {desc_keywords}
2. Determine Decimal Separator (Comma ',' or Dot '.').
3. Determine Polarity Logic:
   - Case A: One 'Amount' column with signed values.
   - Case B: One 'Amount' column + a 'Direction' column (e.g. Credit/Debit words).
   - Case C: Separate 'Credit' and 'Debit' value columns.
"""

# Keywords (Heuristics) --------------------------------------------------------
KEYWORDS_DATE = [
    'date', 'datum', 'transactiedatum', 'valutadatum', 'time', 'fecha', 'zeit'
]
KEYWORDS_AMOUNT = [
    'amount', 'bedrag', 'transactiebedrag', 'debit', 'credit', 'eur', 'value', 'betrag', 
    'importe', 'saldo'
]
KEYWORDS_DESC = [
    'description', 'omschrijving', 'mededelingen', 'naam', 'name', 'text', 'desc', 
    'book', 'narr', 'memo', 'payee', 'concepto', 'verwendungszweck'
]
KEYWORDS_CREDIT = ['credit', 'bij']
KEYWORDS_DEBIT = ['debit', 'af']
KEYWORDS_DIRECTION = ['cd', 'c/d', 'direction', 'type', 'af_bij']

# Heuristics for Logic
DECIMAL_COMMA_INDICATORS = ['bedrag', 'valuta', 'buchung', 'eur', 'transactiebedrag', 'betrag', 'importe']

# ==============================================================================
# STAGE 4: RULES ENGINE CONFIGURATION
# ==============================================================================
# Regex pattern to strip non-numeric characters from amount strings (keeps digits, ., ,, -)
# actually we want to strip everything EXCEPT digits, separators and negative signs.
# But often easier to target what to STRIP.
# Common symbols: $, €, £, Spaces, Letters (EUR, USD).
CLEAN_CURRENCY_REGEX = r'[^\d.,\-]'

# Map of unicode characters to standard ASCII for normalization
UNICODE_REPLACEMENTS = {
    "−": "-",  # Unicode minus
    "\u2013": "-", # En dash
    "\u2014": "-", # Em dash
    "\u00A0": " ", # Non-breaking space (often used as thousand sep)
    "\u202F": " "  # Narrow no-break space
}

# ==============================================================================
# STAGE 6: LEDGER CONFIGURATION
# ==============================================================================
DEFAULT_ASSET_ACCOUNT = "Assets:Current:Bank"
DEFAULT_CURRENCY = "EUR"

# Investment Keywords (Locale Agnostic)
# Maps generic actions to list of local triggers
INVESTMENT_KEYWORDS = {
    "buy": ["buy", "purchase", "koop", "aankoop", "achat", "kaufen"],
    "sell": ["sell", "sold", "verkoop", "vente", "verkaufen"]
}

# Regex Patterns for "Fast Path" Investment Detection
# Pattern: Action + Qty + Ticker + @ + Price
# e.g. "Buy 10 AAPL @ 150.00"
# Capture Groups: 1=Action, 2=Qty, 3=Ticker, 4=Price
INVESTMENT_REGEX_PATTERNS = [
    r"(?i)(buy|sell|koop|verkoop)\s+(\d+)\s+([A-Z]{2,5})\s+@\s+([\d.,]+)"
]

# System Prompt for Investment Extraction (Slow Path)
LEDGER_INVESTMENT_PROMPT = """
You are a financial transaction analyzer.
Extract investment details from the description.
Return a valid JSON object with:
- "action": "buy" or "sell"
- "quantity": float
- "ticker": string (Symbol)
- "price": float (Price per unit)

If you cannot confidently extract these details, return null for all fields.
"""

# ==============================================================================
# STAGE 5: CATEGORIZER CONFIGURATION
# ==============================================================================
SIMILARITY_THRESHOLD = 0.85
UNKNOWN_CATEGORY = "Uncategorized"
CATEGORIZER_EMBEDDING_MODEL = "all-minilm"

# Expanded Default Categories to prevent Bucket Errors
DEFAULT_CATEGORIES = [
    "Expenses:Groceries", 
    "Expenses:Rent", 
    "Expenses:Salary", 
    "Expenses:Transfer", 
    "Expenses:Eating Out", 
    "Expenses:Utilities", 
    "Expenses:Entertainment", 
    "Expenses:Transport", 
    "Expenses:Shopping:General",
    "Expenses:Shopping:Online",
    "Expenses:Housing:Mortgage",
    "Expenses:Services:Education",
    "Expenses:Insurance", 
    "Expenses:Subscriptions", 
    "Expenses:Medical", 
    "Expenses:Travel"
]


ENTITY_SEGMENTATION_PROMPT = """
You are a strict extraction engine. You are given a batch of raw banking transaction strings, each with an ID.
Your job is to split each string into two lists of substrings: "keywords" (technical data) and "descriptions" (human readable text).

Structure:
{
  "items": [
    { "id": <int>, "keywords": [...], "descriptions": [...] },
    ...
  ]
}

CRITICAL RULES:
1. NO HALLUCINATIONS: Your output must consist ONLY of exact substrings found in the input text. Do not correct typos. Do not add words.
2. NO SKIPPING: Every part of the string must be assigned to one of the two lists.
3. PRESERVE IDs: You MUST return the exact ID for each input item.

CLASSIFICATION LOGIC:
1. "keywords":
   - Any string containing numbers (e.g., "PAS142", "20.11.25", "13.4").
   - Any technical tags or routing codes (e.g., "/TRTP/", "CSID", "IBAN", "BIC").
   - Codes that look like IDs (e.g., "NL27INGB...", "CR123").
   - Single letters or noise (e.g., "Yy").

2. "descriptions":
   - Merchant names (e.g., "Bol.com", "Philips", "Bakkerij Bart").
   - City names (e.g., "Kerkdriel", "Amsterdam").
   - Remittance text (readable sentences).
   - EXCEPTION: If a Merchant Name contains numbers (e.g. "Key4Music"), put it here.

Examples:
Input: [{"id": 1, "text": "BEA, Google Pay Philips MedicalSystems,PAS132 NR:L32V7F, 20.11.25"}]
Output: { "items": [{ "id": 1, "keywords": ["BEA", "PAS132", "NR:L32V7F", "20.11.25"], "descriptions": ["Google Pay", "Philips MedicalSystems"] }] }

Input: [{"id": 2, "text": "/TRTP/SEPA Incasso/CSID/NL98ZZZ/NAME/Key4Music VOF/EREF/20201127"}]
Output: { "items": [{ "id": 2, "keywords": ["/TRTP/", "/CSID/", "NL98ZZZ", "/EREF/", "20201127"], "descriptions": ["SEPA Incasso", "Key4Music VOF"] }] }
"""

BATCH_CATEGORIZATION_PROMPT = """
You are a financial classifier.
I will give you a list of merchant names.
Categorize each one into a high-level financial category (e.g., 'Groceries', 'Transport', 'Utilities', 'Salary', 'Investment').
Use the context of the entire list to improve accuracy (e.g., if you see multiple gas stations, 'Shell' is likely a gas station).
Return a JSON dictionary mapping the Merchant Name to the Category.
"""
