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

# Regex Patterns for the Cleaner Layer
# Strips common banking noise to isolate the Merchant Name
CLEANER_REGEX_PATTERNS = [
    r"/NAME/", 
    r"/TRTP/", 
    r"/SEPA/", 
    r"SEPA Incasso", 
    r"Incasso",
    r"Datum:.*", 
    r"Omschrijving:\s*", # Only strip the label, keep the content
    r"Kenmerk:.*",       # Strip Reference and everything after
    r"IBAN:.*",          # Strip IBAN and everything after
    r"BIC:.*",           # Strip BIC and everything after
    r"[0-9]{14,}", # Long numeric strings (IDs)
    r"\s{2,}"     # Multiple spaces
]

# Hard-coded Dictionary Rules for Deterministic Matches
HARD_CODED_RULES = {
    "hypotheek": "Expenses:Housing:Mortgage",
    "albert heijn": "Expenses:Groceries",
    "jumbo": "Expenses:Groceries",
    "netflix": "Expenses:Subscriptions",
    "ns groep": "Expenses:Transport",
    "shell": "Expenses:Transport",
    "bol.com": "Expenses:Shopping:Online",
    "amazon": "Expenses:Shopping:Online",
    "ziggo": "Expenses:Utilities",
    "kpn": "Expenses:Utilities"
}

# System Prompt for the Agent Layer (Chain of Thought)
CATEGORIZER_SYSTEM_PROMPT = """
You are a financial transaction classifier.
Your goal is to categorize a transaction based on the provided Merchant Name or Description.

Context: 
The user uses the following categories: {existing_categories}.

Task: 
Analyze the input description.
1. Identify the industry or purpose of the merchant.
2. Select the BEST fit from the existing categories.
3. If it is a completely new concept, suggest a new category in the format 'Expenses:Category:Subcategory'.
4. If you are unsure, use 'Uncategorized'.

Output:
Return a JSON object with:
- "reasoning": A brief explanation of your thought process.
- "category": The selected or created category.
"""
