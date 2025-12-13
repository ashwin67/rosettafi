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
