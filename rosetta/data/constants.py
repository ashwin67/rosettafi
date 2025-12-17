"""
Centralized constants for the Rosetta application.
"""

# ==============================================================================
# GLOBAL / LLM CONFIGURATION
# ==============================================================================
LLM_MODEL_NAME = "qwen2.5:7b" 
LLM_BASE_URL = "http://localhost:11434/v1"
LLM_API_KEY = "ollama"

# ==============================================================================
# STAGE 1: SNIFFER CONFIGURATION
# ==============================================================================
SNIFFER_HEADER_KEYWORDS = [
    'date', 'booking', 'transaction', 'amount', 'debit', 'credit', 
    'description', 'memo', 'payee', 'valuta', 'bedrag', 'datum', 
    'omschrijving', 'tegenrekening', 'naam', 'code', 'fecha', 'importe', 
    'concepto', 'saldo', 'verwendungszweck'
]
DATA_SEPARATORS = {'.', ',', ';', '-', '/'}
DATA_DENSITY_THRESHOLD = 0.5 
SNIFF_WINDOW_SIZE = 20

# ==============================================================================
# STAGE 2: MAPPER CONFIGURATION
# ==============================================================================
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
3. Determine Polarity Logic.
"""

KEYWORDS_DATE = ['date', 'datum', 'transactiedatum', 'valutadatum', 'time', 'fecha']
KEYWORDS_AMOUNT = ['amount', 'bedrag', 'transactiebedrag', 'debit', 'credit', 'eur', 'value', 'betrag', 'importe', 'saldo']
KEYWORDS_DESC = ['description', 'omschrijving', 'mededelingen', 'naam', 'name', 'text', 'desc', 'book', 'narr', 'memo', 'payee', 'concepto', 'verwendungszweck']
KEYWORDS_CREDIT = ['credit', 'bij']
KEYWORDS_DEBIT = ['debit', 'af']
KEYWORDS_DIRECTION = ['cd', 'c/d', 'direction', 'type', 'af_bij']
DECIMAL_COMMA_INDICATORS = ['bedrag', 'valuta', 'buchung', 'eur', 'transactiebedrag', 'betrag', 'importe']

# ==============================================================================
# STAGE 4: RULES ENGINE CONFIGURATION
# ==============================================================================
CLEAN_CURRENCY_REGEX = r'[^\d.,\-]'
UNICODE_REPLACEMENTS = {"−": "-", "\u2013": "-", "\u2014": "-", "\u00A0": " ", "\u202F": " "}

# ==============================================================================
# STAGE 6: LEDGER CONFIGURATION
# ==============================================================================
DEFAULT_ASSET_ACCOUNT = "Assets:Current:Bank"
DEFAULT_CURRENCY = "EUR"
INVESTMENT_KEYWORDS = {
    "buy": ["buy", "purchase", "koop", "aankoop", "achat", "kaufen"],
    "sell": ["sell", "sold", "verkoop", "vente", "verkaufen"]
}
INVESTMENT_REGEX_PATTERNS = [r"(?i)(buy|sell|koop|verkoop)\s+(\d+)\s+([A-Z]{2,5})\s+@\s+([\d.,]+)"]
LEDGER_INVESTMENT_PROMPT = """
You are a financial transaction analyzer. Extract investment details (action, quantity, ticker, price).
"""

# ==============================================================================
# STAGE 5: CATEGORIZER (ENTITY ENGINE) CONFIGURATION
# ==============================================================================
UNKNOWN_CATEGORY = "Uncategorized"

# The "Mechanical" Prompt - Focused on Splitting, not Judging
TOKENIZATION_PROMPT = """
You are a strict tokenizer. 
Your job is to SPLIT the input string into a list of segments based on delimiters.

RULES:
1. **SPLIT** on all separators: `/`, `:`, `*`, `;`, `-`, and double spaces.
2. **DISCARD** only these specific types of "Trash":
   - Purely numeric strings (e.g., "12345", "12.50").
   - Alphanumeric IDs longer than 15 characters (e.g., "NL27INGB000...").
   - Single characters (e.g., "A", "x", ".").
3. **KEEP EVERYTHING ELSE**. 
   - If a segment is a word, a code, a tag, or a sentence: KEEP IT.
   - Do not judge if it is "meaningful". When in doubt, KEEP IT.
   - Keep tags like "REMI", "NAME", "SEPA" in the output list.

Input: "BEA, Betaalpas CCV*Gebroeders van Hez,PAS142"
Output: { "parts": ["BEA", "Betaalpas", "CCV", "Gebroeders van Hez", "PAS142"] }

Input: "/TRTP/iDEAL/NAME/bol.com/REMI/12345"
Output: { "parts": ["TRTP", "iDEAL", "NAME", "bol.com", "REMI"] }
"""

# Deterministic Blocklist for Python Filter
BANNED_TAGS_SET = {
    'TRTP', 'TRCT', 'IBAN', 'BIC', 'CSID', 'EREF', 'REMI', 
    'NAME', 'MARF', 'NR', 'BEA', 'SEPA', 'PAS', 'VAL', 'KREF',
    'OMSCHRIJVING', 'MEDEDELINGEN', 'KENMERK', 'BETALINGSKENMERK',
    'INCASSO', 'ALGEMEEN', 'DOORLOPEND', 'EURO', 'MUNT', 'CODE',
    'BETALING', 'ACCEPTGIRO', 'SPOED', 'CREDIT', 'DEBIT', 'BOEKDATUM'
}