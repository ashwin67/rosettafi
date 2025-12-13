"""
Constants for the sniffer module.
"""

# Keywords for fallback heuristic (English and Dutch support mainly)
HEADER_KEYWORDS = [
    'date', 'booking', 'transaction', 'amount', 'debit', 'credit', 
    'description', 'memo', 'payee', 'valuta', 'bedrag', 'datum', 
    'omschrijving', 'tegenrekening', 'naam', 'code'
]

# Characters that count as "data" structure separators or content in numeric fields
DATA_SEPARATORS = {'.', ',', ';', '-', '/'}

# Minimum ratio of (Digits + Separators) / Length to consider a line "Data"
DATA_DENSITY_THRESHOLD = 0.5 

# How many lines to analyze
SNIFF_WINDOW_SIZE = 20
