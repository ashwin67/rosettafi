import re
from flashtext import KeywordProcessor
from typing import List

class TextCleaner:
    def __init__(self, additional_stopwords: List[str] = None):
        # Regex for dynamic noise: dates, long IDs, card numbers
        self.noise_patterns = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', # Dates
            r'\d{4}\d{4}\d{4}\d{4}',           # Full card numbers
            r'X{4,}\d{4}',                     # Masked card numbers
            r'\b[A-Z0-9]{10,}\b',              # Long alphanumeric IDs
            r'\b\d{6,}\b',                     # Long numeric IDs
            r'[^\w\s]',                        # Special characters
        ]
        
        self.keyword_processor = KeywordProcessor()
        
        # Default bank static noise
        bank_stopwords = [
            "POS", "VISA", "TERMINAL", "CARD", "PURCHASE", "AUTH", 
            "DEBIT", "CREDIT", "PAYMENT", "TRANSACTION", "DESCRIPTION",
            "AVAILABLE", "BALANCE", "DATE", "TIME"
        ]
        if additional_stopwords:
            bank_stopwords.extend(additional_stopwords)
            
        # Map each stopword to a space for removal
        for word in bank_stopwords:
            self.keyword_processor.add_keyword(word, " ")

    def clean(self, text: str) -> str:
        if not text:
            return ""
            
        # Convert to uppercase for consistent processing
        text = text.upper()
        
        # 1. FlashText processing for static stopwords (O(N) time)
        # We do this BEFORE regex to avoid breaking keywords
        text = self.keyword_processor.replace_keywords(text)
        
        # 2. Regex processing for dynamic noise
        for pattern in self.noise_patterns:
            text = re.sub(pattern, ' ', text)
            
        # 3. Final cleanup: remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
