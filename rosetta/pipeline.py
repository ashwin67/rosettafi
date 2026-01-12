import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from rosetta.database import RosettaDB
from rosetta.logic.cleaning import TextCleaner
from rosetta.logic.resolution import EntityResolver
from rosetta.logic.classification import Categorizer
from rosetta.logic.ledger import LedgerEngine
from rosetta.sniffer import sniff_header_row
from rosetta.mapper import get_column_mapping
import uuid

class RosettaPipeline:
    def __init__(self, db_path: str = "rosetta.db"):
        self.db = RosettaDB(db_path)
        self.cleaner = TextCleaner()
        self.resolver = EntityResolver(self.db)
        self.categorizer = Categorizer()
        self.ledger = LedgerEngine()

    def process_file(self, file_path: str, threshold: float = 0.85) -> Dict[str, Any]:
        """
        Stage 1-5 Orchestration:
        Load -> Map -> Clean -> Resolve -> Categorize -> Review
        """
        # 1. Load Data
        df = sniff_header_row(file_path)
        if df.empty:
            return {"status": "error", "message": "Empty or unparseable file."}

        # 2. Map Columns
        mapping = get_column_mapping(df)
        desc_col = mapping.desc_col
        
        # 3. Vector Resolution (High Confidence Path)
        processed = []
        needs_review = []
        
        for _, row in df.iterrows():
            raw_desc = str(row.get(desc_col, ""))
            item = row.to_dict()
            item['transaction_id'] = str(uuid.uuid4())
            
            # Use deterministic cleaning for resolution
            cleaned_desc = self.cleaner.clean(raw_desc)
            item['cleaned_description'] = cleaned_desc
            
            resolution = self.resolver.resolve(raw_desc, threshold=threshold)
            
            if resolution:
                item['entity'] = resolution['canonical_name']
                item['account'] = resolution['default_category']
                item['confidence'] = resolution['similarity']
                item['method'] = 'vector_search'
                processed.append(item)
            else:
                item['entity'] = None
                item['account'] = None
                item['confidence'] = 0.0
                item['method'] = None
                needs_review.append(item)
                
        # 4. SetFit Predictive Fallback for Review Items (if model is trained)
        if needs_review and self.categorizer.trained:
            review_texts = [item['cleaned_description'] for item in needs_review]
            predictions = self.categorizer.predict(review_texts, threshold=threshold)
            
            for item, pred in zip(needs_review, predictions):
                if pred['category']:
                    item['account'] = pred['category']
                    item['confidence'] = pred['confidence']
                    item['method'] = 'setfit'
        
        return {
            "processed": processed,
            "needs_review": needs_review,
            "mapping": mapping.model_dump() if hasattr(mapping, 'model_dump') else mapping
        }

    def update_knowledge(self, labeled_items: List[Dict]):
        """
        Update Vector DB and Retrain SetFit with new manual labels.
        """
        # Update Vector DB for Entity Resolution
        for item in labeled_items:
            entity = item.get('entity')
            category = item.get('account') or item.get('category')
            description = item.get('description') or item.get('cleaned_description')
            
            if entity and category and description:
                self.resolver.add_merchant(entity, category, description)
        
        # Retrain SetFit for Predictive Categorization
        texts = [item.get('description') or item.get('cleaned_description') for item in labeled_items]
        labels = [item.get('account') or item.get('category') for item in labeled_items]
        
        if texts and labels:
            self.categorizer.train(texts, labels)

    def finalize_ledger(self, items: List[Dict], mapping_dict: Dict) -> pd.DataFrame:
        """
        Convert processed items to a double-entry ledger.
        """
        # Convert back to DataFrame
        df = pd.DataFrame(items)
        if df.empty:
            return pd.DataFrame()
            
        # Ensure mapping-specific column names are used by the LedgerEngine
        # Usually LedgerEngine expects 'date', 'amount', 'description'
        from rosetta.models import ColumnMapping
        mapping = ColumnMapping(**mapping_dict)
        
        # Harmonize columns for LedgerEngine
        df['date'] = df[mapping.date_col]
        df['description'] = df[mapping.desc_col]
        
        # Handle polarity/amount logic
        # For simplicity, we assume the user has a way to get the 'amount' field
        # The mapper usually handles this but let's be careful.
        # This part might need better integration with mapper's logic.
        from rosetta.pipeline_utils import normalize_amounts
        df = normalize_amounts(df, mapping)
        
        return self.ledger.generate_splits(df)
