import torch
from setfit import SetFitModel, Trainer, TrainingArguments
from datasets import Dataset
from typing import List, Dict, Tuple, Optional, Any
import pandas as pd

class Categorizer:
    def __init__(self, model_id: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model_id = model_id
        self.model = SetFitModel.from_pretrained(model_id)
        self.trained = False
        self.label2id = {}
        self.id2label = {}

    def train(self, texts: List[str], labels: List[str]):
        """
        Fine-tune the SetFit model on a small set of labeled examples.
        """
        if not texts or not labels:
            return

        # Prepare labels
        unique_labels = sorted(list(set(labels)))
        self.label2id = {label: i for i, label in enumerate(unique_labels)}
        self.id2label = {i: label for label, i in self.label2id.items()}
        
        numeric_labels = [self.label2id[l] for l in labels]
        train_dataset = Dataset.from_dict({
            "text": texts,
            "label": numeric_labels
        })

        # Training arguments for fast local fine-tuning
        args = TrainingArguments(
            batch_size=8,
            num_epochs=1,
            num_iterations=10, # Good for few-shot
            # use_amp=True if torch.cuda.is_available() else False
        )

        trainer = Trainer(
            model=self.model,
            args=args,
            train_dataset=train_dataset
        )
        
        trainer.train()
        self.trained = True

    def predict(self, texts: List[str], threshold: float = 0.7) -> List[Dict[str, Any]]:
        """
        Predict categories for a list of texts.
        Returns a list of dicts with 'category' and 'confidence'.
        If confidence < threshold, category is None (for Active Learning).
        """
        if not self.trained or not texts:
            return [{"category": None, "confidence": 0.0} for _ in texts]

        # Get probabilities
        probs = self.model.predict_proba(texts)
        if isinstance(probs, torch.Tensor):
            probs = probs.detach().cpu().numpy()

        results = []
        for p in probs:
            max_idx = p.argmax()
            confidence = float(p[max_idx])
            
            if confidence >= threshold:
                results.append({
                    "category": self.id2label[max_idx],
                    "confidence": confidence
                })
            else:
                results.append({
                    "category": None,
                    "confidence": confidence
                })
        
        return results

    def get_uncertain_items(self, texts: List[str], threshold: float = 0.7) -> List[Tuple[int, str, float]]:
        """
        Identify items that need manual labeling.
        Returns list of (index, text, confidence).
        """
        predictions = self.predict(texts, threshold=threshold)
        uncertain = []
        for i, pred in enumerate(predictions):
            if pred["category"] is None:
                uncertain.append((i, texts[i], pred["confidence"]))
        return uncertain
