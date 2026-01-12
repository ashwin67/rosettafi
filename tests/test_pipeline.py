import pandas as pd
import os
import pytest
from rosetta.pipeline import RosettaPipeline

def test_pipeline_e2e(tmp_path):
    # 1. Create Mock CSV
    csv_file = tmp_path / "transactions.csv"
    csv_file.write_text(
        "Date,Description,Amount\n"
        "2023-01-01,AMAZON.COM SEATTLE,-50.00\n"
        "2023-01-02,SHELL GAS STATION,-100.00\n"
        "2023-01-03,NEW UNKNOWN SHOP,-10.00\n"
    )
    
    db_path = tmp_path / "test_pipeline.db"
    pipeline = RosettaPipeline(str(db_path))
    
    # 2. Pre-seed knowledge via Resolver
    # Similarity should catch "AMAZON.COM SEATTLE"
    pipeline.resolver.add_merchant("Amazon", "Shopping", "AMAZON.COM")
    
    # 3. Process File
    results = pipeline.process_file(str(csv_file), threshold=0.8)
    
    print(f"DEBUG: Mapping: {results.get('mapping')}")
    processed = results['processed']
    needs_review = results['needs_review']
    
    for item in needs_review:
        print(f"DEBUG: Unresolved item cleaned description: '{item.get('cleaned_description')}'")
    
    # Amazon should be resolved
    amazon_items = [i for i in processed if i['entity'] == "Amazon"]
    assert len(amazon_items) > 0
    assert amazon_items[0]['account'] == "Shopping"
    
    # Unknown shop should be in needs_review
    assert len(needs_review) >= 1
    all_unresolved_desc = [item.get('Description', '') for item in needs_review]
    assert any("NEW UNKNOWN SHOP" in d for d in all_unresolved_desc)
    
    # 4. Simulate User Feedback & SetFit Training
    # User labels the unknown shops
    review_item1 = next(item for item in needs_review if "NEW UNKNOWN SHOP" in item.get('Description', ''))
    review_item1['entity'] = "New Shop"
    review_item1['account'] = "General"
    
    review_item2 = next(item for item in needs_review if "SHELL" in item.get('Description', ''))
    review_item2['entity'] = "Shell"
    review_item2['account'] = "Transport"
    
    pipeline.update_knowledge([review_item1, review_item2])
    
    # 5. Verify SetFit prediction on a similar item
    new_items = [{"Description": "NEW UNKNOWN SHOP LONDON", "cleaned_description": "NEW UNKNOWN SHOP LONDON"}]
    # We need to ensure the model is considered 'trained' and predict
    predictions = pipeline.categorizer.predict(["NEW UNKNOWN SHOP LONDON"], threshold=0.0)
    assert predictions[0]['category'] == "General"

    # 6. Finalize Ledger
    all_items = processed + [review_item1, review_item2]
    # We need to simulate the mapping dict
    mapping_dict = results['mapping']
    
    ledger_df = pipeline.finalize_ledger(all_items, mapping_dict)
    assert not ledger_df.empty
    # Each transaction becomes 2 splits
    assert len(ledger_df) == len(all_items) * 2
