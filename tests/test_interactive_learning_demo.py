import pytest
from fastapi.testclient import TestClient
import pandas as pd
from pathlib import Path
import shutil

# This is a bit of a hack to make the demo app importable.
# In a real project, the demo app would be a proper package.
import sys
sys.path.append(str(Path(__file__).parent.parent))

from demo.backend.main import app, SESSION_STORE

@pytest.fixture(scope="module")
def client():
    """Create a TestClient instance for the FastAPI app."""
    return TestClient(app)

@pytest.fixture(scope="function", autouse=True)
def cleanup_session():
    """Ensure the session store is clean before and after each test."""
    SESSION_STORE.clear()
    yield
    SESSION_STORE.clear()

@pytest.fixture(scope="module")
def mock_csv_path():
    """Create a mock CSV file for testing."""
    test_data = """Date,Description,Amount
2023-10-01,COFFEE SHOP NYC,-5.50
2023-10-02,Monthly Salary,3500.00
2023-10-03,Gas Station Shell,-45.20
2023-10-04,Starbucks 123,-4.75
2023-10-05,Random Supermarket,-120.10
2023-10-06,Amazon.com*Purchase, -30.00
2023-10-07,COFFEE SHOP NYC #2,-6.00
2023-10-08,Shell Gas Station #456, -50.00
2023-10-09,Amazon Web Services, -150.00
2023-10-10,Google Cloud Platform, -200.00
"""
    test_dir = Path("tests/temp_test_data")
    test_dir.mkdir(exist_ok=True)
    file_path = test_dir / "test_transactions.csv"
    with open(file_path, "w") as f:
        f.write(test_data)
    
    yield file_path
    
    # Teardown
    shutil.rmtree(test_dir)

import json

# ... (keep existing fixtures) ...

@pytest.fixture
def seeded_phonebook(monkeypatch, tmp_path):
    """
    Creates a temporary workspace with a pre-seeded merchants.json file
    and monkeypatches the Workspace to use it.
    """
    # 1. Monkeypatch Path.home() to point to our temporary directory
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    # 2. The Workspace will now automatically create its structure inside tmp_path
    temp_memory_dir = tmp_path / ".rosetta_cache" / "memory"
    temp_memory_dir.mkdir(parents=True, exist_ok=True)

    # 3. Pre-seed the merchants.json file
    seeded_data = {
        "amazon": {
            "id": "amazon",
            "canonical_name": "Amazon",
            "default_category": "Shopping",
            "aliases": ["amazon", "amazon.com"],
            "rules": []
        }
    }
    with open(temp_memory_dir / "merchants.json", "w") as f:
        json.dump(seeded_data, f)
        
    yield
    
    # Teardown is handled by pytest's tmp_path fixture

def test_high_confidence_auto_categorization(client, mock_csv_path, seeded_phonebook):
    """
    Tests that items with a high confidence score are auto-categorized and skipped.
    """
    # 1. Upload the file to initialize a session
    # This will use the seeded phonebook because of the monkeypatch
    with open(mock_csv_path, "r") as f:
        content = f.read()
    response = client.post("/upload", json={"filename": "test.csv", "content": content, "encoding": "text"})
    
    assert response.status_code == 200
    session_id = response.json()["session_id"]

    # 2. Get the first batch of unknowns
    response = client.post("/interactive-categorize", json={"session_id": session_id, "feedback": []})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["status"] == "pending_categorization"

    # 3. Verify the unknowns list
    unknowns = json_response["unknowns"]
    
    # The batch size is 5, but one item ("Amazon.com*Purchase") should have been
    # auto-categorized with high confidence and skipped.
    # So we expect 4 items to be returned for manual categorization.
    # The processing order is:
    # - coffee shop nyc (unknown) -> returned
    # - monthly salary (unknown) -> returned
    # - gas station shell (unknown) -> returned
    # - starbucks 123 (unknown) -> returned
    # - random supermarket (unknown) -> returned
    # - amazon.com*purchase (high confidence) -> SKIPPED
    # The loop in the backend stops when it has 5 items to return.
    # Let's check what's actually in the batch.
    
    raw_descriptions = [item["raw"] for item in unknowns]
    
    # "amazon.com*purchase" should NOT be in the list of unknowns
    assert "amazon.com*purchase" not in raw_descriptions
    
    # We should have the first 5 non-Amazon items, which are now lowercased
    assert "coffee shop nyc" in raw_descriptions
    assert "monthly salary" in raw_descriptions
    assert "gas station shell" in raw_descriptions
    assert "starbucks 123" in raw_descriptions
    assert "random supermarket" in raw_descriptions
    
    # The loop should stop after finding 5 unknowns, so the batch size is 5
    assert len(raw_descriptions) == 5

def test_full_interactive_loop(client, mock_csv_path):
    """
    Tests the entire interactive workflow from file upload to final categorization.
    """
    # 1. Upload the file to initialize a session
    with open(mock_csv_path, "r") as f:
        content = f.read()
    response = client.post("/upload", json={"filename": "test.csv", "content": content, "encoding": "text"})
    
    assert response.status_code == 200
    json_response = response.json()
    assert "session_id" in json_response
    session_id = json_response["session_id"]

    # 2. Get the first batch of unknowns
    response = client.post("/interactive-categorize", json={"session_id": session_id, "feedback": []})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["status"] == "pending_categorization"
    assert len(json_response["unknowns"]) == 5
    # The 'raw' value is now the cleaned, lowercased version
    assert json_response["unknowns"][0]["raw"] == "coffee shop nyc"
    assert json_response["unknowns"][1]["raw"] == "monthly salary"
    
    # 3. Provide feedback for the first batch
    feedback_1 = [
        {"raw": "coffee shop nyc", "name": "Generic Coffee", "category": "Food & Drink"},
        {"raw": "monthly salary", "name": "Salary", "category": "Income"},
        {"raw": "gas station shell", "name": "Shell", "category": "Gas"},
        {"raw": "starbucks 123", "name": "Starbucks", "category": "Food & Drink"},
        {"raw": "random supermarket", "name": "Supermarket", "category": "Groceries"},
    ]
    response = client.post("/interactive-categorize", json={"session_id": session_id, "feedback": feedback_1})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["status"] == "pending_categorization"
    
    # 4. Get the second batch and verify learning
    unknowns_2 = json_response["unknowns"]
    assert len(unknowns_2) > 0
    
    # "coffee shop nyc #2" should have been auto-categorized due to high similarity
    # and therefore should NOT be in this batch of unknowns.
    coffee_item = next((item for item in unknowns_2 if item["raw"] == "coffee shop nyc #2"), None)
    assert coffee_item is None

    # The raw description "shell gas station #456" should also be auto-categorized
    shell_item = next((item for item in unknowns_2 if item["raw"] == "shell gas station #456"), None)
    assert shell_item is None

    # 5. Provide feedback for the rest and finalize
    feedback_2 = []
    for item in unknowns_2:
        # Just accept the suggestion for the test
        name = item["suggested_name"] or item["raw"]
        category = "Default"
        if name == "Generic Coffee":
            category = "Food & Drink"
        elif name == "Shell":
            category = "Gas"
        feedback_2.append({"raw": item["raw"], "name": name, "category": category})

    response = client.post("/interactive-categorize", json={"session_id": session_id, "feedback": feedback_2})
    assert response.status_code == 200
    json_response = response.json()
    assert json_response["status"] == "completed"
    assert "data" in json_response
    assert len(json_response["data"]) > 0

    # Verify that the final data has the correct categories
    df = pd.DataFrame(json_response["data"])
    # The ledger creates two rows per transaction, so we check one side.
    assert "Food & Drink" in df[df["description"].str.contains("COFFEE SHOP", na=False)]["account"].values
    
    # Check that both auto-categorized items are also correct
    assert "Food & Drink" in df[df["description"] == "COFFEE SHOP NYC #2"]["account"].values
    assert "Gas" in df[df["description"] == "Shell Gas Station #456"]["account"].values

