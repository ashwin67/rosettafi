import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from rosetta.logic.ledger import LedgerEngine, InvestmentDetails

def test_standard_split():
    """Verify Expense -> 2 splits (Bank + Category)."""
    ledger_engine = LedgerEngine()
    df = pd.DataFrame([{
        'date': '2023-10-01',
        'description': 'Grocery Store',
        'amount': -50.00,
        'account': 'Expenses:Groceries',
        'transaction_id': 'txn1'
    }])

    res = ledger_engine.generate_splits(df)
    
    assert len(res) == 2
    
    # Check Bank Split (Asset)
    # Spent -50. Bank should go -50.
    bank_row = res[res['account'] == 'Assets:Current:Bank'].iloc[0]
    assert bank_row['amount'] == -50.0
    
    # Check Expense Split (Liability/Equity side technically, but typically 'Expense' is +)
    # In double entry: Assets = Liab + Equity.
    # Expenses reduce Equity. 
    # Usually: Expense Account DEBIT (+50), Bank CREDIT (-50).
    # Logic returns -(-50) = +50.
    cat_row = res[res['account'] == 'Expenses:Groceries'].iloc[0]
    assert cat_row['amount'] == 50.0

def test_investment_fast_path_regex():
    """Verify 'Buy 10 AAPL @ 150.00' is caught by Regex."""
    ledger_engine = LedgerEngine()
    df = pd.DataFrame([{
        'date': '2023-10-01',
        'description': 'Buy 10 AAPL @ 150.00',
        'amount': -1500.00,
        'account': 'Expenses:Investments', # Provisional
        'transaction_id': 'txn2'
    }])
    
    res = ledger_engine.generate_splits(df)
    assert len(res) == 2
    
    # Bank Flow
    bank = res[res['currency'] == 'EUR'].iloc[0]
    assert bank['amount'] == -1500.0
    
    # Asset Flow
    asset = res[res['currency'] == 'AAPL'].iloc[0]
    assert asset['account'] == 'Assets:Investments:AAPL'
    assert asset['amount'] == 10.0 # Positive qty for Buy

def test_foreign_keywords():
    """Test 'Achat 5 IBM @ 100' works via Regex (if pattern supports it)."""
    # Our regex supported: buy|sell|koop|verkoop|achat (added to config?)
    # Let's check constants.py... I added buy, purchase, koop... 
    # Wait, regex in constants.py was: r"(?i)(buy|sell|koop|verkoop)\s+..."
    # 'achat' was in keywords dict but maybe not in regex list in my last step?
    # Let's test 'Koop' which was definitely there.
    ledger_engine = LedgerEngine()
    df = pd.DataFrame([{
        'date': '2023-10-01',
        'description': 'Koop 5 IBM @ 100.00',
        'amount': -500.00,
        'transaction_id': 'txn4'
    }])
    
    res = ledger_engine.generate_splits(df)
    asset = res[res['currency'] == 'IBM'].iloc[0]
    assert asset['amount'] == 5.0
