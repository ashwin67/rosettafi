import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from rosetta.mapper import get_column_mapping, heuristic_map_columns
from rosetta.models import ColumnMapping, DecimalSeparator

# Helper to mock LLM failure so we hit fallback
@pytest.fixture
def mock_llm_fail():
    with patch('rosetta.mapper._get_llm_mapping', side_effect=Exception("LLM Mock Failure")):
        yield

@pytest.fixture
def mock_config_missing():
    # Ensure no config is found so we trigger logic
    with patch('os.path.exists', return_value=False):
        yield

# ==============================================================================
# STANDARD CASES (ENGLISH)
# ==============================================================================

def test_heuristic_standard_us(mock_llm_fail, mock_config_missing):
    """Test standard US headers: Date, Amount, Description"""
    df = pd.DataFrame(columns=['Date', 'Amount', 'Description'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Date'
    assert mapping.amount_col == 'Amount'
    assert mapping.desc_col == 'Description'
    assert mapping.decimal_separator == DecimalSeparator.DOT
    assert mapping.polarity.type == 'signed'

# ==============================================================================
# CASE C: CREDIT / DEBIT (USA / UK)
# ==============================================================================

def test_heuristic_credit_debit(mock_llm_fail, mock_config_missing):
    """Test Case C: Separate Credit and Debit columns"""
    df = pd.DataFrame(columns=['Date', 'Description', 'Credit', 'Debit'])
    mapping = get_column_mapping(df)
    
    assert mapping.polarity.type == 'credit_debit'
    assert mapping.polarity.credit_col == 'Credit'
    assert mapping.polarity.debit_col == 'Debit'

def test_heuristic_direction_column(mock_llm_fail, mock_config_missing):
    """Test Case B: Amount + Direction (Type/CD)"""
    df = pd.DataFrame(columns=['Date', 'Amount', 'Type', 'Description'])
    mapping = get_column_mapping(df)
    
    assert mapping.polarity.type == 'direction'
    assert mapping.polarity.direction_col == 'Type'

# ==============================================================================
# INTERNATIONAL CASES (DUTCH, GERMAN, SPANISH)
# ==============================================================================

def test_heuristic_standard_eu(mock_llm_fail, mock_config_missing):
    """Test standard EU headers (Dutch): Datum, Bedrag, Omschrijving"""
    df = pd.DataFrame(columns=['Ref', 'Transactiedatum', 'Transactiebedrag', 'Omschrijving'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Transactiedatum'
    assert mapping.amount_col == 'Transactiebedrag'
    assert mapping.desc_col == 'Omschrijving'
    assert mapping.decimal_separator == DecimalSeparator.COMMA

def test_heuristic_german(mock_llm_fail, mock_config_missing):
    """Test German headers: Datum, Betrag, Verwendungszweck"""
    df = pd.DataFrame(columns=['Datum', 'Betrag', 'Verwendungszweck', 'Waehrung'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Datum'
    assert mapping.amount_col == 'Betrag'
    # Verwendungszweck is a keyword for description now
    assert mapping.desc_col == 'Verwendungszweck' 
    assert mapping.decimal_separator == DecimalSeparator.COMMA

def test_heuristic_spanish(mock_llm_fail, mock_config_missing):
    """Test Spanish headers: Fecha, Importe, Concepto"""
    df = pd.DataFrame(columns=['Fecha', 'Importe', 'Saldo', 'Concepto'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Fecha'
    assert mapping.amount_col == 'Importe'
    assert mapping.desc_col == 'Concepto'
    assert mapping.decimal_separator == DecimalSeparator.COMMA # Assuming comma for 'Importe'

# ==============================================================================
# EDGE CASES & ROBUSTNESS
# ==============================================================================

def test_garbage_headers_fallback(mock_llm_fail, mock_config_missing):
    """
    Test Garbage headers -> Should degrade gracefully.
    Logic: Date=1st col, Amount=2nd col, Desc=Last col.
    """
    df = pd.DataFrame(columns=['Col1', 'Col2', 'Col3', 'Col4'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Col1'
    assert mapping.amount_col == 'Col2'
    assert mapping.desc_col == 'Col4'

def test_header_cleanup(mock_llm_fail, mock_config_missing):
    """Test headers with spaces are stripped/matched correctly"""
    df = pd.DataFrame(columns=[' Date ', ' Amount ', ' Description '])
    mapping = get_column_mapping(df)
    
    # The mapper should return the original keys STRIPPED of whitespace by its preprocessing
    assert mapping.date_col == 'Date'
    assert mapping.amount_col == 'Amount'
    assert mapping.desc_col == 'Description'

def test_mixed_case_headers(mock_llm_fail, mock_config_missing):
    """Test Mixed Case non-standard headers"""
    # "tRaNsAcTiOn dAtE", "vAlUe", "nOtEs"
    df = pd.DataFrame(columns=['tRaNsAcTiOn dAtE', 'vAlUe', 'nOtEs'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'tRaNsAcTiOn dAtE'
    assert mapping.amount_col == 'vAlUe'
    # 'nOtEs' might not be in keywords? Let's check constants.py
    # KEYWORDS_DESC has 'desc', 'text', 'narr', 'memo', 'payee', 'omschrijving', 'naam', 'name', 'book'.
    # Does it have 'notes'? No. 
    # Fallback for Desc is Last Column.
    assert mapping.desc_col == 'nOtEs'

def test_emoji_headers(mock_llm_fail, mock_config_missing):
    """Test Headers with Emojis (modern app exports)"""
    df = pd.DataFrame(columns=['📅 Date', '💰 Amount', '📝 Description'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == '📅 Date'
    assert mapping.amount_col == '💰 Amount'
    assert mapping.desc_col == '📝 Description'

def test_duplicate_keywords(mock_llm_fail, mock_config_missing):
    """
    Scenario: Two columns match 'Amount' keyword. 
    e.g. 'Transaction Amount' vs 'Billing Amount'.
    Heuristic finds FIRST match usually.
    """
    df = pd.DataFrame(columns=['Date', 'Billing Amount', 'Transaction Amount', 'Desc'])
    mapping = get_column_mapping(df)
    
    # Logic iterates columns. 'Billing Amount' comes first.
    assert mapping.amount_col == 'Billing Amount'

def test_two_column_file(mock_llm_fail, mock_config_missing):
    """
    Scenario: Only Date and Amount. No description.
    Fallback for Desc is last col (-1).
    """
    df = pd.DataFrame(columns=['Date', 'Amount'])
    mapping = get_column_mapping(df)
    
    assert mapping.date_col == 'Date'
    assert mapping.amount_col == 'Amount'
    assert mapping.desc_col == 'Amount' # Last col is Amount. It overlaps.

def test_direct_logic_invocation():
    """Test the standalone logic function strictly without the Mapper wrapper."""
    headers = ['Datum', 'Bedrag']
    mapping = heuristic_map_columns(headers)
    assert mapping.date_col == 'Datum'
    assert mapping.amount_col == 'Bedrag'
