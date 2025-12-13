import pytest
import pandas as pd
import numpy as np
from rosetta.rules import RulesEngine, USParsingStrategy, EUParsingStrategy
from rosetta.models import ColumnMapping, DecimalSeparator, PolarityCaseA, PolarityCaseB, PolarityCaseC

# ==============================================================================
# STRATEGY TESTS
# ==============================================================================

def test_us_strategy_parsing():
    strategy = USParsingStrategy()
    
    # Standard
    assert strategy.parse_float("1234.56") == 1234.56
    assert strategy.parse_float("1,234.56") == 1234.56
    
    # Clean Currency
    assert strategy.parse_float("$1,234.56") == 1234.56
    assert strategy.parse_float("USD 1,234.56") == 1234.56
    assert strategy.parse_float("-€50.00") == -50.00
    
    # Weird spacing
    assert strategy.parse_float(" 1, 2 34 . 5 6 ") == 1234.56
    
    # Unicode Minus
    assert strategy.parse_float("−500.00") == -500.00

def test_eu_strategy_parsing():
    strategy = EUParsingStrategy()
    
    # Standard: Dot thousand, Comma decimal
    assert strategy.parse_float("1.234,56") == 1234.56
    
    # Space as thousand separator
    assert strategy.parse_float("1 234,56") == 1234.56
    
    # Clean Currency
    assert strategy.parse_float("€ 1.234,56") == 1234.56
    assert strategy.parse_float("1.234,56 EUR") == 1234.56
    
    # Negative
    assert strategy.parse_float("-50,00") == -50.00

def test_dirty_inputs():
    """Test resilience against None, garbage strings, etc."""
    us_strat = USParsingStrategy()
    eu_strat = EUParsingStrategy()
    
    # NaN/None
    assert us_strat.parse_float(None) == 0.0
    assert us_strat.parse_float(np.nan) == 0.0
    assert eu_strat.parse_float(None) == 0.0
    
    # Empty string
    assert us_strat.parse_float("") == 0.0
    
    # Garbage
    assert us_strat.parse_float("NotANumber") == 0.0

# ==============================================================================
# RULES ENGINE LOGIC TESTS (POLARITY)
# ==============================================================================

@pytest.fixture
def base_df():
    return pd.DataFrame({
        'Date': ['2023-01-01', '2023-01-02'],
        'Description': ['Salary', 'Groceries']
    })

def test_case_a_signed(base_df):
    """Case A (Signed): -50 is Expense, +1000 is Income."""
    df = base_df.copy()
    df['Amount'] = ['1000.00', '-50.00']
    
    mapping = ColumnMapping(
        date_col='Date',
        amount_col='Amount',
        desc_col='Description',
        decimal_separator=DecimalSeparator.DOT,
        polarity=PolarityCaseA()
    )
    
    engine = RulesEngine(mapping)
    result = engine.apply(df)
    
    assert result.iloc[0]['amount'] == 1000.00
    assert result.iloc[1]['amount'] == -50.00

def test_case_b_direction(base_df):
    """Case B: Absolute Amount + Direction Column (Credit/Debit words)"""
    df = base_df.copy()
    df['Amount'] = ['1000.00', '50.00'] # All positive usually
    df['Type'] = ['Credit', 'Debit']
    
    mapping = ColumnMapping(
        date_col='Date',
        amount_col='Amount',
        desc_col='Description',
        decimal_separator=DecimalSeparator.DOT,
        polarity=PolarityCaseB(
            direction_col='Type',
            incoming_value='Credit',
            outgoing_value='Debit'
        )
    )
    
    engine = RulesEngine(mapping)
    result = engine.apply(df)
    
    assert result.iloc[0]['amount'] == 1000.00
    assert result.iloc[1]['amount'] == -50.00

def test_case_c_split(base_df):
    """Case C: Separate Credit and Debit Columns"""
    df = base_df.copy()
    df['Credit'] = ['1000.00', '']
    df['Debit'] = ['', '50.00']
    
    mapping = ColumnMapping(
        date_col='Date',
        desc_col='Description',
        decimal_separator=DecimalSeparator.DOT,
        polarity=PolarityCaseC(
            credit_col='Credit',
            debit_col='Debit'
        )
    )
    
    engine = RulesEngine(mapping)
    result = engine.apply(df)
    
    assert result.iloc[0]['amount'] == 1000.00
    assert result.iloc[1]['amount'] == -50.00

def test_date_cleaning(base_df):
    """Test that weird dates (integers, .0 floats) are handled."""
    df = base_df.copy()
    df['Date'] = [20230101, '20230102.0'] # Int, Float-as-string
    df['Amount'] = ['100', '200']
    
    mapping = ColumnMapping(
        date_col='Date',
        amount_col='Amount',
        desc_col='Description',
        decimal_separator=DecimalSeparator.DOT,
        polarity=PolarityCaseA()
    )
    
    engine = RulesEngine(mapping)
    result = engine.apply(df)
    
    assert str(result.iloc[0]['date'].date()) == '2023-01-01'
    assert str(result.iloc[1]['date'].date()) == '2023-01-02'
