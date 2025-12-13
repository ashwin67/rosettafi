import pytest
from rosetta.logic.categorization.rules import RulesLayer

def test_hard_rules_match():
    # "hypotheek" -> "Expenses:Housing:Mortgage"
    assert RulesLayer.apply_hard_rules("Termijnbetaling Hypotheek Augustus") == "Expenses:Housing:Mortgage"
    
    # "albert heijn" -> "Expenses:Groceries"
    assert RulesLayer.apply_hard_rules("Albert Heijn 1234 Amsterdam") == "Expenses:Groceries"

def test_hard_rules_case_insensitive():
    assert RulesLayer.apply_hard_rules("NETFLIX.COM") == "Expenses:Subscriptions"

def test_hard_rules_no_match():
    assert RulesLayer.apply_hard_rules("Unknown Merchant") is None

def test_hard_rules_empty():
    assert RulesLayer.apply_hard_rules(None) is None
    assert RulesLayer.apply_hard_rules("") is None
