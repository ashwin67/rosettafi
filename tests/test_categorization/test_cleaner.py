import pytest
from rosetta.logic.categorization.cleaner import CleanerLayer

def test_cleaner_strips_sepa_noise():
    raw = "/TRTP/SEPA Incasso algemeen doorlopend/CSID/NL12ZZZ/NAME/Key4Music/MARF/12398"
    # Matches /TRTP/, /SEPA/, etc.
    # Should leave "Key4Music"
    
    cleaned = CleanerLayer.clean(raw)
    assert "Key4Music" in cleaned
    assert "SEPA" not in cleaned
    assert "TRTP" not in cleaned

def test_cleaner_strips_iban():
    raw = "Omschrijving: Payment for Rent IBAN: NL99INGB000123"
    cleaned = CleanerLayer.clean(raw)
    assert "Payment for Rent" in cleaned
    assert "IBAN:" not in cleaned
    assert "NL99INGB" not in cleaned

def test_cleaner_reverts_if_empty():
    """If we strip everything, return usage or raw string to avoid empty vector."""
    raw = "SEPA Incasso /TRTP/ /NAME/" 
    # This matches multiple patterns and might leave nothing if we aren't careful.
    # The logic says if result is empty, return trimmed raw
    
    cleaned = CleanerLayer.clean(raw)
    assert cleaned == raw.strip()

def test_cleaner_handles_none():
    assert CleanerLayer.clean(None) == ""
