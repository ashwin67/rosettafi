from rosetta.logic.cleaning import TextCleaner

def test_text_cleaner_dates():
    cleaner = TextCleaner()
    raw = "PURCHASE 12/10/2023 AMAZON.COM"
    cleaned = cleaner.clean(raw)
    # Check that AMAZON and COM are there, even if dot is gone
    assert "AMAZON" in cleaned
    assert "COM" in cleaned
    assert "12/10/2023" not in cleaned

def test_text_cleaner_card_numbers():
    cleaner = TextCleaner()
    raw = "VISA XXXX4444 STARBUCKS LONDON"
    cleaned = cleaner.clean(raw)
    assert "STARBUCKS" in cleaned
    assert "LONDON" in cleaned
    assert "XXXX4444" not in cleaned
    assert "VISA" not in cleaned

def test_text_cleaner_ids():
    cleaner = TextCleaner()
    raw = "POS AUTH 88291029384756 AH TO GO"
    cleaned = cleaner.clean(raw)
    assert "AH TO GO" in cleaned
    assert "88291029384756" not in cleaned
    assert "POS" not in cleaned
    assert "AUTH" not in cleaned

def test_text_cleaner_case_insensitivity():
    cleaner = TextCleaner()
    raw = "pos purchase amazon"
    cleaned = cleaner.clean(raw)
    assert cleaned == "AMAZON"
