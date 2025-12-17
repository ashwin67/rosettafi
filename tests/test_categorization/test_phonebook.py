
import pytest
import os
import json
from rosetta.logic.categorization.phonebook import Phonebook
from rosetta.models import MerchantEntity

@pytest.fixture
def clean_phonebook(tmp_path):
    """
    Creates a temporary phonebook file for testing.
    """
    db_file = tmp_path / "test_merchants.json"
    
    # Initialize with valid empty structure
    with open(db_file, 'w') as f:
        json.dump({"entities": {}}, f)
        
    # Patch the singleton path in the instance we create
    pb = Phonebook()
    pb.db_path = db_file
    pb.entities = {}
    pb.alias_index = {}
    return pb

def test_create_and_load(clean_phonebook):
    """Test that we can save to disk and reload."""
    
    # 1. Modify
    clean_phonebook.register_entity("Test Corp", "Business")
    
    # 2. Reload new instance pointing to same file
    new_pb = Phonebook()
    new_pb.db_path = clean_phonebook.db_path
    new_pb.load()
    
    assert "test_corp" in new_pb.entities
    assert new_pb.entities["test_corp"].canonical_name == "Test Corp"
    assert new_pb.entities["test_corp"].default_category == "Business"

def test_register_new_entity(clean_phonebook):
    """Test registering a fresh entity."""
    clean_phonebook.register_entity("New Guy", "Personal")
    
    entity = clean_phonebook.find_entity_by_alias("New Guy")
    assert entity is not None
    assert entity.canonical_name == "New Guy"
    assert entity.default_category == "Personal"

def test_update_entity_safe(clean_phonebook):
    """
    Test that updating an entity (e.g. adding alias) 
    does NOT overwrite the category if None is passed.
    """
    # 1. Create with category
    clean_phonebook.register_entity("Stable Corp", "Finance")
    
    # 2. Update with new alias but NO category
    clean_phonebook.register_entity("Stable Corp", category=None, aliases=["Stable Inc"])
    
    entity = clean_phonebook.entities["stable_corp"]
    
    # Category should remain "Finance"
    assert entity.default_category == "Finance"
    # Alias should be added
    assert "Stable Inc" in entity.aliases
    assert clean_phonebook.find_entity_by_alias("Stable Inc") == entity

def test_lookup_by_alias(clean_phonebook):
    """Test O(1) alias lookup."""
    clean_phonebook.register_entity("Big Corp", "Biz", aliases=["BC", "BigC"])
    
    # Canonical
    assert clean_phonebook.find_entity_by_alias("Big Corp").canonical_name == "Big Corp"
    # Aliases
    assert clean_phonebook.find_entity_by_alias("BC").canonical_name == "Big Corp"
    assert clean_phonebook.find_entity_by_alias("bigc").canonical_name == "Big Corp" # Case insensitive
