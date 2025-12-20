import json
import os
from typing import Dict, Optional, List
from rosetta.models import MerchantEntity, ContextRule
from rosetta.workspace import Workspace
from rosetta.utils import get_logger

logger = get_logger(__name__)

class Phonebook:
    """
    The Master Data Engine.
    Manages the 'merchants.json' file which maps canonical Entities to Aliases.
    """
    
    def __init__(self):
        self.workspace = Workspace()
        self.file_path = self.workspace.get_phonebook_path()
        self.entities: Dict[str, MerchantEntity] = {}
        self.alias_index: Dict[str, str] = {} # Map alias -> entity_id
        
        self.load()

    def load(self):
        """Loads entities from disk."""
        if not os.path.exists(self.file_path):
            logger.info("No phonebook found. Starting fresh.")
            return

        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
                for key, val in data.items():
                    entity = MerchantEntity(**val)
                    self.entities[key] = entity
                    # Rebuild alias index
                    for alias in entity.aliases:
                        self.alias_index[alias.lower()] = entity.id
                    # Also map canonical name
                    self.alias_index[entity.canonical_name.lower()] = entity.id
            logger.info(f"Phonebook loaded: {len(self.entities)} entities.")
        except Exception as e:
            logger.error(f"Failed to load phonebook: {e}")

    def save(self):
        """Saves entities to disk."""
        try:
            data = {k: v.model_dump() for k, v in self.entities.items()}
            with open(self.file_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info("Phonebook saved.")
        except Exception as e:
            logger.error(f"Failed to save phonebook: {e}")

    def find_entity_by_alias(self, name: str) -> Optional[MerchantEntity]:
        """Direct O(1) lookup by exact alias match."""
        if not name: return None
        entity_id = self.alias_index.get(name.lower().strip())
        if entity_id:
            return self.entities.get(entity_id)
        return None

    def register_entity(self, name: str, category: Optional[str] = None, aliases: List[str] = None):
        """
        Creates or Updates an entity in the phonebook.
        """
        slug = name.lower().replace(" ", "_").strip()
        
        if slug in self.entities:
            # Update existing
            entity = self.entities[slug]
            if category:
                entity.default_category = category
            if aliases:
                for a in aliases:
                    if a not in entity.aliases:
                        entity.aliases.append(a)
                        self.alias_index[a.lower()] = slug
        else:
            # Create new
            entity = MerchantEntity(
                id=slug,
                canonical_name=name,
                default_category=category or "Uncategorized",
                aliases=aliases or []
            )
            self.entities[slug] = entity
            self.alias_index[name.lower()] = slug
            if aliases:
                for a in aliases:
                    self.alias_index[a.lower()] = slug
        
        self.save()

    def get_all_categories(self) -> set:
        """Returns a set of all unique category names."""
        return {entity.default_category for entity in self.entities.values() if entity.default_category}

    def add_context_rule(self, entity_name: str, keyword: str, category: str):
        """Adds a rule like: If 'AWS' in description, category='Tech'."""
        slug = entity_name.lower().replace(" ", "_")
        if slug in self.entities:
            rule = ContextRule(contains_keyword=keyword, assign_category=category)
            self.entities[slug].rules.append(rule)
            self.save()