import os
from pathlib import Path

class Workspace:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Workspace, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.base_path = Path.home() / ".rosetta_cache"
        self.configs_dir = self.base_path / "configs"
        self.memory_dir = self.base_path / "memory"
        self.logs_dir = self.base_path / "logs"
        self.quarantine_dir = self.base_path / "quarantine"
        self.temp_dir = self.base_path / "temp"

        self._ensure_structure()
        self._initialized = True

    def _ensure_structure(self):
        """Ensures that the workspace directory structure exists."""
        dirs = [
            self.base_path,
            self.configs_dir,
            self.memory_dir,
            self.logs_dir,
            self.quarantine_dir,
            self.temp_dir
        ]
        
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)

    def get_bank_config_path(self) -> str:
        """Returns the absolute path for bank_configs.json."""
        return str(self.configs_dir / "bank_configs.json")

    def get_memory_path(self) -> str:
        """Returns the absolute path for category_memory.json."""
        return str(self.memory_dir / "category_memory.json")

    def get_quarantine_path(self) -> str:
        """Returns the absolute path for the quarantine directory."""
        return str(self.quarantine_dir)
