import duckdb
import numpy as np
from pathlib import Path
from typing import Optional, List, Tuple

class RosettaDB:
    def __init__(self, db_path: str = "rosetta.db"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._setup()

    def _setup(self):
        # Install and load VSS extension if not already present
        # Note: In some environments, extensions might need to be pre-installed
        try:
            self.conn.execute("INSTALL vss;")
            self.conn.execute("LOAD vss;")
        except Exception as e:
            print(f"Warning: Could not load VSS extension: {e}")
            
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS merchants (
                canonical_name TEXT PRIMARY KEY,
                default_category TEXT,
                vector_embedding FLOAT[384]
            );
        """)

    def upsert_merchant(self, canonical_name: str, default_category: str, vector_embedding: List[float]):
        if len(vector_embedding) != 384:
            raise ValueError(f"Expected embedding of size 384, got {len(vector_embedding)}")
            
        self.conn.execute("""
            INSERT OR REPLACE INTO merchants (canonical_name, default_category, vector_embedding)
            VALUES (?, ?, ?)
        """, [canonical_name, default_category, vector_embedding])

    def find_nearest_merchant(self, query_embedding: List[float], threshold: float = 0.85) -> Optional[Tuple[str, str, float]]:
        if len(query_embedding) != 384:
            raise ValueError(f"Expected embedding of size 384, got {len(query_embedding)}")

        # array_cosine_similarity returns 1.0 for identical vectors, 0.0 for orthogonal
        res = self.conn.execute("""
            SELECT canonical_name, default_category, 
                   array_cosine_similarity(vector_embedding, ?::FLOAT[384]) as similarity
            FROM merchants
            WHERE array_cosine_similarity(vector_embedding, ?::FLOAT[384]) >= ?
            ORDER BY similarity DESC
            LIMIT 1
        """, [query_embedding, query_embedding, threshold]).fetchone()
        
        return res

    def close(self):
        self.conn.close()
