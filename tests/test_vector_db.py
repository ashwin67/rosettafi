import pytest
import os
from rosetta.database import RosettaDB

def test_vector_db_lifecycle():
    db_path = "test_vec.db"
    if os.path.exists(db_path):
        os.remove(db_path)
        
    db = RosettaDB(db_path)
    
    # 384-dim vectors
    # vector A: [1, 0, 0, ...]
    vec_a = [0.0] * 384
    vec_a[0] = 1.0
    
    # vector B: [0, 1, 0, ...]
    vec_b = [0.0] * 384
    vec_b[1] = 1.0
    
    db.upsert_merchant("Target", "Shopping", vec_a)
    db.upsert_merchant("Shell", "Transport", vec_b)
    
    # Test exact match
    res = db.find_nearest_merchant(vec_a, threshold=0.99)
    assert res is not None
    assert res[0] == "Target"
    assert res[1] == "Shopping"
    
    # Test similarity search
    # Vector slightly different from vec_b
    vec_b_prime = [0.0] * 384
    vec_b_prime[1] = 0.99
    vec_b_prime[2] = 0.01
    res = db.find_nearest_merchant(vec_b_prime, threshold=0.9)
    assert res is not None
    assert res[0] == "Shell"
    
    # Test low similarity
    vec_c = [0.0] * 384
    vec_c[5] = 1.0
    res = db.find_nearest_merchant(vec_c, threshold=0.95)
    assert res is None
    
    db.close()
    if os.path.exists(db_path):
        os.remove(db_path)
