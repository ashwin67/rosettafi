import pytest
import pandas as pd
import os
import time
import instructor
from openai import OpenAI
from rosetta.logic.categorization.segmentation import LLMSegmenter
from rosetta.data.constants import TOKENIZATION_PROMPT

# Skip if no OLLAMA available - rudimentary check or just let it fail if user wants to run it.
# We'll use a mark for integration.
# @pytest.mark.integration
def test_real_llm_pattern_learning():
    """
    Integration test: Hits the real LLM (Llama 3.2 via Ollama) to see if it can 
    actually induce the correct pattern for the complex iDEAL string.
    """
    
    # Setup Real Client
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
    api_key = os.getenv("OLLAMA_API_KEY", "ollama")
    
    try:
        client = instructor.from_openai(
            OpenAI(base_url=base_url, api_key=api_key),
            mode=instructor.Mode.JSON,
        )
        # Simple ping check using structured output to satisfy instructor
        from pydantic import BaseModel
        class PingResponse(BaseModel):
            message: str

        client.chat.completions.create(
            model="llama3.2",
            response_model=PingResponse,
            messages=[{"role": "user", "content": "say pong"}],
            max_tokens=10
        )
    except Exception as e:
        pytest.skip(f"Skipping integration test, LLM not reachable: {e}")

    segmenter = LLMSegmenter(client=client, model="llama3.2")
    
    # The Complex Case
    complex_desc = "/TRTP/iDEAL/IBAN/NL27INGB0000026500/BIC/INGBNL2A/NAME/bol.com b.v./REMI/4129973829 0051100576503291 bol.com 41-29-97-38-29 bol.com/EREF/01-09-2024 20:57 0051100576503291"
    # Run
    if not segmenter.client:
        pytest.skip("No LLM client available (OLLAMA_BASE_URL not reachable)")
        
    start = time.time()
    start = time.time()
    # Pass 'complex_desc' in a list
    results = segmenter.tokenize_batch([complex_desc], TOKENIZATION_PROMPT)
    
    print(f"\n[INTEGRATION] LLM Returned: {results}")
    
    assert len(results) == 1
    parts = results[0]
    
    # We expect 'bol.com' or 'b.v.' or similar in parts
    # And technical tags in parts
    keywords_str = " ".join(parts).lower()
    
    print(f"[INTEGRATION] Extracted Parts: '{keywords_str}'")
    
    assert "bol.com" in keywords_str
    # Check if key tokens are present
    assert "trtp" in keywords_str or "ideal" in keywords_str
