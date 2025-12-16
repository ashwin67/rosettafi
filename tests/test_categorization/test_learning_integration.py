import pytest
import pandas as pd
import os
import instructor
from openai import OpenAI
from rosetta.logic.categorization.segmentation import LLMSegmenter
from rosetta.data.constants import ENTITY_SEGMENTATION_PROMPT

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
    
    print(f"\n[INTEGRATION] Asking LLM to segment: {complex_desc[:50]}...")
    
    # Batch of 1
    results = segmenter.segment_batch([complex_desc], ENTITY_SEGMENTATION_PROMPT)
    
    print(f"\n[INTEGRATION] LLM Returned: {results}")
    
    assert len(results) == 1
    descriptions = results[0].get("descriptions", [])
    keywords = results[0].get("keywords", [])
    
    # We expect 'bol.com b.v.' or similar in descriptions
    # And technical tags in keywords
    extracted_text = " ".join(descriptions)
    
    print(f"[INTEGRATION] Extracted Description: '{extracted_text}'")
    
    assert "bol.com" in extracted_text.lower()
    # Relaxed assertion: Check if key tokens are present even if slashes are stripped
    keywords_str = str(keywords)
    assert "TRTP" in keywords_str or "iDEAL" in keywords_str or "IDEAL" in keywords_str
