from pydantic import BaseModel, Field
import instructor
import ollama
from openai import OpenAI
from rosetta.data.constants import (
    CATEGORIZER_SYSTEM_PROMPT, 
    LLM_MODEL_NAME, 
    LLM_BASE_URL, 
    LLM_API_KEY,
    UNKNOWN_CATEGORY
)
from rosetta.config import get_logger

logger = get_logger(__name__)

# Pydantic Model for Structured Output
class CategorizationDecision(BaseModel):
    reasoning: str = Field(..., description="Brief chain-of-thought explaining the classification.")
    category: str = Field(..., description="The final selected or created category.")

class AgentLayer:
    """
    Layer 4: Intelligent Agent.
    Uses LLM with Chain-of-Thought reasoning to classify new concepts.
    """
    
    def __init__(self):
        # Using Instructor for structured outputs
        self.client = instructor.from_openai(
            OpenAI(
                base_url=LLM_BASE_URL,
                api_key=LLM_API_KEY, 
            ),
            mode=instructor.Mode.JSON,
        )

    def ask_agent(self, description: str, context_categories: list[str]) -> str:
        if not description:
            return UNKNOWN_CATEGORY

        try:
            prompt = CATEGORIZER_SYSTEM_PROMPT.format(
                existing_categories=", ".join(context_categories)
            )
            
            logger.info(f"Asking Agent about: '{description}'")
            
            # Call LLM
            decision = self.client.chat.completions.create(
                model=LLM_MODEL_NAME,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": description}
                ],
                response_model=CategorizationDecision,
                max_retries=1
            )
            
            logger.info(f"AGENT DECISION: {decision.category} | Reason: {decision.reasoning}")
            return decision.category.strip()
            
        except Exception as e:
            logger.error(f"Agent failed for '{description}': {e}")
            return UNKNOWN_CATEGORY
