# services/sql_ai_gemini/router.py

import json
import logging
import google.generativeai as genai
from typing import Dict, Any

from .prompts import ROUTER_PROMPT
from .config import GEMINI_API_KEY
from .gemini_client import gemini_generate_with_backoff

logger = logging.getLogger("nl_sql_audit.router")

def classify_intent(question: str) -> str:
    """
    Classifies the user's question into GENERAL_CHAT, DATA_QUERY, or IRRELEVANT.
    """
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY missing, defaulting to DATA_QUERY")
        return "DATA_QUERY"

    model = genai.GenerativeModel("models/gemini-2.5-pro")
    prompt = f"{ROUTER_PROMPT}\n\nUSER INPUT:\n{question}"

    try:
        response = gemini_generate_with_backoff(model, prompt, max_attempts=2)
        text = response.text.strip()
        
        # Clean up markdown if present
        if text.startswith("```json"):
            text = text[7:]
        if text.endswith("```"):
            text = text[:-3]
        
        data = json.loads(text)
        intent = data.get("intent", "DATA_QUERY")
        logger.info(f"Intent classified: {intent} for question: {question}")
        return intent

    except Exception as e:
        logger.error(f"Router failed: {e}. Defaulting to DATA_QUERY.")
        return "DATA_QUERY"
