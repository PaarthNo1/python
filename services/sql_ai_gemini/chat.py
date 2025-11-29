# services/sql_ai_gemini/chat.py

import logging
import google.generativeai as genai
from typing import Dict, Any

from .prompts import CHAT_PROMPT
from .config import GEMINI_API_KEY
from .gemini_client import gemini_generate_with_backoff

logger = logging.getLogger("nl_sql_audit.chat")

def handle_general_chat(question: str) -> Dict[str, Any]:
    """
    Handles general chat interactions (greetings, identity) without RAG/SQL.
    """
    if not GEMINI_API_KEY:
        return {"type": "plain_text", "text": "I am OceanIQ. Please configure my API key."}

    model = genai.GenerativeModel("models/gemini-2.5-pro")
    prompt = CHAT_PROMPT.format(user_question=question)

    try:
        response = gemini_generate_with_backoff(model, prompt, max_attempts=2, mime_type="text/plain")
        text = response.text.strip().replace("\n", " ").replace("  ", " ")
        return {"type": "plain_text", "text": text}

    except Exception as e:
        logger.error(f"Chat handler failed: {e}")
        return {"type": "plain_text", "text": "I'm having trouble responding right now. Please try again."}
