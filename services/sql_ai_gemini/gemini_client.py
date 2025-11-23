# gemini_client.py
import time
import json
import logging
import google.generativeai as genai
from typing import Optional, Dict, Any
# inside services/sql_ai_gemini/gemini_client.py
from .prompts import SYSTEM_PROMPT
from .fallbacks import fallback_sql_for_common_patterns
from .config import GEMINI_API_KEY  # if config.py is in services/

logger = logging.getLogger("nl_sql_audit.gemini")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

def gemini_generate_with_backoff(model, prompt: str, max_attempts: int = 3, retry_initial: float = 1.0):
    delay = retry_initial
    for attempt in range(1, max_attempts + 1):
        try:
            return model.generate_content(
                prompt,
                generation_config={"temperature": 0, "response_mime_type": "application/json"}
            )
        except Exception as e:
            msg = str(e).lower()
            logger.warning("Gemini attempt %d failed: %s", attempt, msg)
            if ("quota" in msg) or ("429" in msg) or ("rate limit" in msg):
                if attempt == max_attempts:
                    raise
                time.sleep(delay)
                delay *= 2
                continue
            raise

def generate_sql_from_prompt(question: str, rag_context: Optional[str] = None) -> Dict[str, Any]:
    parts = []
    if rag_context:
        parts.append("RETRIEVED_PROFILES_CONTEXT:\n" + rag_context)
        parts.append("\n\n---\n\n")
        parts.append("INSTRUCTIONS:\n- Prefer returning ONE ROW PER PROFILE: ...")
        parts.append("\n\n---\n\n")

    parts.append("USER_QUESTION:\n" + question)
    prompt = SYSTEM_PROMPT + "\n\n" + "\n".join(parts)

    if not GEMINI_API_KEY:
        logger.info("GEMINI_API_KEY not set — using deterministic fallback.")
        return fallback_sql_for_common_patterns(question)

    model = genai.GenerativeModel("models/gemini-2.5-pro")
    try:
        response = gemini_generate_with_backoff(model, prompt, max_attempts=3, retry_initial=1.0)
    except Exception as e:
        logger.warning("Gemini call failed after retries: %s. Using deterministic fallback.", str(e))
        return fallback_sql_for_common_patterns(question)

    try:
        return json.loads(response.text)
    except Exception as e:
        logger.error("Failed to parse Gemini JSON response: %s | raw: %s", str(e), getattr(response, "text", "")[:2000])
        return fallback_sql_for_common_patterns(question)
