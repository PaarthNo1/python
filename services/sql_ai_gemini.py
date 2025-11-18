"""
NL -> SQL service (Gemini) for OceanIQ.

Features:
- Builds optional RAG context from faiss_service
- Calls Google Gemini with retries + exponential backoff
- Improved SQL validation (allows WITH ... SELECT CTEs)
- Enforces parameter sanitization and server-side LIMIT cap
- Executes SQL on a read-only engine
- Collapses measurement-level rows into one-row-per-profile summaries when appropriate
- Provides deterministic fallback SQL templates when Gemini quota fails
- Audit logging of inputs, SQL, params, and row counts
"""

import os
import json
import logging
import time
from typing import Any, Dict, List, Optional

import sqlparse
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# LLM / Gemini
import google.generativeai as genai

# local services
import services.faiss_service as faiss_service

# Load environment
load_dotenv()

# Configure Gemini (GEMINI_API_KEY in .env)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

# Logging / audit
LOG_PATH = os.getenv("NL_SQL_AUDIT_LOG", "nl_sql_audit.log")
logger = logging.getLogger("nl_sql_audit")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

# MCP / policies file (mcp.json)
with open("mcp.json", "r", encoding="utf-8") as f:
    MCP = json.load(f)

DISALLOWED = [w.lower() for w in MCP["policies"].get("disallowed_statements", [])]
MAX_ROWS = int(MCP["policies"].get("max_rows", 500))
DEFAULT_LIMIT = int(MCP["policies"].get("default_limit", 200))

# System prompt given to Gemini
SYSTEM_PROMPT = """
You are OceanIQ SQL Generator AI.

Database Schema:
TABLE floats(float_id, cycle, profile_number, lat, lon, juld)
TABLE measurements(id, float_id, cycle, profile_number, depth, temp, sal)

Rules:
- Use SELECT only.
- If query needs lat/lon/time → floats table.
- If query needs temp/sal/depth → measurements table.
- If both are needed → JOIN floats f with measurements m ON (f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number).
- Always use parameterized SQL: :p0, :p1, etc.
- Always include LIMIT (:p0).
- Never use DELETE/UPDATE/INSERT/DROP/ALTER/TRUNCATE.
Respond with JSON only:
{
  "sql": "SELECT ... LIMIT :p0",
  "params": { "p0": 200, "p1": "..." },
  "explain": "short explanation"
}
"""

# Read-only DB engine
READONLY_DATABASE_URL = os.getenv("READONLY_DATABASE_URL", os.getenv("DATABASE_URL"))
if not READONLY_DATABASE_URL:
    raise RuntimeError("READONLY_DATABASE_URL or DATABASE_URL must be set in environment.")
readonly_engine = create_engine(READONLY_DATABASE_URL, future=True, pool_pre_ping=True)


# ---------------- RAG context ----------------
def build_rag_context(question: str, top_k: int = 5) -> str:
    """
    Retrieve top_k profile summaries (via FAISS) and return a compact context string for the LLM.
    """
    try:
        results = faiss_service.semantic_search(question, top_k=top_k)
    except Exception as e:
        logger.warning("RAG retrieval failed: %s", str(e))
        results = []

    if not results:
        return ""

    parts: List[str] = []
    for r in results:
        md = r.get("metadata", {})
        uid = r.get("uid", "")
        summary = (r.get("summary") or "")[:1000]
        parts.append(
            f"UID: {uid} | float_id: {md.get('float_id')} | cycle: {md.get('cycle')} | "
            f"lat: {md.get('lat')} | lon: {md.get('lon')} | juld: {md.get('juld')}\nSummary: {summary}"
        )
    return "\n\n".join(parts)


# ---------------- Gemini call with retry/backoff & fallback ----------------
def gemini_generate_with_backoff(model, prompt: str, max_attempts: int = 3, retry_initial: float = 1.0):
    """
    Call model.generate_content with exponential backoff on 429 / quota errors.
    Raises last exception if attempts exhausted.
    """
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
            # detect quota/429 heuristically
            if ("quota" in msg) or ("429" in msg) or ("rate limit" in msg):
                if attempt == max_attempts:
                    raise
                time.sleep(delay)
                delay *= 2
                continue
            raise


def fallback_sql_for_common_patterns(question: str) -> Dict[str, Any]:
    """
    Generate deterministic fallback SQL for a few common patterns.
    This is intentionally conservative and SELECT-only.
    If question contains coordinates, try to extract lat/lon; otherwise return a generic safe query.
    """
    # trivial heuristic extraction for lat/lon (looks for two floats)
    import re
    coords = re.findall(r"(-?\d+\.\d+)", question)
    if len(coords) >= 2:
        try:
            lat = float(coords[0])
            lon = float(coords[1])
        except Exception:
            lat = None
            lon = None
    else:
        lat = None
        lon = None

    # Example fallback: max surface temp per profile in bounding box around lat/lon, else global top profiles
    if lat is not None and lon is not None:
        sql = """
SELECT f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld,
       MAX(m.temp) FILTER (WHERE m.depth < :p_depth) AS max_surface_temp
FROM floats f
JOIN measurements m ON f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number
WHERE f.lat BETWEEN :p_lat_min AND :p_lat_max
  AND f.lon BETWEEN :p_lon_min AND :p_lon_max
GROUP BY f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld
ORDER BY max_surface_temp DESC
LIMIT :p0
"""
        params = {
            "p0": 50,
            "p_depth": 10,
            "p_lat_min": lat - 2.0,
            "p_lat_max": lat + 2.0,
            "p_lon_min": lon - 2.0,
            "p_lon_max": lon + 2.0,
        }
        return {"sql": sql, "params": params, "explain": "Fallback: max surface temp per profile in bounding box"}
    # Generic fallback: latest profiles with non-null temp
    sql = """
SELECT f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld,
       MAX(m.temp) FILTER (WHERE m.depth < :p_depth) AS max_surface_temp
FROM floats f
JOIN measurements m ON f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number
WHERE m.temp IS NOT NULL
GROUP BY f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld
ORDER BY juld DESC
LIMIT :p0
"""
    params = {"p0": 50, "p_depth": 10}
    return {"sql": sql, "params": params, "explain": "Fallback: recent profiles with surface temp"}


def generate_sql_from_nl(question: str, rag_context: Optional[str] = None) -> Dict[str, Any]:
    """
    Ask Gemini to generate a JSON containing sql/params/explain.
    Uses backoff; if Gemini quota fails, return deterministic fallback SQL.
    """
    prompt_parts: List[str] = []
    if rag_context:
        prompt_parts.append("RETRIEVED_PROFILES_CONTEXT:\n" + rag_context)
        prompt_parts.append("\n\n---\n\n")
        prompt_parts.append(
            "INSTRUCTIONS:\n"
            "- Prefer returning ONE ROW PER PROFILE: the profile's maximum surface temperature within the top :p5 meters (or equivalent when the user asks for haloclines, salinity, etc.).\n"
            "- Use GROUP BY or DISTINCT ON so SQL returns a single row per profile when appropriate.\n"
            "- If the RAG context lists UIDs, prefer restricting the query to those UIDs (WHERE (float_id, cycle) IN (...)).\n"
            "- Use parameterized placeholders like :p0, :p1, etc. Always include LIMIT :p0.\n"
            "- Return only JSON in the format: {\"sql\":\"...\",\"params\":{...},\"explain\":\"...\"}.\n"
        )
        prompt_parts.append("\n\n---\n\n")

    prompt_parts.append("USER_QUESTION:\n" + question)
    prompt = SYSTEM_PROMPT + "\n\n" + "\n".join(prompt_parts)

    # If no Gemini API key configured, skip model call and fallback immediately
    if not GEMINI_API_KEY:
        logger.info("GEMINI_API_KEY not set — using deterministic fallback.")
        return fallback_sql_for_common_patterns(question)

    model = genai.GenerativeModel("models/gemini-2.5-pro")
    try:
        response = gemini_generate_with_backoff(model, prompt, max_attempts=3, retry_initial=1.0)
    except Exception as e:
        logger.warning("Gemini call failed after retries: %s. Using deterministic fallback.", str(e))
        return fallback_sql_for_common_patterns(question)

    # parse JSON response (gemini returns JSON string when response_mime_type set)
    try:
        payload = json.loads(response.text)
    except Exception as e:
        logger.error("Failed to parse Gemini JSON response: %s | raw: %s", str(e), response.text[:2000])
        # fallback
        return fallback_sql_for_common_patterns(question)

    return payload


# ---------------- SQL validation & sanitization ----------------
def validate_sql(payload: dict) -> bool:
    """
    Robust SQL validation:
    - Allows a single trailing semicolon (common from LLMs/editors).
    - Rejects multi-statement SQL (true multiple statements).
    - Allows queries starting with WITH (CTE) as long as there's a SELECT.
    - Blocks disallowed keywords anywhere in the SQL (case-insensitive).
    - Requires a LIMIT.
    """
    sql = payload.get("sql", "")
    if not sql or not isinstance(sql, str):
        raise ValueError("No SQL returned by LLM")

    # Trim whitespace
    sql_stripped = sql.strip()

    # Remove a single trailing semicolon (and trailing whitespace) if present.
    # But do NOT remove semicolons inside the statement.
    if sql_stripped.endswith(";"):
        # remove only one trailing semicolon
        sql_stripped = sql_stripped[:-1].rstrip()

    # After removing trailing semicolon, reject any remaining semicolons (multi-statement)
    if ";" in sql_stripped:
        raise ValueError("Semicolons are not allowed in SQL (multiple statements).")

    # Remove comments to help tokenization
    cleaned = sqlparse.format(sql_stripped, strip_comments=True).strip()
    if not cleaned:
        raise ValueError("SQL is empty after stripping comments.")

    parsed = sqlparse.parse(cleaned)
    if not parsed:
        raise ValueError("Unable to parse SQL.")

    # Reject if more than one top-level statement
    if len(parsed) > 1:
        raise ValueError("Only a single SELECT statement is allowed.")

    first_stmt = parsed[0]

    # Find first meaningful token
    first_keyword = None
    for tok in first_stmt.flatten():
        if tok.is_whitespace or tok.ttype is sqlparse.tokens.Comment:
            continue
        val = str(tok).strip().lower()
        if val:
            first_keyword = val
            break

    if not first_keyword:
        raise ValueError("Unable to detect SQL verb.")

    # Allow SELECT or WITH (CTE with SELECT inside)
    if first_keyword not in ("select", "with"):
        raise ValueError("Only SELECT queries allowed")

    if first_keyword == "with":
        has_select = any(str(tok).strip().lower() == "select" for tok in first_stmt.flatten())
        if not has_select:
            raise ValueError("CTE present but no SELECT found; only SELECT queries allowed")

    # Disallowed keyword check
    low = cleaned.lower()
    for bad in DISALLOWED:
        if bad in low:
            raise ValueError(f"Disallowed SQL keyword: {bad}")

    # Ensure LIMIT exists
    if "limit" not in low:
        raise ValueError("SQL must include LIMIT")

    return True



def enforce_and_sanitize_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Ensure numeric limits are capped and param types are safe.
    Guarantee p0 exists and is <= MAX_ROWS.
    """
    if not isinstance(params, dict):
        params = {}

    if "p0" not in params:
        params["p0"] = DEFAULT_LIMIT

    for k, v in list(params.items()):
        if isinstance(v, int):
            if v < 0:
                params[k] = 0
            elif v > MAX_ROWS:
                params[k] = MAX_ROWS
        if isinstance(v, float):
            # keep floats as is (depths, thresholds)
            pass
        if isinstance(v, str) and len(v) > 2000:
            params[k] = v[:2000]

    # final ensure p0
    try:
        params["p0"] = int(params.get("p0", DEFAULT_LIMIT))
    except Exception:
        params["p0"] = DEFAULT_LIMIT

    if params["p0"] > MAX_ROWS:
        params["p0"] = MAX_ROWS

    return params


# ---------------- Execute SQL ----------------
def execute_sql(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Execute parameterized SQL using readonly_engine and return rows as list of dicts.
    """
    sql = payload["sql"]
    params = payload.get("params", {})

    with readonly_engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result.fetchall()]
    return rows


# ---------------- Collapse helper ----------------
def collapse_rows_to_profiles(rows: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """
    Convert raw measurement rows (many depth-level rows per profile)
    into one summary row per profile:
      {
        float_id, cycle, profile_number, lat, lon, juld,
        max_surface_temp, depth_at_max
      }
    Sorted descending by max_surface_temp, limited to `limit`.
    """
    if not rows:
        return []

    profiles: Dict[Any, Dict[str, Any]] = {}
    for r in rows:
        t = r.get("temp")
        if t is None:
            continue
        key = (r.get("float_id"), int(r.get("cycle")) if r.get("cycle") is not None else None)
        try:
            temp_val = float(t)
        except Exception:
            continue

        cur = profiles.get(key)
        if cur is None or temp_val > cur["max_surface_temp"]:
            profiles[key] = {
                "float_id": r.get("float_id"),
                "cycle": int(r.get("cycle")) if r.get("cycle") is not None else None,
                "profile_number": r.get("profile_number"),
                "lat": r.get("lat"),
                "lon": r.get("lon"),
                "juld": r.get("juld"),
                "max_surface_temp": temp_val,
                "depth_at_max": r.get("depth"),
            }

    out = list(profiles.values())
    out.sort(key=lambda x: x.get("max_surface_temp", -9999), reverse=True)
    return out[:int(limit)]


# ---------------- Public NL→SQL flow ----------------
def nl_to_sql_and_execute(question: str, use_rag: bool = False, top_k: int = 5) -> Dict[str, Any]:
    """
    Public function called by the API.
    If use_rag is True, we retrieve FAISS results and include them in the LLM prompt.
    Returns dict: explain, sql, params, rows
    """
    # 1) RAG context if requested
    rag_context = None
    retrieved_uids: List[str] = []
    if use_rag:
        rag_context = build_rag_context(question, top_k=top_k)
        if rag_context:
            for line in rag_context.splitlines():
                if line.startswith("UID:"):
                    try:
                        retrieved_uids.append(line.split("|")[0].replace("UID:", "").strip())
                    except Exception:
                        pass

    # 2) generate SQL via Gemini (or fallback)
    payload = generate_sql_from_nl(question, rag_context=rag_context)
    # Log raw SQL for debugging (trim large text)
    logger.info("LLM payload sql (raw): %s", (payload.get("sql") or "")[:4000])
    logger.info("LLM payload params: %s", payload.get("params"))

    # 3) validate SQL structurally
    validate_sql(payload)

    # 4) sanitize params server-side
    params = enforce_and_sanitize_params(payload.get("params", {}))
    payload["params"] = params

    # 5) execute on readonly engine
    rows = execute_sql(payload)

    # 6) audit
    logger.info(
        "NLQ_EXECUTED | question=%s | use_rag=%s | retrieved=%s | sql=%s | params=%s | rows=%d",
        question,
        use_rag,
        retrieved_uids,
        (payload.get("sql") or "")[:2000],
        params,
        len(rows),
    )

    # 7) collapse measurement rows to profile summaries if rows appear to be measurement-level
    try:
        if rows and isinstance(rows, list) and isinstance(rows[0], dict) and "temp" in rows[0]:
            final_limit = params.get("p0", DEFAULT_LIMIT)
            collapsed = collapse_rows_to_profiles(rows, final_limit)
            return {"explain": payload.get("explain", ""), "sql": payload.get("sql"), "params": params, "rows": collapsed}
    except Exception as e:
        logger.warning("Collapsing rows failed: %s", str(e))

    # default: return raw rows
    return {"explain": payload.get("explain", ""), "sql": payload.get("sql"), "params": params, "rows": rows}