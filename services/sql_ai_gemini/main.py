# services/sql_ai_gemini/main.py

import logging
import re
from typing import List, Dict, Any, Optional

from .config import LOG_PATH, DEFAULT_LIMIT
from .rag_builder import build_rag_context
from .gemini_client import generate_sql_from_prompt
from .validator import validate_sql
from .sanitizer import enforce_and_sanitize_params
from .executor import execute_sql
from .collapse import collapse_rows_to_profiles


logger = logging.getLogger("nl_sql_audit")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fh = logging.FileHandler(LOG_PATH)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)


# ---------------- HARD GATE: domain relevance ----------------
_OCEAN_TERMS = {
    "ocean", "oceanography", "indian ocean", "atlantic", "pacific", "southern",
    "argo", "float", "floats", "wmo", "profile", "profiles", "cycle", "juld",
    "ctd", "bgc", "trajectory", "traj",
    "temperature", "temp", "salinity", "sal", "psal", "pressure", "pres", "depth",
    "latitude", "lat", "longitude", "lon", "measurements", "nc", "netcdf"
}


def _is_ocean_relevant(question: str) -> bool:
    """
    Conservative domain filter.
    Returns True only if the question clearly contains ocean/ARGO vocabulary.
    Numbers alone (e.g., WMOs) are not considered sufficient.
    """
    if not question:
        return False
    s = question.lower()
    return any(term in s for term in _OCEAN_TERMS)
# --------------------------------------------------------------


def fix_params_using_rag_or_question(
    sql_text: str, params: Dict[str, Any], retrieved_uids: List[str], question: str
) -> Dict[str, Any]:
    """
    Ensure float_id and cycle params are correctly assigned and typed.
    Priority:
      1) If retrieved_uids present and formatted as "floatid_cycle", use that.
      2) Otherwise, heuristically extract numbers from the question (longer number -> float_id).
    Detects placeholders in SQL like 'float_id = :p1' and 'cycle = :p2' and assigns values.
    """
    if not isinstance(params, dict):
        params = {}

    sql_low = (sql_text or "").lower()

    m_f = re.search(r"float_id\s*=\s*:(p\d+)", sql_low)
    m_c = re.search(r"\bcycle\s*=\s*:(p\d+)", sql_low)

    # nothing to fix
    if not m_f and not m_c:
        return params

    fid: Optional[str] = None
    cyc: Optional[int] = None

    # 1) Use RAG UID if present
    if retrieved_uids:
        try:
            uid = retrieved_uids[0]
            if isinstance(uid, str) and "_" in uid:
                a, b = uid.split("_", 1)
                fid = a
                try:
                    cyc = int(b)
                except Exception:
                    cyc = None
        except Exception:
            pass

    # 2) fallback: extract numbers from question
    if fid is None or cyc is None:
        nums = re.findall(r"\b\d{2,}\b", question)
        if nums:
            if len(nums) >= 2:
                sorted_by_len = sorted(nums, key=lambda x: (-len(x), nums.index(x)))
                cand_fid = sorted_by_len[0]
                try:
                    idx = nums.index(cand_fid)
                    if idx + 1 < len(nums):
                        cand_cyc = nums[idx + 1]
                    else:
                        cand_cyc = sorted_by_len[1] if len(sorted_by_len) > 1 else None
                except Exception:
                    cand_cyc = sorted_by_len[1] if len(sorted_by_len) > 1 else None

                if cand_fid and fid is None:
                    fid = cand_fid
                if cand_cyc and cyc is None:
                    try:
                        cyc = int(cand_cyc)
                    except Exception:
                        pass
            else:
                # Single numeric token: ambiguous, avoid overwriting
                pass

    new_params = dict(params)  # shallow copy

    if m_f and fid is not None:
        key = m_f.group(1)
        # ensure string type for float_id
        new_params[key] = str(fid)

    if m_c and cyc is not None:
        key = m_c.group(1)
        try:
            new_params[key] = int(cyc)
        except Exception:
            new_params[key] = cyc

    # Ensure p0 is int & capped by sanitizer downstream (but keep defensive)
    if "p0" in new_params:
        try:
            new_params["p0"] = int(new_params["p0"])
        except Exception:
            new_params["p0"] = DEFAULT_LIMIT

    if new_params != params:
        logger.info(
            "Param alignment applied. Before: %s | After: %s | retrieved_uids=%s",
            params,
            new_params,
            retrieved_uids,
        )

    return new_params


def nl_to_sql_and_execute(question: str, use_rag: bool = False, top_k: int = 5):
    # ----- HARD GATE: reject non-ocean queries up front -----
    if not _is_ocean_relevant(question):
        msg = "I can only help with ocean and ARGO data. Please ask a clear, specific question about ocean or ARGO data."
        logger.info("HARD_GATE_REJECT | question=%s", question)
        return {"type": "plain_text", "text": msg}
    # --------------------------------------------------------

    rag_context = None
    retrieved_uids: List[str] = []

    # ---------------- RAG context ----------------
    if use_rag:
        rag_context = build_rag_context(question, top_k=top_k)
        if rag_context:
            for line in rag_context.splitlines():
                if line.startswith("UID:"):
                    try:
                        retrieved_uids.append(
                            line.split("|")[0].replace("UID:", "").strip()
                        )
                    except Exception:
                        pass

    # ---------------- LLM SQL generation ----------------
    payload = generate_sql_from_prompt(question, rag_context=rag_context)

    # --- DEBUG: print/log the raw LLM response for visibility ---
    try:
        print("LLM raw response:", payload)
    except Exception:
        pass

    logger.info("LLM raw response (repr): %s", repr(payload))

    # ---------------- Defensive payload-shape handling ----------------
    # If LLM returned plain text (string) or a dict missing required SQL keys,
    # return that plaintext immediately (do NOT run validate_sql/execute_sql).
    if not isinstance(payload, dict):
        logger.info("LLM returned non-dict payload; returning plain text. payload_preview=%s", str(payload)[:200])
        return {"type": "plain_text", "text": str(payload)}

    # If payload is a dict but missing SQL keys, consider it plain text-like
    if not all(k in payload for k in ("sql", "params", "explain")):
        logger.info("LLM payload missing SQL keys; treating as plain text. payload_keys=%s", list(payload.keys()))
        text_val = payload.get("text") or payload.get("message") or \
                   "I can only help with ocean and ARGO data. Please ask a clear, specific question about ocean or ARGO data."
        return {"type": "plain_text", "text": str(text_val)}

    # From here on, payload is SQL-shaped (dict with sql, params, explain)
    logger.info("LLM payload sql (raw): %s", (payload.get("sql") or "")[:4000])
    logger.info("LLM payload params: %s", payload.get("params"))

    # ---------------- SQL validation ----------------
    validate_sql(payload)

    # ---------------- Param sanitization ----------------
    params = enforce_and_sanitize_params(payload.get("params", {}))

    # ---- fix param hallucinations using RAG or question heuristics ----
    params = fix_params_using_rag_or_question(payload.get("sql", ""), params, retrieved_uids, question)

    # ---- INDIAN OCEAN server-side override (non-negotiable) ----
    try:
        if "indian ocean" in (question or "").lower():
            # apply the hardcoded bounding box
            params["p1"] = 30.0   # lon_min
            params["p2"] = 120.0  # lon_max
            params["p3"] = -60.0  # lat_min (must be negative)
            params["p4"] = 30.0   # lat_max
            # ensure p0 exists and is int
            try:
                params["p0"] = int(params.get("p0", DEFAULT_LIMIT))
            except Exception:
                params["p0"] = DEFAULT_LIMIT
            logger.info("Applied INDIAN_OCEAN override to params for question: %s", question)
    except Exception as e:
        logger.warning("Indian Ocean override failed: %s | params=%s", str(e), params)

    payload["params"] = params

    # ---------------- SQL execution ----------------
    rows = execute_sql(payload)

    logger.info(
        "NLQ_EXECUTED | question=%s | use_rag=%s | retrieved=%s | sql=%s | params=%s | rows=%d",
        question,
        use_rag,
        retrieved_uids,
        (payload.get("sql") or "")[:2000],
        params,
        len(rows) if isinstance(rows, list) else -1,
    )

    # ---------------- FIXED POST-PROCESSING ----------------
    # RULE:
    #   - If SQL returns more than one row → return them all, DO NOT collapse.
    #   - Collapse only if exactly ONE row exists AND it contains measurement-level columns.

    try:
        # If DB returned multiple rows → return them exactly as-is
        if isinstance(rows, list) and len(rows) > 1:
            return {
                "explain": payload.get("explain", ""),
                "sql": payload.get("sql"),
                "params": params,
                "rows": rows,
            }

        # If no rows
        if not rows:
            return {
                "explain": payload.get("explain", ""),
                "sql": payload.get("sql"),
                "params": params,
                "rows": [],
            }

        # SINGLE ROW CASE → check if collapse is appropriate
        first_row = rows[0] if isinstance(rows, list) else rows
        if isinstance(first_row, dict) and "temp" in first_row:
            final_limit = params.get("p0", DEFAULT_LIMIT)
            collapsed = collapse_rows_to_profiles(rows, final_limit)
            return {
                "explain": payload.get("explain", ""),
                "sql": payload.get("sql"),
                "params": params,
                "rows": collapsed,
            }

    except Exception as e:
        logger.warning("Collapsing decision failed: %s", str(e))

    # Default: return raw rows
    return {
        "explain": payload.get("explain", ""),
        "sql": payload.get("sql"),
        "params": params,
        "rows": rows,
    }
