# services/sql_ai_gemini/sanitizer.py

from typing import Dict, Any
from .config import DEFAULT_LIMIT, MAX_ROWS

def enforce_and_sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    # 0) Normalize dict; strip leading ':' from keys (critical for SQLAlchemy binds)
    if not isinstance(params, dict):
        params = {}
    norm: Dict[str, Any] = {}
    for k, v in params.items():
        key = k.lstrip(":") if isinstance(k, str) else k
        norm[key] = v
    params = norm

    # 1) Default limit
    if "p0" not in params:
        params["p0"] = DEFAULT_LIMIT

    # 2) Clamp ints, trim long strings
    for k, v in list(params.items()):
        if isinstance(v, int):
            if v < 0:
                params[k] = 0
            elif v > MAX_ROWS:
                params[k] = MAX_ROWS
        elif isinstance(v, float):
            pass
        elif isinstance(v, str) and len(v) > 2000:
            params[k] = v[:2000]

    # 3) Ensure p0 is int and <= MAX_ROWS
    try:
        params["p0"] = int(params.get("p0", DEFAULT_LIMIT))
    except Exception:
        params["p0"] = DEFAULT_LIMIT
    if params["p0"] > MAX_ROWS:
        params["p0"] = MAX_ROWS

    # 4) Guardrail: p1..p4 are reserved for bbox; do not allow strings there.
    for n in (1, 2, 3, 4):
        k = f"p{n}"
        if k in params and isinstance(params[k], str):
            # If LLM put sensor/date/string into p1..p4, drop it — it’s wrong;
            # validator will force regeneration anyway.
            del params[k]

    return params
