# sanitizer.py
from typing import Dict, Any
from .config import DEFAULT_LIMIT, MAX_ROWS

def enforce_and_sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
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
            pass
        if isinstance(v, str) and len(v) > 2000:
            params[k] = v[:2000]

    try:
        params["p0"] = int(params.get("p0", DEFAULT_LIMIT))
    except Exception:
        params["p0"] = DEFAULT_LIMIT

    if params["p0"] > MAX_ROWS:
        params["p0"] = MAX_ROWS

    return params
