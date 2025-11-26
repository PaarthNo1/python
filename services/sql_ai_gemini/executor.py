# executor.py
from sqlalchemy import create_engine, text
from typing import List, Dict, Any, Tuple
from .config import READONLY_DATABASE_URL
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re

logger = logging.getLogger("nl_sql_audit.db")

_engine = None
IST = ZoneInfo("Asia/Kolkata")

def get_readonly_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(READONLY_DATABASE_URL, future=True, pool_pre_ping=True)
    return _engine

# -------------------- Date parsing helpers (no extra files needed) --------------------

def _floor_day(dt: datetime) -> datetime:
    dt = dt.astimezone(IST)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=IST)

def _parse_single_date(text: str) -> datetime:
    """
    Parse a single human date into local midnight (IST).
    Supports:
      - '25 nov 2025'
      - '25-11-25', '25/11/2025'
      - '2025-11-25'
      - 'today', 'yesterday', 'tomorrow'
    """
    s = (text or "").strip().lower()
    now = datetime.now(IST)

    if s in ("today", "td"):
        return _floor_day(now)
    if s in ("yesterday", "yd"):
        return _floor_day(now - timedelta(days=1))
    if s in ("tomorrow", "tmr", "tmrw"):
        return _floor_day(now + timedelta(days=1))

    # dd-mm-yy or dd-mm-yyyy (also accepts /)
    m = re.fullmatch(r"(\d{1,2})[-/](\d{1,2})[-/](\d{2}|\d{4})", s)
    if m:
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        return _floor_day(datetime(y, mon, d, tzinfo=IST))

    # try python-dateutil if available for things like '25 nov 2025'
    try:
        import dateutil.parser as du
        dt = du.parse(s, dayfirst=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        else:
            dt = dt.astimezone(IST)
        return _floor_day(dt)
    except Exception:
        raise ValueError(f"Unrecognized date: '{text}'")

def _parse_date_window_from_text(text: str) -> Tuple[datetime, datetime]:
    """
    Accepts a single date OR a simple range like '25-11-25 to 28-11-25'.
    Returns (start, end) where end is exclusive.
    """
    s = (text or "").strip().lower()
    # split on ' to ', ' and ', or ' - ' as a range indicator
    parts = re.split(r"\s+(to|and|-)\s+", s)
    if len(parts) >= 3 and parts[1] in ("to", "and", "-"):
        left, right = parts[0], parts[-1]
        start = _parse_single_date(left)
        end = _parse_single_date(right) + timedelta(days=1)
        if end <= start:
            raise ValueError(f"Invalid date range '{text}': end must be after start.")
        return start, end

    start = _parse_single_date(s)
    end = start + timedelta(days=1)
    return start, end

def _looks_like_human_date(val: Any) -> bool:
    """Heuristic: strings with words or dd-mm style are likely human dates."""
    if not isinstance(val, str):
        return False
    s = val.strip().lower()
    if s in ("today", "td", "yesterday", "yd", "tomorrow", "tmr", "tmrw"):
        return True
    if re.fullmatch(r"\d{1,2}[-/]\d{1,2}[-/](\d{2}|\d{4})", s):
        return True
    if re.search(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)", s):
        return True
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return True
    return False

def _normalize_param_keys(params: Dict[str, Any]) -> Dict[str, Any]:
    """Strip leading ':' so both ':p5' and 'p5' work."""
    norm = {}
    for k, v in (params or {}).items():
        key = k[1:] if isinstance(k, str) and k.startswith(":") else k
        norm[key] = v
    return norm

def _apply_date_params_if_explicit(sql: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Only set p5/p6 if the caller explicitly supplied a date via:
      - params['date_text']  (e.g., '25 nov 2025', 'today')
      - params['p5'] / params['p6'] (in human or ISO date form)
    If SQL references :p5/:p6 but no explicit date supplied -> raise.
    If SQL does not reference :p5/:p6 -> do nothing even if date_text was provided.
    """
    needs_dates = (":p5" in sql and ":p6" in sql)
    norm = _normalize_param_keys(params)

    if not needs_dates:
        # Query doesn't use date placeholders; don't inject anything.
        return norm

    # Check if user explicitly gave any date info
    explicit = any(k in norm for k in ("date_text", "p5", "p6"))

    if not explicit:
        # The SQL expects p5/p6 but the caller didn't ask for a date → fail fast.
        raise ValueError("Missing date parameters: SQL requires :p5 and :p6, but no date was provided.")

    # Priority 1: date_text (single source of truth)
    if "date_text" in norm and norm["date_text"] is not None and str(norm["date_text"]).strip() != "":
        start, end = _parse_date_window_from_text(str(norm["date_text"]))
        norm["p5"] = start.isoformat()
        norm["p6"] = end.isoformat()
        return norm

    # Priority 2: p5/p6 individually; if human, parse; if only p5 provided, set p6 = p5 + 1 day
    if "p5" in norm and _looks_like_human_date(norm["p5"]):
        start = _parse_single_date(str(norm["p5"]))
        norm["p5"] = start.isoformat()
        if "p6" not in norm or _looks_like_human_date(norm["p6"]):
            norm["p6"] = (start + timedelta(days=1)).isoformat()

    if "p6" in norm and _looks_like_human_date(norm["p6"]):
        end_start = _parse_single_date(str(norm["p6"]))
        norm["p6"] = (end_start + timedelta(days=1)).isoformat()

    # Final validation: both must exist and end > start
    if "p5" not in norm or "p6" not in norm:
        raise ValueError("Incomplete date parameters: provide date_text or both p5 and p6.")
    try:
        s = datetime.fromisoformat(norm["p5"])
        e = datetime.fromisoformat(norm["p6"])
        if e <= s:
            raise ValueError("Invalid date window: p6 must be after p5.")
    except Exception as ex:
        raise ValueError(f"Bad date parameters: {ex}")

    return norm

# -------------------- Main execution --------------------

def execute_sql(sql_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    sql = sql_payload["sql"]
    params = sql_payload.get("params", {}) or {}

    # Only apply date window if user supplied date info; otherwise leave params alone.
    safe_params = _apply_date_params_if_explicit(sql, params)

    engine = get_readonly_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), safe_params)
        rows = [dict(r._mapping) for r in result.fetchall()]
    logger.debug("Executed SQL rows=%d", len(rows))
    return rows
