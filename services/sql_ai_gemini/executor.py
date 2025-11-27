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

# -------------------- Regex guards --------------------
# 1) If LLM used :p1/:p2 for juld window, upgrade to :p5/:p6
_DATE_WINDOW_P1P2 = re.compile(r"(?is)\bjuld\s*>=\s*:p1\b.*?\bjuld\s*<\s*:p2\b")
# 2) Correct juld window using :p5/:p6
_DATE_WINDOW_P5P6 = re.compile(r"(?is)\bjuld\s*>=\s*:p5\b.*?\bjuld\s*<\s*:p6\b")
# 3) Same-day form
_DATE_CAST_P5 = re.compile(r"(?is)cast\s*\(\s*juld\s+as\s+date\s*\)\s*=\s*:p5\b")
# 4) Misuse of :p5/:p6 for depth ranges
_DEPTH_P5P6 = re.compile(r"(?is)\b(u\.pres|m\.depth_m)\s+between\s+:p5\s+and\s+:p6\b")

def _enforce_p56_date_placeholders(sql: str) -> Tuple[str, bool]:
    """
    If the SQL uses a date window like `juld >= :p1 AND juld < :p2`,
    rewrite it to `juld >= :p5 AND juld < :p6`.

    Returns (sql, rewrote_flag)
    """
    rewrote = False
    if _DATE_WINDOW_P1P2.search(sql):
        sql = re.sub(r":p1\b", ":p5", sql)
        sql = re.sub(r":p2\b", ":p6", sql)
        rewrote = True
    return sql, rewrote

def _remap_depth_p56_to_p78(sql: str, params: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
    """
    If depth filters incorrectly use :p5/:p6 (reserved for dates),
    rewrite them to :p7/:p8 and map params accordingly.
    """
    if not _DEPTH_P5P6.search(sql):
        return sql, params, False
    sql = re.sub(r":p5\b", ":p7", sql)
    sql = re.sub(r":p6\b", ":p8", sql)
    norm = _normalize_param_keys(params)
    if "p7" not in norm and ("p5" in norm):
        norm["p7"] = norm["p5"]
    if "p8" not in norm and ("p6" in norm):
        norm["p8"] = norm["p6"]
    return sql, norm, True

# -------------------- Date parsing helpers --------------------

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

def _from_iso_z_ok(s: str) -> datetime:
    """
    Parse ISO 8601 timestamps, tolerating a trailing 'Z' by converting to '+00:00'.
    """
    s = str(s)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)

def _sql_needs_dates(sql: str) -> bool:
    """True only if SQL actually has a juld date constraint using :p5/:p6 or CAST(juld AS DATE)=:p5."""
    return bool(_DATE_WINDOW_P5P6.search(sql) or _DATE_CAST_P5.search(sql))

def _apply_date_params_if_explicit(sql: str, params: Dict[str, Any], rewrote_from_p12: bool = False) -> Dict[str, Any]:
    """
    Handle p5/p6 ONLY when SQL has a juld constraint that uses them.
    - Accept date_text or p5/p6 (and p1/p2 if sql was rewritten from p1/p2).
    - Validate ISO (support trailing 'Z').
    """
    needs_dates = _sql_needs_dates(sql)
    norm = _normalize_param_keys(params)

    if not needs_dates:
        return norm

    # If SQL was rewritten from p1/p2 → p5/p6, allow p1/p2 as source for dates
    if rewrote_from_p12 and "p5" not in norm and "p6" not in norm and ("p1" in norm or "p2" in norm):
        if "p1" in norm: norm["p5"] = norm["p1"]
        if "p2" in norm: norm["p6"] = norm["p2"]

    explicit = any(k in norm for k in ("date_text", "p5", "p6"))
    if not explicit:
        raise ValueError("Missing date parameters: SQL requires :p5 and :p6, but no date was provided.")

    if "date_text" in norm and str(norm["date_text"]).strip():
        start, end = _parse_date_window_from_text(str(norm["date_text"]))
        norm["p5"] = start.isoformat()
        norm["p6"] = end.isoformat()

    if "p5" in norm and _looks_like_human_date(norm["p5"]):
        start = _parse_single_date(str(norm["p5"]))
        norm["p5"] = start.isoformat()
        if "p6" not in norm or _looks_like_human_date(norm.get("p6")):
            norm["p6"] = (start + timedelta(days=1)).isoformat()

    if "p6" in norm and _looks_like_human_date(norm["p6"]):
        end_start = _parse_single_date(str(norm["p6"]))
        norm["p6"] = (end_start + timedelta(days=1)).isoformat()

    if "p5" not in norm or "p6" not in norm:
        raise ValueError("Incomplete date parameters: provide date_text or both p5 and p6.")
    try:
        s = _from_iso_z_ok(norm["p5"])
        e = _from_iso_z_ok(norm["p6"])
        if e <= s:
            raise ValueError("Invalid date window: p6 must be after p5.")
    except Exception as ex:
        raise ValueError(f"Bad date parameters: {ex}")

    return norm

# -------------------- Main execution --------------------

def execute_sql(sql_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    sql = sql_payload["sql"]
    params = sql_payload.get("params", {}) or {}

    # Step 1: normalize juld windows written as :p1/:p2 → :p5/:p6
    sql, rewrote_dates = _enforce_p56_date_placeholders(sql)

    # Step 2: if depth BETWEEN misused :p5/:p6, rewrite them to :p7/:p8 and remap params
    # Only attempt if there is no juld window using :p5/:p6 (dates win if both appear).
    if not _sql_needs_dates(sql):
        sql, params, _ = _remap_depth_p56_to_p78(sql, params)

    # Step 3: apply date params ONLY if SQL actually has a juld constraint using p5/p6
    safe_params = _apply_date_params_if_explicit(sql, params, rewrote_from_p12=rewrote_dates)

    engine = get_readonly_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), safe_params)
        rows = [dict(r._mapping) for r in result.fetchall()]
    logger.debug("Executed SQL rows=%d", len(rows))
    return rows
