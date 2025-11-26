# services/sql_ai_gemini/validator.py
import re
import sqlparse
from typing import Dict, Any
from .config import DISALLOWED

def validate_sql(payload: Dict[str, Any]) -> bool:
    sql = payload.get("sql", "")
    if not sql or not isinstance(sql, str):
        raise ValueError("No SQL returned by LLM")

    # Strip trailing semicolon (we only allow a single statement without ';')
    sql_stripped = sql.strip()
    if sql_stripped.endswith(";"):
        sql_stripped = sql_stripped[:-1].rstrip()

    # Disallow multiple statements
    if ";" in sql_stripped:
        raise ValueError("Semicolons are not allowed in SQL (multiple statements).")

    # Remove comments and normalize
    cleaned = sqlparse.format(sql_stripped, strip_comments=True).strip()
    if not cleaned:
        raise ValueError("SQL is empty after stripping comments.")

    # Parse and enforce single statement
    parsed = sqlparse.parse(cleaned)
    if not parsed:
        raise ValueError("Unable to parse SQL.")
    if len(parsed) > 1:
        raise ValueError("Only a single SELECT statement is allowed.")

    # Ensure top-level verb is SELECT (or WITH ... SELECT)
    first_stmt = parsed[0]
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
    if first_keyword not in ("select", "with"):
        raise ValueError("Only SELECT queries allowed")
    if first_keyword == "with":
        has_select = any(str(tok).strip().lower() == "select" for tok in first_stmt.flatten())
        if not has_select:
            raise ValueError("CTE present but no SELECT found; only SELECT queries allowed")

    # Disallow dangerous keywords
    low = cleaned.lower()
    for bad in DISALLOWED:
        if bad in low:
            raise ValueError(f"Disallowed SQL keyword: {bad}")

    # Require LIMIT
    if " limit " not in f" {low} ":
        raise ValueError("SQL must include LIMIT")

    # ---------------- New guardrail you asked for ----------------
    # Sensor param must not collide with bbox params (p1..p4)
    if "m.sensor" in low:
        matches = re.findall(r"m\.sensor\s*=\s*:(p\d+)", low)
        for p in matches:
            n = int(p[1:])
            if n <= 4:
                raise ValueError("Sensor parameter cannot use p1..p4 (reserved for geographic bbox).")
    # -------------------------------------------------------------

    return True
