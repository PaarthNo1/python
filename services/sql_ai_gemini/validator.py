# validator.py
import sqlparse
from typing import Dict, Any
# GOOD — relative import inside the package
from .config import DISALLOWED


def validate_sql(payload: Dict[str, Any]) -> bool:
    sql = payload.get("sql", "")
    if not sql or not isinstance(sql, str):
        raise ValueError("No SQL returned by LLM")

    sql_stripped = sql.strip()
    if sql_stripped.endswith(";"):
        sql_stripped = sql_stripped[:-1].rstrip()

    if ";" in sql_stripped:
        raise ValueError("Semicolons are not allowed in SQL (multiple statements).")

    cleaned = sqlparse.format(sql_stripped, strip_comments=True).strip()
    if not cleaned:
        raise ValueError("SQL is empty after stripping comments.")

    parsed = sqlparse.parse(cleaned)
    if not parsed:
        raise ValueError("Unable to parse SQL.")
    if len(parsed) > 1:
        raise ValueError("Only a single SELECT statement is allowed.")

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

    low = cleaned.lower()
    for bad in DISALLOWED:
        if bad in low:
            raise ValueError(f"Disallowed SQL keyword: {bad}")

    if "limit" not in low:
        raise ValueError("SQL must include LIMIT")
    return True
