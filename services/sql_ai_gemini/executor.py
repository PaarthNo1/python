# executor.py
from sqlalchemy import create_engine, text
from typing import List, Dict, Any
from .config import READONLY_DATABASE_URL
import logging

logger = logging.getLogger("nl_sql_audit.db")

_engine = None

def get_readonly_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(READONLY_DATABASE_URL, future=True, pool_pre_ping=True)
    return _engine

def execute_sql(sql_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    sql = sql_payload["sql"]
    params = sql_payload.get("params", {}) or {}
    engine = get_readonly_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = [dict(r._mapping) for r in result.fetchall()]
    logger.debug("Executed SQL rows=%d", len(rows))
    return rows
