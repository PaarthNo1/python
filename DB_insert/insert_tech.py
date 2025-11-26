# insert_tech.py

from psycopg2.extras import execute_batch
from sqlalchemy.engine import Engine
import numpy as np


def _clean_value(v):
    """Convert numpy scalar types to pure Python types."""
    if isinstance(v, np.generic):
        return v.item()
    return v


def insert_tech(engine: Engine, tech_rows):
    """
    Fast bulk insert for tech table using psycopg2 execute_batch().
    This avoids per-row inserts and reduces DB round-trips dramatically.
    """

    # Clean numpy types and timestamps for each row
    clean_rows = []
    for row in tech_rows:
        cleaned = {}

        for k, v in row.items():

            # Fix juld-like timestamps (nanosecond precision)
            if k == "collected_at" and hasattr(v, "round"):
                try:
                    v = v.round("us")
                except Exception:
                    pass

            cleaned[k] = _clean_value(v)

        clean_rows.append(cleaned)

    insert_sql = """
        INSERT INTO tech (
            float_id, cycle, param_name, param_value,
            units, collected_at, source_file
        )
        VALUES (
            %(float_id)s, %(cycle)s, %(param_name)s, %(param_value)s,
            %(units)s, %(collected_at)s, %(source_file)s
        )
        ON CONFLICT DO NOTHING;
    """

    # raw_connection() is NOT a context manager → use try/finally
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        # Batch insert (best performance at 300–500 rows per batch)
        execute_batch(cur, insert_sql, clean_rows, page_size=500)

        raw_conn.commit()
        cur.close()

    finally:
        raw_conn.close()

    print(f"✔ Inserted {len(tech_rows)} tech log rows (FAST batch mode)")
