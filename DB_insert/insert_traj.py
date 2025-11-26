# insert_traj.py

from psycopg2.extras import execute_batch
from sqlalchemy.engine import Engine
import numpy as np


def _clean(val):
    """Convert numpy scalar types to native Python types."""
    if isinstance(val, np.generic):
        return val.item()
    return val


def insert_traj(engine: Engine, rows):
    """
    Fast bulk insert for traj table using psycopg2 execute_batch().
    10x–20x faster than row-by-row inserts.
    """

    # Clean numpy data and round timestamps if needed
    clean_rows = []
    for r in rows:
        cleaned = {}
        for k, v in r.items():

            # Fix nanosecond timestamps
            if k == "juld" and hasattr(v, "round"):
                try:
                    v = v.round("us")
                except Exception:
                    pass

            cleaned[k] = _clean(v)

        clean_rows.append(cleaned)

    insert_sql = """
        INSERT INTO traj (
            float_id, cycle, profile_number, juld, lat, lon,
            position_qc, location_system, source_file
        )
        VALUES (
            %(float_id)s, %(cycle)s, %(profile_number)s, %(juld)s,
            %(lat)s, %(lon)s, %(position_qc)s, %(location_system)s, %(source_file)s
        )
        ON CONFLICT DO NOTHING;
    """

    # raw_connection() is NOT a context manager → use try/finally
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        # Batch insert — page_size=500 is optimal for PostgreSQL
        execute_batch(cur, insert_sql, clean_rows, page_size=500)

        raw_conn.commit()
        cur.close()

    finally:
        raw_conn.close()

    print(f"✔ Inserted {len(rows)} trajectory rows (FAST batch mode)")
