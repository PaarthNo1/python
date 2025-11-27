# insert_meta_kv.py

from psycopg2.extras import execute_batch
from sqlalchemy.engine import Engine
import numpy as np


def _clean(val):
    """Convert numpy types to normal Python values."""
    if isinstance(val, (np.generic,)):
        return val.item()
    return val


def insert_meta_kv(engine: Engine, rows):
    """
    Fast bulk insert for meta_kv table using psycopg2 execute_batch().
    10x–20x faster than row-by-row inserts.
    """

    # Clean numpy types for safe database insertion
    clean_rows = []
    for r in rows:
        clean_rows.append({k: _clean(v) for k, v in r.items()})

    insert_sql = """
        INSERT INTO meta_kv (
            float_id, var_name, attr_name, value_text, dtype, shape, source_file
        )
        VALUES (
            %(float_id)s, %(var_name)s, %(attr_name)s, %(value_text)s,
            %(dtype)s, %(shape)s, %(source_file)s
        )
        ON CONFLICT DO NOTHING;
    """

    # raw_connection() does NOT support "with", so we close manually
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()

        # Perform batch inserts (best performance: 300–500 rows per batch)
        execute_batch(cur, insert_sql, clean_rows, page_size=500)

        raw_conn.commit()
        cur.close()

    finally:
        raw_conn.close()

    print(f"✔ Inserted {len(rows)} meta key-value rows (FAST batch mode)")

