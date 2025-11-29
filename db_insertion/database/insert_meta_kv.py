# insert_meta_kv.py
from sqlalchemy.engine import Engine
from sqlalchemy import text
import numpy as np


def _clean(val):
    """Convert numpy scalar types to normal Python values."""
    if isinstance(val, np.generic):
        return val.item()
    if isinstance(val, np.ndarray):
        if val.size == 1:
            return val.item()
    return val


def insert_meta_kv(conn, rows):
    """
    Fast & stable bulk insert for meta_kv table.
    - Accepts list of dicts (rich metadata).
    - Uses SQLAlchemy executemany via conn.execute(sql, list_of_dicts).
    - ON CONFLICT DO NOTHING to avoid duplicate meta rows.
    """

    # Guard: nothing to insert
    if not rows:
        return

    # Clean numpy types for safe database insertion
    clean_rows = []
    for r in rows:
        clean_rows.append({k: _clean(v) for k, v in r.items()})

    insert_sql = text("""
        INSERT INTO meta_kv (
            float_id, var_name, attr_name, value_text, dtype, shape, source_file
        )
        VALUES (
            :float_id, :var_name, :attr_name, :value_text,
            :dtype, :shape, :source_file
        )
        ON CONFLICT DO NOTHING;
    """)

    # Execute using passed connection
    # conn.execute(insert_sql, clean_rows)
    
    # ---------------------------------------------------------
    # NEW LOGIC: Check for existing data to avoid duplicates
    # ---------------------------------------------------------
    # Meta KV is usually per float. If we have any meta for this float, assume we have it all.
    # Or we can check if we are inserting new keys.
    # Simplest safe approach: Check if this float has ANY meta_kv entries.
    
    if not clean_rows:
        return

    float_id = str(clean_rows[0].get("float_id"))
    
    # We need a cursor for SELECT
    # Since conn is SQLAlchemy Connection, we can use execute() with text()
    
    check_sql = text("SELECT 1 FROM meta_kv WHERE float_id = :fid LIMIT 1")
    result = conn.execute(check_sql, {"fid": float_id}).fetchone()
    
    if result:
        # print(f"⏩ Skipping duplicate META_KV data for Float {float_id}")
        return

    conn.execute(insert_sql, clean_rows)

    # print(f"✔ Inserted {len(clean_rows)} meta key-value rows (FAST, safe mode)")
