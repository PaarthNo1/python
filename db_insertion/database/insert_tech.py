
# insert_tech.py
from psycopg2.extras import execute_batch
from sqlalchemy.engine import Engine
import numpy as np


def _clean_value(v):
    """Convert numpy scalar types to pure Python types."""
    if isinstance(v, np.generic):
        return v.item()
    if isinstance(v, np.ndarray):
        if v.size == 1:
            return v.item()
    return v


def insert_tech(conn, float_id, tech_rows):
    """
    Fast bulk insert for tech table using psycopg2 execute_batch().
    This avoids per-row inserts and reduces DB round-trips dramatically.
    """

    # Clean numpy types and timestamps for each row
    clean_rows = []
    for row in tech_rows:
        cleaned = {}
        # Inject float_id if missing
        if "float_id" not in row:
            cleaned["float_id"] = float_id

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

    # Use existing connection's cursor
    try:
        raw_conn = conn.connection
        cur = raw_conn.cursor()

        # Batch insert (best performance at 300–500 rows per batch)
        
        # ---------------------------------------------------------
        # NEW LOGIC: Check for existing data to avoid duplicates
        # ---------------------------------------------------------
        # Tech data is usually per cycle. Let's check which cycles we are trying to insert.
        cycles_to_insert = set()
        for r in clean_rows:
            if "cycle" in r:
                cycles_to_insert.add(int(r["cycle"]))
        
        existing_cycles = set()
        for cyc in cycles_to_insert:
            cur.execute("SELECT 1 FROM tech WHERE float_id = %s AND cycle = %s LIMIT 1", (str(float_id), cyc))
            if cur.fetchone():
                existing_cycles.add(cyc)
                # print(f"⏩ Skipping duplicate TECH data for Float {float_id} Cycle {cyc}")

        # Filter rows
        final_rows = [r for r in clean_rows if int(r.get("cycle", -1)) not in existing_cycles]

        if not final_rows:
            # print("✔ All TECH rows were duplicates. Nothing to insert.")
            cur.close()
            return

        execute_batch(cur, insert_sql, final_rows, page_size=500)
        
        cur.close()
        # DO NOT commit/close here

    except Exception as e:
        print(f"❌ Error in tech insert: {e}")
        raise

    # print(f"✔ Inserted {len(tech_rows)} tech log rows (FAST batch mode)")
