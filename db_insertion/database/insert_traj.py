# insert_traj.py

from psycopg2.extras import execute_batch
from sqlalchemy.engine import Engine
import numpy as np

def _clean(val):
    if isinstance(val, np.generic):
        return val.item()
    if isinstance(val, np.ndarray):
        if val.size == 1:
            return val.item()
    return val

def insert_traj(conn, rows):
    """
    Fast bulk insert for traj table (PostGIS ready).
    """
    clean_rows = []
    for r in rows:
        cleaned = {}
        for k, v in r.items():
            if k == "juld" and hasattr(v, "round"):
                try:
                    v = v.round("us")
                except Exception:
                    pass
            cleaned[k] = _clean(v)
        clean_rows.append(cleaned)

    insert_sql = """
        INSERT INTO traj (
            float_id, cycle, profile_number, juld,
            lat, lon, position_qc, location_system, measurement_code,
            satellite_name, juld_qc,
            source_file, geom
        )
        VALUES (
            %(float_id)s, %(cycle)s, %(profile_number)s, %(juld)s,
            %(lat)s, %(lon)s, %(position_qc)s, %(location_system)s, %(measurement_code)s,
            %(satellite_name)s, %(juld_qc)s,
            %(source_file)s,
            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
        )
        ON CONFLICT DO NOTHING;
    """

    try:
        raw_conn = conn.connection
        cur = raw_conn.cursor()
        
        # ---------------------------------------------------------
        # NEW LOGIC: Check for existing data to avoid duplicates
        # ---------------------------------------------------------
        # Trajectory data is per float/cycle.
        unique_keys = set()
        for r in clean_rows:
            if "float_id" in r and "cycle" in r:
                unique_keys.add((str(r["float_id"]), int(r["cycle"])))
        
        existing_keys = set()
        for fid, cyc in unique_keys:
            cur.execute("SELECT 1 FROM traj WHERE float_id = %s AND cycle = %s LIMIT 1", (fid, cyc))
            if cur.fetchone():
                existing_keys.add((fid, cyc))
                # print(f"⏩ Skipping duplicate TRAJ data for Float {fid} Cycle {cyc}")
        
        final_rows = []
        for r in clean_rows:
            key = (str(r.get("float_id")), int(r.get("cycle")))
            if key not in existing_keys:
                final_rows.append(r)
                
        if not final_rows:
            # print("✔ All TRAJ rows were duplicates. Nothing to insert.")
            cur.close()
            return

        execute_batch(cur, insert_sql, final_rows, page_size=500)
        cur.close()
        # DO NOT commit/close here
    except Exception as e:
        print(f"❌ Error in traj insert: {e}")
        raise

    # print(f"✔ Inserted {len(rows)} trajectory rows (FAST + POSTGIS mode)")
