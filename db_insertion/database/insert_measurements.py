# insert_measurements.py
from sqlalchemy.engine import Engine
from psycopg2.extras import execute_batch


def insert_measurements(conn, df):
    """
    High-speed measurement insert (no schema change, duplicates allowed).
    - Uses psycopg2 execute_batch() for fast bulk insert.
    - Rounds juld to microseconds to satisfy PostgreSQL timestamptz.
    """

    # 0) Guard: empty DataFrame to avoid useless DB roundtrip
    if df is None or df.empty:
        print("ℹ No measurement rows to insert.")
        return

    # 1) juld ko microseconds tak round karo (Postgres compatible)
    if "juld" in df.columns:
        col = df["juld"]

        # Agar pura column datetime64[ns] type ka hai → vectorized round
        if str(col.dtype).startswith("datetime64"):
            df["juld"] = col.dt.round("us")
        else:
            # Mixed / object type → per-row safe rounding
            df["juld"] = col.apply(
                lambda x: x.round("us") if hasattr(x, "round") else x
            )

    # (Optional but safe) types ko normalize kar sakte ho:
    # cycle / profile_number ko int me convert karne ki try:
    if "cycle" in df.columns:
        try:
            df["cycle"] = df["cycle"].astype("int32")
        except Exception:
            pass

    if "profile_number" in df.columns:
        try:
            df["profile_number"] = df["profile_number"].astype("int32")
        except Exception:
            pass

    # 2) DataFrame → list[dict] (execute_batch ke liye best format)
    # rows = df.to_dict(orient="records")

    # ---------------------------------------------------------
    # NEW LOGIC: Check for existing data to avoid duplicates
    # ---------------------------------------------------------
    # Extract unique keys (float_id, cycle, profile_number)
    unique_keys = df[["float_id", "cycle", "profile_number"]].drop_duplicates()
    
    rows_to_insert = []
    
    # We need to filter out rows that already exist in DB
    # Since we are in a transaction, we can query safely.
    
    # Get raw connection for cursor
    raw_conn = conn.connection
    cur = raw_conn.cursor()

    # Pre-filter: Check which profiles already exist
    # Note: This assumes all rows for a profile are either present or absent together.
    
    existing_profiles = set()
    
    try:
        for _, row in unique_keys.iterrows():
            fid = str(row["float_id"])
            cyc = int(row["cycle"])
            prof = int(row["profile_number"])
            
            # Check if ANY row exists for this profile
            check_sql = "SELECT 1 FROM measurements WHERE float_id = %s AND cycle = %s AND profile_number = %s LIMIT 1"
            cur.execute(check_sql, (fid, cyc, prof))
            if cur.fetchone():
                existing_profiles.add((fid, cyc, prof))
                # print(f"⏩ Skipping duplicate measurements for Float {fid} Cycle {cyc} Profile {prof}")

    except Exception as e:
        print(f"⚠ Error checking for duplicates: {e}")
        # Fallback: try to insert everything (might fail if we had constraints, but here we don't)
    
    # Filter dataframe
    # We iterate and keep only those NOT in existing_profiles
    # A bit slow for huge DFs but safe.
    
    # Faster way: Filter DF using boolean mask
    # But for simplicity and safety with mixed types, let's just build the list.
    
    full_rows = df.to_dict(orient="records")
    final_rows = []
    
    for r in full_rows:
        key = (str(r["float_id"]), int(r["cycle"]), int(r["profile_number"]))
        if key not in existing_profiles:
            final_rows.append(r)
            
    if not final_rows:
        # print("✔ All rows were duplicates. Nothing to insert.")
        cur.close()
        return

    # ---------------------------------------------------------
    # INSERT REMAINING ROWS
    # ---------------------------------------------------------
    
    insert_sql = """
        INSERT INTO measurements (
            float_id, cycle, profile_number, juld,
            latitude, longitude, depth_m,
            sensor, value, qc, source_file
        )
        VALUES (
            %(float_id)s, %(cycle)s, %(profile_number)s, %(juld)s,
            %(latitude)s, %(longitude)s, %(depth_m)s,
            %(sensor)s, %(value)s, %(qc)s, %(source_file)s
        );
    """

    try:
        # page_size ko tune kar sakti ho:
        # - 500: safe & fast for remote DB
        # - 1000: zyada speed, thoda heavy
        execute_batch(cur, insert_sql, final_rows, page_size=500)
        
        cur.close()
        # DO NOT commit or close raw_conn here; let the outer transaction handle it.

    except Exception as e:
        print(f"❌ Error in bulk insert: {e}")
        raise

    # print(f"\n✔ Inserted {len(final_rows)} measurement rows (FAST batch mode)")
