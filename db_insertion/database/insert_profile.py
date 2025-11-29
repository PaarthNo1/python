
# insert_profile.py
from sqlalchemy import text
import numpy as np


def _clean_list(arr):
    """
    Convert numpy arrays or None to normal Python lists.
    Ensures PostgreSQL receives proper native arrays instead of strings.
    """
    if arr is None:
        return None
    if isinstance(arr, np.ndarray):
        return arr.tolist()
    return list(arr)  # safe for lists/tuples


def insert_profile(conn, data):
    """
    Insert or update a single profile row.
    Uses PostgreSQL's native array binding instead of manual string-building.
    MUCH safer and cleaner.
    """

    # Normalize arrays for PostgreSQL
    data["pres"] = _clean_list(data.get("pres"))
    data["temp"] = _clean_list(data.get("temp"))
    data["psal"] = _clean_list(data.get("psal"))
    data["temp_qc"] = _clean_list(data.get("temp_qc"))
    data["psal_qc"] = _clean_list(data.get("psal_qc"))

    # Round timestamp to microseconds (Postgres does not allow nanoseconds)
    if data.get("juld") is not None:
        try:
            data["juld"] = data["juld"].round("us")
        except Exception:
            pass

    # Map latitude/longitude to lat/lon for SQL binding
    if "lat" not in data and "latitude" in data:
        data["lat"] = data["latitude"]
    if "lon" not in data and "longitude" in data:
        data["lon"] = data["longitude"]

    sql = text("""
        INSERT INTO profiles (
            float_id, cycle, profile_number, juld, lat, lon,
            pres, temp, psal, temp_qc, psal_qc, source_file,
            geom
        ) VALUES (
            :float_id, :cycle, :profile_number, :juld, :lat, :lon,
            :pres, :temp, :psal, :temp_qc, :psal_qc, :source_file,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
        )
        ON CONFLICT (float_id, cycle)
        DO UPDATE SET
            juld = EXCLUDED.juld,
            lat  = EXCLUDED.lat,
            lon  = EXCLUDED.lon,
            pres = EXCLUDED.pres,
            temp = EXCLUDED.temp,
            psal = EXCLUDED.psal,
            temp_qc = EXCLUDED.temp_qc,
            psal_qc = EXCLUDED.psal_qc,
            source_file = EXCLUDED.source_file,
            geom = EXCLUDED.geom;
    """)

    # Single commit inside auto_loader transaction → very fast
    conn.execute(sql, data)

    # print("✔ Profile row inserted/updated (FAST + SAFE)")
