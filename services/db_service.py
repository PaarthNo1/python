# services/db_service.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:simran%4004@localhost:5432/oceaniq_db")
engine = create_engine(DATABASE_URL, future=True)


def get_profile_metadata(float_id: str, cycle: int):
    """
    Returns metadata for one profile.
    """
    q = text("""
        SELECT float_id, cycle, profile_number, lat, lon, juld
        FROM floats
        WHERE float_id = :fid AND cycle = :cy
        LIMIT 1
    """)

    with engine.connect() as conn:
        res = conn.execute(q, {"fid": float_id, "cy": int(cycle)}).mappings().fetchone()
        return dict(res) if res else None


def get_profile_measurements(float_id: str, cycle: int, profile_number: int):
    """
    Returns measurement DataFrame: depth, temp, sal sorted by depth.
    Automatically cleans NaN and invalid JSON values.
    """
    q = text("""
        SELECT depth, temp, sal
        FROM measurements
        WHERE float_id = :fid AND cycle = :cy AND profile_number = :p
        ORDER BY depth ASC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={
            "fid": float_id,
            "cy": int(cycle),
            "p": int(profile_number)
        })

    # FIX: Make JSON-safe
    df = df.replace({float("nan"): None})

    return df
