# # services/db_service.py
# import os
# from typing import Optional, Dict, Any

# import pandas as pd
# from sqlalchemy import create_engine, text
# from sqlalchemy.engine import Engine

# # Optional: auto-load .env if python-dotenv is installed
# try:
#     from dotenv import load_dotenv, find_dotenv
#     load_dotenv(find_dotenv(), override=False)
# except Exception:
#     pass

# _engine: Optional[Engine] = None


# def get_engine() -> Engine:
#     """Singleton engine; fails fast if DATABASE_URL is missing."""
#     global _engine
#     if _engine is None:
#         db_url = os.getenv("DATABASE_URL")
#         if not db_url:
#             raise RuntimeError(
#                 "DATABASE_URL is not set. Put it in your environment or a .env file."
#             )
#         _engine = create_engine(db_url, future=True)
#     return _engine


# def get_profile_metadata(float_id: str, cycle: int) -> Optional[Dict[str, Any]]:
#     """
#     Return a single profile's core metadata.
#     Prefer `profiles` (lat/lon/juld there), fallback to `floats` (latitude/longitude).
#     """
#     q = text("""
#         SELECT
#             COALESCE(p.float_id, f.float_id) AS float_id,
#             COALESCE(p.cycle,    f.cycle)    AS cycle,
#             COALESCE(p.profile_number, f.profile_number, 0) AS profile_number,
#             COALESCE(p.lat, f.latitude)  AS lat,
#             COALESCE(p.lon, f.longitude) AS lon,
#             COALESCE(p.juld, f.juld)     AS juld
#         FROM profiles p
#         FULL OUTER JOIN floats f
#           ON f.float_id = p.float_id AND f.cycle = p.cycle
#         WHERE COALESCE(p.float_id, f.float_id) = :fid
#           AND COALESCE(p.cycle,    f.cycle)    = :cyc
#         LIMIT 1;
#     """)
#     with get_engine().connect() as conn:
#         row = conn.execute(q, {"fid": float_id, "cyc": int(cycle)}).mappings().first()
#     return dict(row) if row else None


# def get_profile_measurements(float_id: str, cycle: int, profile_number: int) -> pd.DataFrame:
#     """
#     Return a tidy DataFrame: columns = ['depth','temp','sal'] sorted by depth.
#     1) Try `profiles` by unnesting (pres,temp,psal) arrays.
#     2) If absent, pivot from row-wise `measurements` (sensor,value).
#     """
#     q_profiles = text("""
#         SELECT
#             u.pres AS depth,
#             u.temp AS temp,
#             u.psal AS sal
#         FROM profiles p
#         LEFT JOIN LATERAL unnest(p.pres, p.temp, p.psal) AS u(pres, temp, psal) ON TRUE
#         WHERE p.float_id = :fid AND p.cycle = :cyc
#         ORDER BY u.pres;
#     """)
#     with get_engine().connect() as conn:
#         df = pd.read_sql(q_profiles, conn, params={"fid": float_id, "cyc": int(cycle)})

#     if not df.empty:
#         df = df.rename(columns={"pres": "depth"})
#         return df.where(pd.notnull(df), None)[["depth", "temp", "sal"]]

#     q_meas = text("""
#         SELECT
#             depth_m AS depth,
#             MAX(CASE WHEN sensor = 'temp' THEN value END) AS temp,
#             MAX(CASE WHEN sensor = 'psal' THEN value END) AS sal
#         FROM measurements
#         WHERE float_id = :fid AND cycle = :cyc
#         GROUP BY depth_m
#         ORDER BY depth_m;
#     """)
#     with get_engine().connect() as conn:
#         df2 = pd.read_sql(q_meas, conn, params={"fid": float_id, "cyc": int(cycle)})

#     if df2.empty:
#         return pd.DataFrame(columns=["depth", "temp", "sal"])

#     return df2.where(pd.notnull(df2), None)[["depth", "temp", "sal"]]

