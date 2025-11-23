# db.py
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from .config import DATABASE_URL
import logging

logger = logging.getLogger("faiss.db")
engine = create_engine(DATABASE_URL, future=True)

SUMMARY_SQL = """
WITH summary AS (
  SELECT
    f.float_id,
    f.cycle,
    f.profile_number,
    f.lat,
    f.lon,
    f.juld,
    COUNT(m.*) as n_points,
    AVG(m.temp) FILTER (WHERE m.temp IS NOT NULL) as mean_temp,
    AVG(m.sal) FILTER (WHERE m.sal IS NOT NULL) as mean_sal,
    MIN(m.depth) as min_depth,
    MAX(m.depth) as max_depth
  FROM floats f
  LEFT JOIN measurements m
    ON f.float_id = m.float_id AND f.cycle = m.cycle AND f.profile_number = m.profile_number
  GROUP BY f.float_id, f.cycle, f.profile_number, f.lat, f.lon, f.juld
  ORDER BY f.float_id, f.cycle
)
SELECT * FROM summary
"""

def fetch_profiles(limit: Optional[int] = None) -> pd.DataFrame:
    q = SUMMARY_SQL
    if limit:
        q += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    logger.info("Fetched %d profiles", len(df))
    return df
