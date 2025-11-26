from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from .config import DATABASE_URL
import logging

logger = logging.getLogger("faiss.db")
engine = create_engine(DATABASE_URL, future=True)

# Summaries from the new `profiles` table with array columns.
# We align pres/temp/psal with a LATERAL multi-unnest and compute stats.
SUMMARY_SQL = """
WITH m AS (
  SELECT
    p.float_id,
    p.cycle,
    COALESCE(p.profile_number, 0) AS profile_number,
    p.lat,
    p.lon,
    p.juld,
    u.pres   AS depth,  -- pressure used as depth
    u.temp   AS temp,
    u.psal   AS sal
  FROM profiles p
  LEFT JOIN LATERAL unnest(p.pres, p.temp, p.psal) AS u(pres, temp, psal) ON TRUE
)
SELECT
  float_id,
  cycle,
  profile_number,
  lat,
  lon,
  juld,
  COUNT(depth)                         AS n_points,
  AVG(temp) FILTER (WHERE temp IS NOT NULL) AS mean_temp,
  AVG(sal)  FILTER (WHERE sal  IS NOT NULL) AS mean_sal,
  MIN(depth) AS min_depth,
  MAX(depth) AS max_depth
FROM m
GROUP BY float_id, cycle, profile_number, lat, lon, juld
ORDER BY float_id, cycle, profile_number;
"""

def fetch_profiles(limit: Optional[int] = None) -> pd.DataFrame:
    q = SUMMARY_SQL + (f" LIMIT {int(limit)}" if limit else "")
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    logger.info("Fetched %d profile summaries", len(df))
    return df
