# meta_store.py
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine
from .config import META_DB_PATH
import logging
import math

logger = logging.getLogger("faiss.meta")

def save_metadata(df: pd.DataFrame, sqlite_path: Optional[str] = None):
    p = sqlite_path or str(META_DB_PATH)
    engine = create_engine(f"sqlite:///{p}")
    # ensure ordering index column to preserve index positions
    df = df.reset_index(drop=True).copy()
    df["_pos"] = df.index
    df.to_sql("profiles_meta", engine, index=False, if_exists="replace")
    logger.info("Saved metadata (%d rows) to %s", len(df), p)

def load_metadata(sqlite_path: Optional[str] = None) -> pd.DataFrame:
    p = sqlite_path or str(META_DB_PATH)
    import os
    if not os.path.exists(p):
        logger.warning("Metadata DB not found at %s", p)
        return pd.DataFrame()
    engine = create_engine(f"sqlite:///{p}")
    df = pd.read_sql("SELECT * FROM profiles_meta", engine)
    # ensure _pos exists
    if "_pos" not in df.columns:
        df["_pos"] = df.index
    return df

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))
