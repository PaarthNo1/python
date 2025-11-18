"""
faiss_pipeline.py
FAISS-based vector pipeline for OceanIQ profiles.

Features:
- Read profile summaries from Postgres
- Build text summaries
- Compute embeddings with sentence-transformers (all-MiniLM-L6-v2)
- Store embeddings in FAISS index (inner-product on normalized vectors -> cosine)
- Store metadata in local SQLite (faiss_meta.db)
- Provide semantic_search() and geo_semantic_search()
- Save/load index and metadata for persistence
"""

import os
import math
import logging
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
from tqdm import tqdm

# embedding model
from sentence_transformers import SentenceTransformer

# postgres/sql
from sqlalchemy import create_engine, text

# faiss
try:
    import faiss
except Exception as e:
    raise ImportError("faiss import failed. Install faiss-cpu: pip install faiss-cpu") from e

# ---------------------------
# CONFIG
# ---------------------------





DATABASE_URL = os.getenv("DATABASE_URL","postgresql://postgres:ocean@localhost:5432/oceaniq_db")
FAISS_DIR = os.getenv("FAISS_DIR", "vector_store")             # folder to store index + metadata
FAISS_INDEX_PATH = os.path.join(FAISS_DIR, "faiss_index.bin")
META_DB_PATH = os.path.join(FAISS_DIR, "faiss_meta.db")        # sqlite file for metadata
EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 128

# ---------------------------
# logging
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("faiss_pipeline")

# ---------------------------
# Create directories
# ---------------------------
os.makedirs(FAISS_DIR, exist_ok=True)

# ---------------------------
# SQLAlchemy engine (Postgres)
# ---------------------------
engine = create_engine(DATABASE_URL, future=True)

# ---------------------------
# Load embedding model
# ---------------------------
logger.info("Loading embedding model: %s", EMBED_MODEL_NAME)
embed_model = SentenceTransformer(EMBED_MODEL_NAME)

# ---------------------------
# Helper: fetch profiles from Postgres (one row per float_id+cycle)
# ---------------------------
def fetch_profiles_from_db(limit: Optional[int] = None) -> pd.DataFrame:
    q = """
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
    if limit:
        q += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        df = pd.read_sql(q, conn)
    logger.info("Fetched %d profiles from Postgres", len(df))
    return df

# ---------------------------
# Build human-readable summary
# ---------------------------
def build_summary(row: pd.Series) -> str:
    float_id = str(row["float_id"])
    cycle = int(row["cycle"])
    prof = int(row["profile_number"])
    lat = float(row["lat"]) if pd.notna(row["lat"]) else None
    lon = float(row["lon"]) if pd.notna(row["lon"]) else None
    juld = row["juld"]
    n_points = int(row["n_points"]) if pd.notna(row["n_points"]) else 0
    mean_temp = float(row["mean_temp"]) if pd.notna(row["mean_temp"]) else None
    mean_sal = float(row["mean_sal"]) if pd.notna(row["mean_sal"]) else None
    min_d = float(row["min_depth"]) if pd.notna(row["min_depth"]) else None
    max_d = float(row["max_depth"]) if pd.notna(row["max_depth"]) else None

    parts = [f"Float {float_id}, cycle {cycle} (profile {prof})."]
    if juld is not None:
        parts.append(f"Date: {pd.to_datetime(juld).strftime('%Y-%m-%d')}.")
    if lat is not None and lon is not None:
        parts.append(f"Location: {lat:.3f}N, {lon:.3f}E.")
    if n_points:
        parts.append(f"{n_points} depth levels from {min_d:.1f}m to {max_d:.1f}m.")
    if mean_temp is not None:
        parts.append(f"Mean temperature: {mean_temp:.2f} °C.")
    if mean_sal is not None:
        parts.append(f"Mean salinity: {mean_sal:.2f} PSU.")
    return " ".join(parts)

# ---------------------------
# Compute embeddings (batched)
# ---------------------------
def compute_embeddings(texts: List[str]) -> np.ndarray:
    embs = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        emb = embed_model.encode(batch, show_progress_bar=False, convert_to_numpy=True)
        embs.append(emb)
    embs = np.vstack(embs)
    logger.info("Computed embeddings shape: %s", embs.shape)
    return embs

# ---------------------------
# FAISS helpers
# ---------------------------
def build_faiss_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    """
    Build FAISS index for cosine similarity:
    - normalize vectors to unit length
    - use IndexFlatIP with inner-product (which equals cosine when vectors normalized)
    """
    # normalize
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    embeddings = embeddings / norms
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings.astype(np.float32))
    return index

def save_index(index: faiss.IndexFlatIP, path: str):
    faiss.write_index(index, path)
    logger.info("FAISS index saved to %s", path)

def load_index(path: str) -> Optional[faiss.IndexFlatIP]:
    if not os.path.exists(path):
        return None
    idx = faiss.read_index(path)
    logger.info("FAISS index loaded from %s", path)
    return idx

# ---------------------------
# Metadata storage (SQLite)
# ---------------------------
def save_metadata(df_meta: pd.DataFrame, sqlite_path: str = META_DB_PATH):
    # Store DataFrame to a local sqlite file (overwrites for simplicity)
    from sqlalchemy import create_engine
    meta_engine = create_engine(f"sqlite:///{sqlite_path}")
    df_meta.to_sql("profiles_meta", meta_engine, index=False, if_exists="replace")
    logger.info("Saved metadata to %s", sqlite_path)

def load_metadata(sqlite_path: str = META_DB_PATH) -> pd.DataFrame:
    if not os.path.exists(sqlite_path):
        return pd.DataFrame()
    from sqlalchemy import create_engine
    meta_engine = create_engine(f"sqlite:///{sqlite_path}")
    df = pd.read_sql("SELECT * FROM profiles_meta", meta_engine)
    return df

# ---------------------------
# Build pipeline: fetch -> summaries -> embeddings -> save
# ---------------------------
def build_and_persist(limit: Optional[int] = None):
    df = fetch_profiles_from_db(limit=limit)
    if df.empty:
        logger.warning("No profiles found")
        return

    # create unique id and summary
    df["uid"] = df.apply(lambda r: f"{r['float_id']}_{int(r['cycle'])}", axis=1)
    df["summary"] = df.apply(build_summary, axis=1)

    texts = df["summary"].tolist()
    ids = df["uid"].tolist()

    # compute embeddings
    embeddings = compute_embeddings(texts)

    # build index
    index = build_faiss_index(embeddings)

    # save index
    save_index(index, FAISS_INDEX_PATH)

    # metadata to sqlite (store id, metadata fields and summary)
    meta_df = df[["uid", "float_id", "cycle", "profile_number", "lat", "lon", "juld", "n_points", "mean_temp", "mean_sal", "min_depth", "max_depth", "summary"]].copy()
    save_metadata(meta_df)

    logger.info("Build & persist complete. %d items indexed.", len(ids))

# ---------------------------
# Query helpers
# ---------------------------
def semantic_search(query: str, top_k: int = 5) -> List[Dict[str, Any]]:
    idx = load_index(FAISS_INDEX_PATH)
    if idx is None:
        logger.error("Index not found; run build_and_persist() first")
        return []

    q_emb = embed_model.encode([query], convert_to_numpy=True)[0]
    # normalize
    q_emb = q_emb / np.linalg.norm(q_emb)

    D, I = idx.search(np.expand_dims(q_emb.astype(np.float32), axis=0), top_k)
    D = D[0].tolist()
    I = I[0].tolist()

    # load metadata
    meta = load_metadata()
    if meta.empty:
        return []

    results = []
    for score, i in zip(D, I):
        if i < 0:
            continue
        row = meta.iloc[i]
        results.append({
            "uid": row["uid"],
            "metadata": {
                "float_id": row["float_id"],
                "cycle": int(row["cycle"]),
                "profile_number": int(row["profile_number"]),
                "lat": row["lat"],
                "lon": row["lon"],
                "juld": row["juld"],
            },
            "summary": row["summary"],
            "score": float(score)   # cosine-like
        })
    return results

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))

def geo_semantic_search(lat: float, lon: float, radius_km: float = 200.0, text_query: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
    # load metadata
    meta = load_metadata()
    if meta.empty:
        return []

    # compute distances and filter by radius
    meta["dist_km"] = meta.apply(lambda r: haversine_km(lat, lon, r["lat"], r["lon"]) if (pd.notna(r["lat"]) and pd.notna(r["lon"])) else 1e9, axis=1)
    candidates = meta[meta["dist_km"] <= radius_km].reset_index(drop=True)
    if candidates.empty:
        return []

    # if text_query provided, compute embedding and rank by cosine with candidate embeddings
    if text_query:
        q_emb = embed_model.encode([text_query], convert_to_numpy=True)[0]
        q_emb = q_emb / np.linalg.norm(q_emb)
        # load FAISS index to get vector for candidate indices
        idx = load_index(FAISS_INDEX_PATH)
        if idx is None:
            return []

        # we stored items in order — index positions correspond to meta row numbers
        scores = []
        for i, row in candidates.iterrows():
            # fetch vector by reconstruct? FAISS IndexFlat doesn't store reconstruct for individual vectors easily;
            # simpler: compute embeddings for candidate summaries directly (small candidate set)
            emb = embed_model.encode([row["summary"]], convert_to_numpy=True)[0]
            emb = emb / np.linalg.norm(emb)
            score = float(np.dot(q_emb, emb))
            scores.append(score)
        candidates["score"] = scores
        candidates = candidates.sort_values("score", ascending=False).head(top_k)
        out = []
        for _, r in candidates.iterrows():
            out.append({
                "uid": r["uid"],
                "metadata": {
                    "float_id": r["float_id"],
                    "cycle": int(r["cycle"]),
                    "profile_number": int(r["profile_number"]),
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "juld": r["juld"]
                },
                "summary": r["summary"],
                "score": float(r["score"]),
                "dist_km": float(r["dist_km"])
            })
        return out

    # else no text_query → return nearest by distance
    candidates = candidates.sort_values("dist_km").head(top_k)
    out = []
    for _, r in candidates.iterrows():
        out.append({
            "uid": r["uid"],
            "metadata": {
                "float_id": r["float_id"],
                "cycle": int(r["cycle"]),
                "profile_number": int(r["profile_number"]),
                "lat": r["lat"],
                "lon": r["lon"],
                "juld": r["juld"]
            },
            "summary": r["summary"],
            "dist_km": float(r["dist_km"])
        })
    return out

# ---------------------------
# CLI
# ---------------------------
def main(limit: Optional[int] = None):
    logger.info("Building FAISS pipeline")
    build_and_persist(limit=limit)
    logger.info("Quick test: semantic_search('warm surface', top_k=3)")
    print(semantic_search("warm surface", top_k=3))
    logger.info("Quick geo test: geo_semantic_search(-35.63, 37.18, 500, 'warm')")
    print(geo_semantic_search(-35.63, 37.18, 500, text_query="warm", top_k=3))

if __name__ == "__main__":
    main(limit=None)
