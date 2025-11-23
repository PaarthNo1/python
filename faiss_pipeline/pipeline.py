# pipeline.py
import logging
import pandas as pd
from .db import fetch_profiles
from .summaries import build_summary
from .embeddings import compute_embeddings
from .index_store import build_index, save_index
from .meta_store import save_metadata
from .config import FAISS_INDEX_PATH, META_DB_PATH

logger = logging.getLogger("faiss.pipeline")

def build_and_persist(limit: int = None):
    df = fetch_profiles(limit=limit)
    if df.empty:
        logger.warning("No profiles to index.")
        return

    df["uid"] = df.apply(lambda r: f"{r['float_id']}_{int(r['cycle'])}", axis=1)
    df["summary"] = df.apply(build_summary, axis=1)

    texts = df["summary"].tolist()
    ids = df["uid"].tolist()

    embeddings = compute_embeddings(texts)
    index = build_index(embeddings)
    save_index(index, str(FAISS_INDEX_PATH))

    # choose metadata columns to persist; include _pos to map index order
    meta_df = df[["uid", "float_id", "cycle", "profile_number", "lat", "lon", "juld",
                  "n_points", "mean_temp", "mean_sal", "min_depth", "max_depth", "summary"]].copy().reset_index(drop=True)
    save_metadata(meta_df, str(META_DB_PATH))
    logger.info("Indexed %d items.", len(ids))

def main(limit: int = None):
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting build_and_persist")
    build_and_persist(limit=limit)
    # quick smoke tests
    from .search import semantic_search, geo_semantic_search
    print(semantic_search("warm surface", top_k=3))
    print(geo_semantic_search(-35.63, 37.18, 500, text_query="warm", top_k=3))

if __name__ == "__main__":
    main(limit=None)
