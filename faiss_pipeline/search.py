import faiss
import numpy as np
from typing import List, Dict, Any, Optional
import pandas as pd
from .embeddings import compute_embeddings, get_model
from .index_store import load_index
from .meta_store import load_metadata, haversine_km
from .reranker import rerank
import logging

logger = logging.getLogger("faiss.search")

def _gather_by_positions(meta: pd.DataFrame, positions: List[int]) -> List[Dict[str, Any]]:
    out = []
    for pos in positions:
        if pos < 0:
            continue
        row = meta[meta["_pos"] == pos]
        if row.empty and 0 <= pos < len(meta):
            row = meta.iloc[[pos]]
        if row.empty:
            continue
        r = row.iloc[0]
        out.append({
            "uid": r["uid"],
            "metadata": {
                "float_id": r["float_id"],
                "cycle": int(r["cycle"]),
                "profile_number": int(r["profile_number"]),
                "lat": r["lat"],
                "lon": r["lon"],
                "juld": r["juld"],
            },
            "summary": r["summary"],
        })
    return out

def semantic_search(query: str, top_k: int = 5) -> List[Dict[str, Any]]:
    idx = load_index()
    if idx is None:
        logger.error("Index missing; build index first.")
        return []

    # wider retrieval, then rerank
    initial_k = max(top_k * 5, 20)

    # encode + normalize
    q = compute_embeddings([query])[0]
    D, I = idx.search(np.expand_dims(q, axis=0), initial_k)
    positions = [int(x) for x in I[0].tolist() if x >= 0]

    meta = load_metadata()
    if meta.empty:
        logger.warning("No metadata present.")
        return []

    cands = _gather_by_positions(meta, positions)
    if not cands:
        return []

    # cross-encoder rerank on summaries
    ranked = rerank(query, [c["summary"] for c in cands])
    final = []
    for i, ce_score in ranked[:top_k]:
        item = dict(cands[i])
        item["score"] = ce_score  # cross-encoder score
        final.append(item)
    return final

def geo_semantic_search(lat: float, lon: float, radius_km: float = 200.0,
                        text_query: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
    meta = load_metadata()
    if meta.empty:
        return []

    meta["dist_km"] = meta.apply(
        lambda r: haversine_km(lat, lon, r["lat"], r["lon"]) if not (pd.isna(r["lat"]) or pd.isna(r["lon"])) else 1e9,
        axis=1
    )
    nearby = meta[meta["dist_km"] <= radius_km].reset_index(drop=True)
    if nearby.empty:
        return []

    # If no text, just nearest by distance
    if not text_query:
        nearby = nearby.sort_values("dist_km").head(top_k)
        out = []
        for _, r in nearby.iterrows():
            out.append({
                "uid": r["uid"],
                "metadata": { "float_id": r["float_id"], "cycle": int(r["cycle"]), "profile_number": int(r["profile_number"]), "lat": r["lat"], "lon": r["lon"], "juld": r["juld"] },
                "summary": r["summary"],
                "dist_km": float(r["dist_km"])
            })
        return out

    # With text: cross-encoder ranks the nearby set directly
    ranked = rerank(text_query, nearby["summary"].tolist())
    keep = ranked[:top_k]
    out = []
    for idx_in_df, score in keep:
        r = nearby.iloc[int(idx_in_df)]
        out.append({
            "uid": r["uid"],
            "metadata": { "float_id": r["float_id"], "cycle": int(r["cycle"]), "profile_number": int(r["profile_number"]), "lat": r["lat"], "lon": r["lon"], "juld": r["juld"] },
            "summary": r["summary"],
            "score": float(score),
            "dist_km": float(r["dist_km"])
        })
    return out
