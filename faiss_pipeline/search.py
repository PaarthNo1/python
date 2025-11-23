# search.py
from typing import List, Dict, Any, Optional
import numpy as np
from .index_store import load_index
from .meta_store import load_metadata, haversine_km
from .embeddings import get_model
from .config import FAISS_INDEX_PATH
import logging

logger = logging.getLogger("faiss.search")

def semantic_search(query: str, top_k: int = 5) -> List[Dict[str, Any]]:
    idx = load_index()
    if idx is None:
        logger.error("Index missing; build index first.")
        return []

    model = get_model()
    q_emb = model.encode([query], convert_to_numpy=True)[0]
    q_emb = q_emb / (np.linalg.norm(q_emb) or 1.0)
    D, I = idx.search(np.expand_dims(q_emb.astype(np.float32), axis=0), top_k)
    distances = D[0].tolist()
    indices = I[0].tolist()

    meta = load_metadata()
    if meta.empty:
        logger.warning("No metadata present.")
        return []

    results = []
    for score, pos in zip(distances, indices):
        if pos < 0:
            continue
        # metadata stored with _pos; attempt direct row lookup by _pos to be robust
        row = meta[meta["_pos"] == pos]
        if row.empty:
            # fallback: use iloc if within bounds
            if 0 <= pos < len(meta):
                row = meta.iloc[[pos]]
            else:
                continue
        row = row.iloc[0]
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
            "score": float(score)
        })
    return results

def geo_semantic_search(lat: float, lon: float, radius_km: float = 200.0,
                        text_query: Optional[str] = None, top_k: int = 10) -> List[Dict[str, Any]]:
    meta = load_metadata()
    if meta.empty:
        return []

    meta["dist_km"] = meta.apply(lambda r: haversine_km(lat, lon, r["lat"], r["lon"]) if not (pd.isna(r["lat"]) or pd.isna(r["lon"])) else 1e9, axis=1)
    candidates = meta[meta["dist_km"] <= radius_km].reset_index(drop=True)
    if candidates.empty:
        return []

    if text_query:
        # compute candidate embeddings on the fly (small set) and rank
        model = get_model()
        q_emb = model.encode([text_query], convert_to_numpy=True)[0]
        q_emb = q_emb / (np.linalg.norm(q_emb) or 1.0)
        scores = []
        for _, r in candidates.iterrows():
            emb = model.encode([r["summary"]], convert_to_numpy=True)[0]
            emb = emb / (np.linalg.norm(emb) or 1.0)
            scores.append(float(np.dot(q_emb, emb)))
        candidates["score"] = scores
        candidates = candidates.sort_values("score", ascending=False).head(top_k)
        out = []
        for _, r in candidates.iterrows():
            out.append({
                "uid": r["uid"],
                "metadata": { "float_id": r["float_id"], "cycle": int(r["cycle"]), "profile_number": int(r["profile_number"]), "lat": r["lat"], "lon": r["lon"], "juld": r["juld"] },
                "summary": r["summary"],
                "score": float(r["score"]),
                "dist_km": float(r["dist_km"])
            })
        return out

    # no text: return nearest by distance
    candidates = candidates.sort_values("dist_km").head(top_k)
    out = []
    for _, r in candidates.iterrows():
        out.append({
            "uid": r["uid"],
            "metadata": { "float_id": r["float_id"], "cycle": int(r["cycle"]), "profile_number": int(r["profile_number"]), "lat": r["lat"], "lon": r["lon"], "juld": r["juld"] },
            "summary": r["summary"],
            "dist_km": float(r["dist_km"])
        })
    return out
