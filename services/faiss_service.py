# services/faiss_service.py
import json
import numpy as np

try:
    from faiss_pipeline import (
        semantic_search as _semantic_search,
        geo_semantic_search as _geo_search
    )
except ImportError:
    # fallback if function names differ
    from faiss_pipeline import semantic_search as _semantic_search
    try:
        from faiss_pipeline import geo_semantic_search as _geo_search
    except ImportError:
        _geo_search = None


def semantic_search(query: str, top_k: int = 5):
    try:
        res = _semantic_search(query, top_k=top_k)
        return json.loads(json.dumps(res, default=_safe_json))
    except Exception as e:
        return {"error": f"semantic_search failed: {str(e)}"}


def geo_search(lat, lon, radius_km=200, text_query=None, top_k=5):
    if _geo_search is None:
        return {"error": "geo_search not available in faiss_pipeline."}

    try:
        res = _geo_search(lat, lon, radius_km=radius_km, text_query=text_query, top_k=top_k)
        return json.loads(json.dumps(res, default=_safe_json))
    except Exception as e:
        return {"error": f"geo_search failed: {str(e)}"}


def _safe_json(obj):
    """Fixes numpy values and datetimes."""
    if isinstance(obj, (np.float32, np.float64)):
        return float(obj)
    if isinstance(obj, (np.int32, np.int64)):
        return int(obj)
    return str(obj)
