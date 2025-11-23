# services/faiss_service.py
import json
import numpy as np
import logging

logger = logging.getLogger("services.faiss_service")

# Try to import from the refactored package layout first
_semantic_search = None
_geo_search = None

# 1) Preferred: import from the search module directly
try:
    from faiss_pipeline.search import semantic_search as _semantic_search, geo_semantic_search as _geo_search
    logger.debug("Imported semantic_search, geo_semantic_search from faiss_pipeline.search")
except Exception:
    # 2) Fallback: package-level exports (if __init__.py exposes them)
    try:
        from faiss_pipeline import semantic_search as _semantic_search, geo_semantic_search as _geo_search
        logger.debug("Imported semantic_search, geo_semantic_search from faiss_pipeline package")
    except Exception:
        # 3) Last-resort: try older single-file API names
        try:
            from faiss_pipeline import semantic_search as _semantic_search
            try:
                from faiss_pipeline import geo_semantic_search as _geo_search
            except Exception:
                _geo_search = None
            logger.debug("Imported (partial) functions from faiss_pipeline package")
        except Exception as e:
            logger.error("Failed to import faiss_pipeline search functions: %s", e)
            _semantic_search = None
            _geo_search = None


def semantic_search(query: str, top_k: int = 5):
    """Wrapper used by the rest of the app. Always returns a list (empty on error)."""
    if _semantic_search is None:
        logger.warning("semantic_search not available (faiss index not built or import failed).")
        return []
    try:
        res = _semantic_search(query, top_k=top_k)
        # ensure it is a list
        if isinstance(res, dict) and res.get("error"):
            logger.warning("semantic_search returned error dict: %s", res.get("error"))
            return []
        if isinstance(res, list):
            return json.loads(json.dumps(res, default=_safe_json))
        # try to coerce iterables
        try:
            coerced = list(json.loads(json.dumps(res, default=_safe_json)))
            return coerced
        except Exception:
            logger.warning("semantic_search returned unexpected type; coercion failed.")
            return []
    except Exception as e:
        logger.exception("semantic_search failed: %s", e)
        return []


def geo_search(lat, lon, radius_km=200, text_query=None, top_k=5):
    """Wrapper that returns a list (empty on error)."""
    if _geo_search is None:
        logger.warning("geo_search not available in faiss_pipeline.")
        return []
    try:
        res = _geo_search(lat, lon, radius_km=radius_km, text_query=text_query, top_k=top_k)
        if isinstance(res, dict) and res.get("error"):
            logger.warning("geo_search returned error dict: %s", res.get("error"))
            return []
        if isinstance(res, list):
            return json.loads(json.dumps(res, default=_safe_json))
        try:
            coerced = list(json.loads(json.dumps(res, default=_safe_json)))
            return coerced
        except Exception:
            logger.warning("geo_search returned unexpected type; coercion failed.")
            return []
    except Exception as e:
        logger.exception("geo_search failed")
        return []


def _safe_json(obj):
    """Fixes numpy values and datetimes for JSON serialization."""
    # numpy numbers
    if isinstance(obj, (np.float32, np.float64, np.floating)):
        return float(obj)
    if isinstance(obj, (np.int32, np.int64, np.integer)):
        return int(obj)
    # pandas Timestamps, datetimes, etc.
    try:
        import pandas as pd
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except Exception:
        pass
    # fallback to str
    return str(obj)
