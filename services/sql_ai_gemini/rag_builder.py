# rag_builder.py
import logging
from typing import List
import services.faiss_service as faiss_service  # keep using your services dir
logger = logging.getLogger("nl_sql_audit.rag")

def build_rag_context(question: str, top_k: int = 5) -> str:
    try:
        results = faiss_service.semantic_search(question, top_k=top_k)
    except Exception as e:
        logger.warning("RAG retrieval failed: %s", str(e))
        results = []

    if not results:
        return ""

    parts: List[str] = []
    for r in results:
        md = r.get("metadata", {})
        uid = r.get("uid", "")
        summary = (r.get("summary") or "")[:1000]
        parts.append(
            f"UID: {uid} | float_id: {md.get('float_id')} | cycle: {md.get('cycle')} | "
            f"lat: {md.get('lat')} | lon: {md.get('lon')} | juld: {md.get('juld')}\nSummary: {summary}"
        )
    return "\n\n".join(parts)
