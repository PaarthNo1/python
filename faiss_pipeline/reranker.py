import os
from typing import List, Tuple
from sentence_transformers import CrossEncoder
import logging

logger = logging.getLogger("faiss.rerank")

_RERANK = None

# Force BGE reranker
RERANK_MODEL = "BAAI/bge-reranker-base"

def _get_reranker() -> CrossEncoder:
    global _RERANK
    if _RERANK is None:
        logger.info("Loading reranker: %s", RERANK_MODEL)
        _RERANK = CrossEncoder(RERANK_MODEL)
    return _RERANK

def rerank(query: str, texts: List[str]) -> List[Tuple[int, float]]:
    """
    Returns list of (index_in_texts, score), sorted descending.
    Higher score means more relevant.
    """
    ce = _get_reranker()
    pairs = [(query, t) for t in texts]
    scores = ce.predict(pairs)
    ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
    return [(i, float(s)) for i, s in ranked]
