# index_store.py
import numpy as np
from typing import Optional
import faiss
from .config import FAISS_INDEX_PATH
import logging

logger = logging.getLogger("faiss.index")

def _normalize_embeddings(emb: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(emb, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return emb / norms

def build_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    emb = _normalize_embeddings(embeddings).astype(np.float32)
    dim = emb.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(emb)
    logger.info("Built FAISS index with %d vectors (dim=%d)", index.ntotal, dim)
    return index

def save_index(index: faiss.IndexFlatIP, path: str = None):
    p = path or str(FAISS_INDEX_PATH)
    faiss.write_index(index, p)
    logger.info("Saved FAISS index to %s", p)

def load_index(path: str = None) -> Optional[faiss.IndexFlatIP]:
    p = path or str(FAISS_INDEX_PATH)
    import os
    if not os.path.exists(p):
        logger.warning("FAISS index not found at %s", p)
        return None
    idx = faiss.read_index(p)
    logger.info("Loaded FAISS index from %s (ntotal=%d)", p, idx.ntotal)
    return idx
