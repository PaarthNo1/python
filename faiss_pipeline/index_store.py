import numpy as np
from typing import Optional
import faiss
import json, os
from .config import FAISS_INDEX_PATH
from .embeddings import embedding_dimension
import logging

logger = logging.getLogger("faiss.index")
META_PATH = os.path.join(os.path.dirname(str(FAISS_INDEX_PATH)), "faiss_index_meta.json")

def _save_meta(dim: int):
    with open(META_PATH, "w") as f:
        json.dump({"dim": dim}, f)

def _load_meta() -> Optional[int]:
    if os.path.exists(META_PATH):
        with open(META_PATH) as f:
            return json.load(f).get("dim")
    return None

def build_index(embeddings: np.ndarray) -> faiss.IndexFlatIP:
    dim = embeddings.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(embeddings.astype(np.float32))
    logger.info("Built FAISS index with %d vectors (dim=%d)", index.ntotal, dim)
    _save_meta(dim)
    return index

def save_index(index: faiss.IndexFlatIP, path: str = None):
    p = path or str(FAISS_INDEX_PATH)
    faiss.write_index(index, p)
    logger.info("Saved FAISS index to %s", p)

def load_index(path: str = None) -> Optional[faiss.IndexFlatIP]:
    p = path or str(FAISS_INDEX_PATH)
    if not os.path.exists(p):
        logger.warning("FAISS index not found at %s", p)
        return None
    idx = faiss.read_index(p)
    meta_dim = _load_meta()
    if meta_dim is not None and idx.d != meta_dim:
        raise RuntimeError(f"Index dim {idx.d} != meta dim {meta_dim}. Delete and rebuild.")
    logger.info("Loaded FAISS index from %s (ntotal=%d, dim=%d)", p, idx.ntotal, idx.d)
    return idx
