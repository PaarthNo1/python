import os, json
import faiss
from .config import SCHEMA_INDEX_PATH, SCHEMA_INDEX_META
from .embeddings import embedding_dimension

def _save_meta(dim: int):
    with open(SCHEMA_INDEX_META, "w") as f:
        json.dump({"dim": dim}, f)

def _load_meta():
    if os.path.exists(SCHEMA_INDEX_META):
        return json.load(open(SCHEMA_INDEX_META))
    return None

def create_empty_index() -> faiss.Index:
    dim = embedding_dimension()
    idx = faiss.IndexFlatIP(dim)  # cosine (we normalize)
    _save_meta(dim)
    return idx

def save_index(index: faiss.Index):
    faiss.write_index(index, str(SCHEMA_INDEX_PATH))

def load_index() -> faiss.Index:
    if not os.path.exists(SCHEMA_INDEX_PATH):
        return create_empty_index()
    meta = _load_meta()
    idx = faiss.read_index(str(SCHEMA_INDEX_PATH))
    if meta and int(meta.get("dim", idx.d)) != idx.d:
        raise RuntimeError("Schema FAISS dimension mismatch; delete and rebuild.")
    return idx
