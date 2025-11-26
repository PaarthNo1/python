import numpy as np
from typing import List, Dict, Any
from .index_store import load_index
from .embeddings import embed_texts
from .meta_store import fetch_by_ids

def search_schema(query: str, k: int = 8) -> List[Dict[str, Any]]:
    idx = load_index()
    q = embed_texts([query])
    D, I = idx.search(q, k)
    ids = [int(i) + 1 for i in I[0] if i >= 0]
    rows = fetch_by_ids(ids)
    out = []
    for j, r in enumerate(rows):
        o = dict(r)
        o["score"] = float(D[0][j]) if j < len(D[0]) else 0.0
        out.append(o)
    return out
