import numpy as np
from typing import List
from sentence_transformers import SentenceTransformer
from .config import EMBED_MODEL_NAME, BATCH_SIZE

_model = None
_dim = None

def get_model() -> SentenceTransformer:
    global _model, _dim
    if _model is None:
        _model = SentenceTransformer(EMBED_MODEL_NAME)
        _dim = int(_model.get_sentence_embedding_dimension())
    return _model

def embedding_dimension() -> int:
    if _dim is None:
        get_model()
    return _dim

def embed_texts(texts: List[str]) -> np.ndarray:
    model = get_model()
    embs = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        emb = model.encode(batch, convert_to_numpy=True, normalize_embeddings=True, show_progress_bar=False)
        embs.append(emb.astype(np.float32))
    return np.vstack(embs) if embs else np.zeros((0, embedding_dimension()), dtype=np.float32)
