from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List
from .config import EMBED_MODEL_NAME, BATCH_SIZE
import logging

logger = logging.getLogger("faiss.embeddings")
_model = None
_dim = None

def get_model() -> SentenceTransformer:
    global _model, _dim
    if _model is None:
        logger.info("Loading embedding model: %s", EMBED_MODEL_NAME)
        _model = SentenceTransformer(EMBED_MODEL_NAME)
        _dim = int(_model.get_sentence_embedding_dimension())
        logger.info("Model dimension: %d", _dim)
    return _model

def embedding_dimension() -> int:
    if _dim is None:
        get_model()
    return _dim

def compute_embeddings(texts: List[str]) -> np.ndarray:
    model = get_model()
    embs = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        emb = model.encode(
            batch,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True  # cosine via inner product
        )
        embs.append(emb.astype(np.float32))
    out = np.vstack(embs) if embs else np.zeros((0, embedding_dimension()), dtype=np.float32)
    logger.info("Computed embeddings shape: %s", out.shape)
    return out
