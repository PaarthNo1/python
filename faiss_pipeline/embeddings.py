# embeddings.py
from sentence_transformers import SentenceTransformer
import numpy as np
from typing import List
from .config import EMBED_MODEL_NAME, BATCH_SIZE
import logging

logger = logging.getLogger("faiss.embeddings")
_model = None

def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        logger.info("Loading embedding model: %s", EMBED_MODEL_NAME)
        _model = SentenceTransformer(EMBED_MODEL_NAME)
    return _model

def compute_embeddings(texts: List[str]) -> np.ndarray:
    model = get_model()
    embs = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i+BATCH_SIZE]
        emb = model.encode(batch, show_progress_bar=False, convert_to_numpy=True)
        embs.append(emb)
    embs = np.vstack(embs)
    logger.info("Computed embeddings shape: %s", embs.shape)
    return embs
