# config.py
from pathlib import Path
import os

ROOT = Path(__file__).parent
FAISS_DIR = Path(os.getenv("FAISS_DIR", ROOT / "vector_store"))
FAISS_DIR.mkdir(parents=True, exist_ok=True)

FAISS_INDEX_PATH = FAISS_DIR / "faiss_index.bin"
META_DB_PATH = FAISS_DIR / "faiss_meta.db"

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:simran%4004@localhost:5432/oceaniq_db")
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "128"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
