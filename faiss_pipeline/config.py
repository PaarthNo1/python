# config.py
from pathlib import Path
import os

# Optional: load .env automatically (requires python-dotenv)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    # If python-dotenv isn't installed, os.getenv will still read real env vars.
    pass

ROOT = Path(__file__).resolve().parent

# Where to keep FAISS artifacts (overridable via FAISS_DIR)
FAISS_DIR = Path(os.getenv("FAISS_DIR", ROOT / "vector_store"))
FAISS_DIR.mkdir(parents=True, exist_ok=True)

FAISS_INDEX_PATH = FAISS_DIR / "faiss_index.bin"
META_DB_PATH = FAISS_DIR / "faiss_meta.db"

# STRICT: no default, no secret in code
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL is not set. Put it in your environment or a .env file."
    )

# Tunables
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-mpnet-base-v2")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "128"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
