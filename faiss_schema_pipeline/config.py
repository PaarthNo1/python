from pathlib import Path
import os

# Optional: load .env (works only if python-dotenv installed)
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

ROOT = Path(__file__).resolve().parent

# Schema FAISS store directory
FAISS_DIR = Path(os.getenv("FAISS_SCHEMA_DIR", ROOT / "vector_store_schema"))
FAISS_DIR.mkdir(parents=True, exist_ok=True)

SCHEMA_INDEX_PATH = FAISS_DIR / "faiss_schema_index.bin"
SCHEMA_META_PATH  = FAISS_DIR / "faiss_schema_meta.db"
SCHEMA_INDEX_META = FAISS_DIR / "faiss_schema_index_meta.json"

# STRICT — no default, no hardcoded DB URL
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. Add it to your environment or .env file."
    )

EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-mpnet-base-v2")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "128"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
