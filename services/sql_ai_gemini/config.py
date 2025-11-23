# services/sql_ai_gemini/config.py
import os
import json
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# file location helpers
THIS_DIR = Path(__file__).resolve().parent            # .../services/sql_ai_gemini
PROJECT_ROOT = THIS_DIR.parent.parent.resolve()       # .../ocean (two levels up if services is inside repo root)
# If your repo root is one level up (services directly under project root), use:
# PROJECT_ROOT = THIS_DIR.parent.resolve()

# Flexible MCP path: environment variable overrides default
MCP_PATH = os.getenv("MCP_PATH", str(PROJECT_ROOT / "mcp.json"))

# fallback: if not found at that path, try one level up too (defensive)
if not Path(MCP_PATH).exists():
    alt = THIS_DIR.parent.resolve() / "mcp.json"   # services/mcp.json
    if alt.exists():
        MCP_PATH = str(alt)

with open(MCP_PATH, "r", encoding="utf-8") as f:
    MCP = json.load(f)

DISALLOWED = [w.lower() for w in MCP["policies"].get("disallowed_statements", [])]
MAX_ROWS = int(MCP["policies"].get("max_rows", 500))
DEFAULT_LIMIT = int(MCP["policies"].get("default_limit", 200))

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
READONLY_DATABASE_URL = os.getenv("READONLY_DATABASE_URL", os.getenv("DATABASE_URL"))
if not READONLY_DATABASE_URL:
    raise RuntimeError("READONLY_DATABASE_URL or DATABASE_URL must be set in environment.")
LOG_PATH = os.getenv("NL_SQL_AUDIT_LOG", str(PROJECT_ROOT / "nl_sql_audit.log"))
