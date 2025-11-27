import sqlite3
from typing import Iterable, Dict, Any, List
from .config import SCHEMA_META_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY,
  kind TEXT NOT NULL,     -- table | column | index | ddl
  key  TEXT NOT NULL,     -- public.profiles.psal, public.profiles.idx_profiles_geom, etc.
  text TEXT NOT NULL      -- embedded card
);
CREATE INDEX IF NOT EXISTS items_kind_key ON items(kind, key);
"""

def _conn():
    con = sqlite3.connect(str(SCHEMA_META_PATH))
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init():
    con = _conn()
    con.executescript(SCHEMA)
    con.commit()
    con.close()

def clear_all():
    con = _conn()
    con.execute("DELETE FROM items;")
    con.commit()
    con.close()

def upsert_items(rows: Iterable[Dict[str, Any]]):
    con = _conn()
    con.executemany(
        "INSERT INTO items(kind, key, text) VALUES(?,?,?)",
        [(r["kind"], r["key"], r["text"]) for r in rows]
    )
    con.commit()
    con.close()

def fetch_by_ids(ids: List[int]) -> List[Dict[str, Any]]:
    con = _conn()
    cur = con.execute(f"SELECT id, kind, key, text FROM items WHERE id IN ({','.join('?'*len(ids))})", ids)
    out = [{"id": r[0], "kind": r[1], "key": r[2], "text": r[3]} for r in cur.fetchall()]
    con.close()
    return out

def all_texts() -> List[str]:
    con = _conn()
    cur = con.execute("SELECT text FROM items ORDER BY id;")
    out = [r[0] for r in cur.fetchall()]
    con.close()
    return out
