# services/sql_ai_gemini/rag_builder.py

from faiss_schema_pipeline.search import search_schema
from faiss_pipeline.search import semantic_search as search_profiles  # <-- FIXED
from .sql_patterns import PATTERNS

def _fmt_schema_hits(q: str, k: int) -> str:
    hits = search_schema(q, k=k)
    lines = []
    for h in hits:
        lines.append(f"[SCHEMA {h['kind']}] {h['key']}\n{h['text']}")
    return "\n\n".join(lines)

def _fmt_profile_hits(q: str, k: int) -> str:
    # semantic_search returns list of dicts with keys like uid, summary, score
    hits = search_profiles(q, top_k=k)  # <-- FIXED
    lines = []
    for h in hits:
        uid = h.get("uid", "")
        summary = h.get("summary", "")
        score = h.get("score", 0.0)
        lines.append(f"UID: {uid} | SCORE: {score:.3f}\n{summary}")
    return "\n\n".join(lines)

def _fmt_patterns(n: int) -> str:
    parts = []
    for p in PATTERNS[:n]:
        parts.append(f"-- {p['title']}\n{p['sql']}")
    return "\n\n".join(parts)

def build_rag_context(question: str, top_k: int = 5) -> str:
    schema_txt  = _fmt_schema_hits(question, k=top_k)
    profile_txt = _fmt_profile_hits(question, k=top_k)
    patterns    = _fmt_patterns(5)
    context = f"""
# SCHEMA CARDS (top {top_k})
{schema_txt}

# PROFILE SUMMARIES (top {top_k})
{profile_txt}

# CANONICAL SQL PATTERNS
{patterns}
"""
    return context.strip()
