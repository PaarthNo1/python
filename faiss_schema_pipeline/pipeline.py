import logging
from .meta_store import init, clear_all, upsert_items, all_texts
from .index_store import load_index, save_index
from .embeddings import embed_texts
from .schema_cards import build_schema_cards

log = logging.getLogger("schema.pipeline")

def rebuild():
    logging.basicConfig(level=logging.INFO)
    log.info("Building schema cards…")
    cards = build_schema_cards()

    init(); clear_all(); upsert_items(cards)

    texts = all_texts()
    idx = load_index()
    # reset fresh
    if idx.ntotal > 0:
        import faiss
        idx = faiss.IndexFlatIP(idx.d)
    idx.add(embed_texts(texts))
    save_index(idx)
    log.info("Schema FAISS rebuilt. %d cards.", len(texts))

if __name__ == "__main__":
    rebuild()
