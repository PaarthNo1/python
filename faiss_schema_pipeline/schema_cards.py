# faiss_schema_pipeline/schema_cards.py
import textwrap
import psycopg2
from typing import List, Dict
from .config import DATABASE_URL

def _pg():
    return psycopg2.connect(DATABASE_URL)

def _rows(cur):
    cols = [d.name for d in cur.description]
    for r in cur.fetchall():
        yield dict(zip(cols, r))

def build_schema_cards() -> List[Dict]:
    con = _pg()
    cur = con.cursor()

    # Tables
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY 1,2;
    """)
    tables = list(_rows(cur))

    # Columns
    cur.execute("""
        SELECT table_schema, table_name, column_name, data_type, udt_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog','information_schema')
        ORDER BY table_schema, table_name, ordinal_position;
    """)
    columns = list(_rows(cur))

    # Indexes
    cur.execute("""
        SELECT
            n.nspname AS schema,
            t.relname AS table_name,
            i.relname AS index_name,
            pg_get_indexdef(ix.indexrelid) AS indexdef
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        WHERE t.relkind='r'
          AND n.nspname NOT IN ('pg_catalog','information_schema')
        ORDER BY 1,2,3;
    """)
    indexes = list(_rows(cur))

    # Constraints
    cur.execute("""
        SELECT
            n.nspname   AS schema,
            t.relname   AS table_name,
            c.conname   AS constraint_name,
            c.contype   AS constraint_type,
            pg_get_constraintdef(c.oid, true) AS constraintdef
        FROM pg_constraint c
        JOIN pg_class t      ON t.oid = c.conrelid
        JOIN pg_namespace n  ON n.oid = t.relnamespace
        WHERE n.nspname NOT IN ('pg_catalog','information_schema')
        ORDER BY 1,2,3;
    """)
    constraints = list(_rows(cur))

    con.close()

    cards: List[Dict] = []

    for t in tables:
        key = f"{t['table_schema']}.{t['table_name']}"
        cards.append({"kind": "table", "key": key, "text": f"Table {key}: base table."})

    for c in columns:
        key = f"{c['table_schema']}.{c['table_name']}.{c['column_name']}"
        txt = textwrap.dedent(f"""
        Column: {key}
        Type: {c['data_type']} ({c['udt_name']})
        Nullable: {c['is_nullable']}
        Default: {c['column_default'] or 'NULL'}
        """).strip()
        cards.append({"kind": "column", "key": key, "text": txt})

    for ix in indexes:
        key = f"{ix['schema']}.{ix['table_name']}.{ix['index_name']}"
        txt = textwrap.dedent(f"""
        Index: {key}
        Definition: {ix['indexdef']}
        """).strip()
        cards.append({"kind": "index", "key": key, "text": txt})

    for k in constraints:
        key = f"{k['schema']}.{k['table_name']}.{k['constraint_name']}"
        txt = textwrap.dedent(f"""
        Constraint: {key}
        Type: {k['constraint_type']}
        Definition: {k['constraintdef']}
        """).strip()
        cards.append({"kind": "constraint", "key": key, "text": txt})

    return cards
