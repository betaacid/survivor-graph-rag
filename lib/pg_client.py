import logging
import os

import psycopg2
from psycopg2.extras import execute_values

_conn = None
log = logging.getLogger(__name__)


def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        url = os.getenv("DATABASE_URL")
        log.debug("Connecting to Postgres: %s", url.split("@")[-1] if url else "DATABASE_URL not set")
        _conn = psycopg2.connect(url)
        _conn.autocommit = True
    return _conn


def setup_schema():
    try:
        conn = get_conn()
    except Exception as e:
        log.error("Postgres connection failed: %s", e)
        raise
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                id SERIAL PRIMARY KEY,
                season_title TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                content TEXT NOT NULL,
                embedding vector(1536)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS chunks_embedding_idx
            ON chunks USING hnsw (embedding vector_cosine_ops);
        """)


def insert_chunks(season_title, texts, embeddings):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM chunks WHERE season_title = %s", (season_title,))
        rows = []
        for i, (text, emb) in enumerate(zip(texts, embeddings)):
            rows.append((season_title, i, text, str(emb)))
        execute_values(
            cur,
            "INSERT INTO chunks (season_title, chunk_index, content, embedding) VALUES %s",
            rows,
            template="(%s, %s, %s, %s::vector)",
        )


def search_similar(query_embedding, top_k=8):
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT season_title, chunk_index, content,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM chunks
            ORDER BY embedding <=> %s::vector
            LIMIT %s
            """,
            (str(query_embedding), str(query_embedding), top_k),
        )
        return [
            {"season_title": r[0], "chunk_index": r[1], "content": r[2], "similarity": float(r[3])}
            for r in cur.fetchall()
        ]


def get_chunk_count():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chunks")
        return cur.fetchone()[0]


def truncate_chunks():
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE chunks RESTART IDENTITY")
