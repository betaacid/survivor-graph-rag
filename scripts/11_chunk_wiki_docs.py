import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.chunking.wiki_chunker import chunk_sections
from lib.neo4j_client import run_query, run_write, setup_document_constraints

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
WIKI_RAW_DIR = DATA_DIR / "wiki_raw"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def _load_sections(doc_id):
    safe = doc_id.replace(":", "_").replace("/", "_")
    path = WIKI_RAW_DIR / f"{safe}.json"
    if not path.exists():
        log.warning("No sections file for %s at %s", doc_id, path)
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _existing_hash(doc_id):
    rows = run_query(
        "MATCH (d:Document {doc_id: $doc_id}) RETURN d.hash AS hash",
        {"doc_id": doc_id},
    )
    return rows[0]["hash"] if rows else None


def _has_chunks(doc_id):
    rows = run_query(
        "MATCH (:Document {doc_id: $doc_id})-[:HAS_CHUNK]->(:Chunk) RETURN count(*) AS cnt",
        {"doc_id": doc_id},
    )
    return rows[0]["cnt"] > 0 if rows else False


def _upsert_chunks(doc_id, chunks):
    run_write(
        """
        MATCH (:Document {doc_id: $doc_id})-[:HAS_CHUNK]->(c:Chunk)
        DETACH DELETE c
        """,
        {"doc_id": doc_id},
    )

    for chunk in chunks:
        run_write(
            """
            MERGE (c:Chunk {chunk_id: $chunk_id})
            SET c.doc_id = $doc_id,
                c.text = $text,
                c.section = $section,
                c.idx = $idx,
                c.char_start = $char_start,
                c.char_end = $char_end
            WITH c
            MATCH (d:Document {doc_id: $doc_id})
            MERGE (d)-[:HAS_CHUNK]->(c)
            """,
            chunk,
        )


def main():
    parser = argparse.ArgumentParser(description="Chunk Wikipedia documents and write Chunk nodes")
    parser.add_argument("--force", action="store_true", help="Re-chunk even if hash unchanged")
    args = parser.parse_args()

    setup_document_constraints()

    docs = run_query(
        "MATCH (d:Document {source: 'wikipedia'}) RETURN d.doc_id AS doc_id, d.hash AS hash"
    )
    log.info("Found %d Wikipedia Document node(s)", len(docs))

    for doc in docs:
        doc_id = doc["doc_id"]
        sections = _load_sections(doc_id)
        if sections is None:
            continue

        if not args.force and _has_chunks(doc_id):
            log.info("  [skip] %s (already chunked, hash unchanged)", doc_id)
            continue

        chunks = chunk_sections(sections, doc_id)
        _upsert_chunks(doc_id, chunks)
        log.info("  [chunked] %s -> %d chunks", doc_id, len(chunks))

    log.info("Done.")


if __name__ == "__main__":
    main()
