import argparse
import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from lib.neo4j_client import run_query, run_write, setup_document_constraints
from lib.utils import extract_season_number
from lib.wiki_chunking import chunk_sections
from lib.wiki_fetcher import extract_sections

load_dotenv()

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
WIKI_RAW_DIR = DATA_DIR / "wiki_raw"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

MAX_MENTIONS_PER_CHUNK = 25
MIN_NAME_LENGTH = 4
SHORT_NAME_ALLOWLIST = {
    "Rob",
    "Abi",
    "Tai",
    "Yau",
    "Jud",
    "Lex",
    "Ami",
    "Ian",
    "Tom",
    "Joe",
    "Jay",
    "Kim",
    "Woo",
    "Dan",
    "Jon",
    "Kat",
    "Boo",
    "Ace",
    "Ben",
    "Dee",
    "Kee",
}


def doc_id_for_title(title):
    slug = title.replace(" ", "_").replace(":", "")
    return f"wikipedia:{slug}"


def content_hash(sections):
    combined = "\n\n".join(s["text"] for s in sections)
    return hashlib.sha256(combined.encode()).hexdigest()


def write_sections_to_disk(doc_id, sections):
    WIKI_RAW_DIR.mkdir(parents=True, exist_ok=True)
    safe = doc_id.replace(":", "_").replace("/", "_")
    plain = "\n\n".join(f"## {s['heading']}\n{s['text']}" for s in sections)
    (WIKI_RAW_DIR / f"{safe}.txt").write_text(plain, encoding="utf-8")
    (WIKI_RAW_DIR / f"{safe}.json").write_text(
        json.dumps(sections, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def ingest_document(title, html):
    log.info("Processing: %s", title)
    sections = extract_sections(html)
    doc_id = doc_id_for_title(title)
    url = "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
    hash_value = content_hash(sections)

    run_write(
        """
        MERGE (d:Document {doc_id: $doc_id})
        SET d.source = $source,
            d.url = $url,
            d.title = $title,
            d.fetched_at = $fetched_at,
            d.hash = $hash
        """,
        {
            "doc_id": doc_id,
            "source": "wikipedia",
            "url": url,
            "title": title,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "hash": hash_value,
        },
    )

    write_sections_to_disk(doc_id, sections)
    log.info("  doc_id=%s  sections=%d  hash=%s", doc_id, len(sections), hash_value[:12])
    return doc_id, sections


def upsert_chunks(doc_id, chunks):
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


def build_name_patterns(names):
    patterns = {}
    for name in names:
        if len(name) < MIN_NAME_LENGTH and name not in SHORT_NAME_ALLOWLIST:
            continue
        try:
            patterns[name] = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
        except re.error:
            continue
    return patterns


def clear_mentions():
    run_write("MATCH (:Chunk)-[r:MENTIONS]->() DELETE r")


def link_players():
    players = run_query("MATCH (p:Player) RETURN p.name AS name")
    player_names = [r["name"] for r in players]
    log.info("Loaded %d player names", len(player_names))
    patterns = build_name_patterns(player_names)
    log.info("Built %d regex patterns (filtered short names)", len(patterns))

    chunks = run_query("MATCH (c:Chunk) RETURN c.chunk_id AS chunk_id, c.text AS text")
    log.info("Processing %d chunks for player mentions", len(chunks))

    total_links = 0
    for chunk in chunks:
        mentions = []
        for name, pattern in patterns.items():
            if pattern.search(chunk["text"]):
                mentions.append(name)
            if len(mentions) >= MAX_MENTIONS_PER_CHUNK:
                break

        for name in mentions:
            run_write(
                """
                MATCH (c:Chunk {chunk_id: $chunk_id})
                MATCH (p:Player {name: $name})
                MERGE (c)-[r:MENTIONS]->(p)
                SET r.method = 'string_match'
                """,
                {"chunk_id": chunk["chunk_id"], "name": name},
            )
        total_links += len(mentions)

    log.info("Created %d MENTIONS->Player links", total_links)


def link_seasons():
    seasons = run_query("MATCH (s:Season) RETURN s.title AS title, s.number AS number")
    log.info("Loaded %d seasons", len(seasons))
    chunks = run_query("MATCH (c:Chunk) RETURN c.chunk_id AS chunk_id, c.text AS text")

    season_patterns = {}
    for season in seasons:
        title = season["title"]
        number = season.get("number")
        patterns = []
        try:
            patterns.append(re.compile(r"\b" + re.escape(title) + r"\b", re.IGNORECASE))
        except re.error:
            continue
        if number:
            patterns.append(re.compile(r"\bSeason\s+" + str(number) + r"\b", re.IGNORECASE))
        season_patterns[title] = patterns

    total_links = 0
    for chunk in chunks:
        matched_titles = set()
        for title, patterns in season_patterns.items():
            if any(pattern.search(chunk["text"]) for pattern in patterns):
                matched_titles.add(title)

        for title in matched_titles:
            run_write(
                """
                MATCH (c:Chunk {chunk_id: $chunk_id})
                MATCH (s:Season {title: $title})
                MERGE (c)-[r:MENTIONS]->(s)
                SET r.method = 'string_match'
                """,
                {"chunk_id": chunk["chunk_id"], "title": title},
            )
        total_links += len(matched_titles)

    log.info("Created %d MENTIONS->Season links", total_links)


def load_manifest():
    manifest_path = DATA_DIR / "seasons_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError("Run 02_download_wiki.py first.")
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def selected_seasons(args):
    seasons = load_manifest()
    if args.seasons:
        wanted = {int(s.strip()) for s in args.seasons.split(",") if s.strip()}
        seasons = [season for season in seasons if extract_season_number(season["title"]) in wanted]
    elif args.limit:
        seasons = seasons[:args.limit]
    return seasons


def main():
    parser = argparse.ArgumentParser(description="Ingest Wikipedia documents and chunk nodes into Neo4j")
    parser.add_argument("--seasons", type=str, help="Comma-separated season numbers")
    parser.add_argument("--limit", type=int, default=None, help="Limit to first N seasons")
    parser.add_argument("--skip-seasons", action="store_true", help="Skip season mention linking")
    args = parser.parse_args()

    setup_document_constraints()

    seasons = selected_seasons(args)
    log.info("Will ingest %d season document(s)", len(seasons))

    for season in seasons:
        title = season["title"]
        html = Path(season["html_path"]).read_text(encoding="utf-8")
        doc_id, sections = ingest_document(title, html)
        chunks = chunk_sections(sections, doc_id)
        upsert_chunks(doc_id, chunks)
        log.info("  [chunked] %s -> %d chunks", doc_id, len(chunks))

    clear_mentions()
    link_players()
    if not args.skip_seasons:
        link_seasons()

    log.info("Done.")


if __name__ == "__main__":
    main()
