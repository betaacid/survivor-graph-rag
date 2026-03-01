import argparse
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.neo4j_client import run_query, run_write, setup_document_constraints

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


def _build_name_patterns(names):
    patterns = {}
    for name in names:
        if len(name) < MIN_NAME_LENGTH and name not in SHORT_NAME_ALLOWLIST:
            continue
        try:
            pattern = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
            patterns[name] = pattern
        except re.error:
            continue
    return patterns


def _link_players():
    players = run_query("MATCH (p:Player) RETURN p.name AS name")
    player_names = [r["name"] for r in players]
    log.info("Loaded %d player names", len(player_names))
    patterns = _build_name_patterns(player_names)
    log.info("Built %d regex patterns (filtered short names)", len(patterns))

    chunks = run_query("MATCH (c:Chunk) RETURN c.chunk_id AS chunk_id, c.text AS text")
    log.info("Processing %d chunks for player mentions", len(chunks))

    total_links = 0
    for chunk in chunks:
        chunk_id = chunk["chunk_id"]
        text = chunk["text"]
        mentions = []
        for name, pattern in patterns.items():
            if pattern.search(text):
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
                {"chunk_id": chunk_id, "name": name},
            )
        total_links += len(mentions)

    log.info("Created %d MENTIONS->Player links", total_links)


def _link_seasons():
    seasons = run_query("MATCH (s:Season) RETURN s.title AS title, s.number AS number")
    log.info("Loaded %d seasons", len(seasons))

    chunks = run_query("MATCH (c:Chunk) RETURN c.chunk_id AS chunk_id, c.text AS text")

    season_patterns = {}
    for s in seasons:
        title = s["title"]
        number = s.get("number")
        pats = []
        try:
            pats.append(re.compile(r"\b" + re.escape(title) + r"\b", re.IGNORECASE))
        except re.error:
            continue
        if number:
            pats.append(re.compile(r"\bSeason\s+" + str(number) + r"\b", re.IGNORECASE))
        season_patterns[title] = pats

    total_links = 0
    for chunk in chunks:
        chunk_id = chunk["chunk_id"]
        text = chunk["text"]
        matched_titles = set()
        for title, pats in season_patterns.items():
            if any(p.search(text) for p in pats):
                matched_titles.add(title)

        for title in matched_titles:
            run_write(
                """
                MATCH (c:Chunk {chunk_id: $chunk_id})
                MATCH (s:Season {title: $title})
                MERGE (c)-[r:MENTIONS]->(s)
                SET r.method = 'string_match'
                """,
                {"chunk_id": chunk_id, "title": title},
            )
        total_links += len(matched_titles)

    log.info("Created %d MENTIONS->Season links", total_links)


def main():
    parser = argparse.ArgumentParser(description="Link chunks to entities via string matching")
    parser.add_argument("--skip-seasons", action="store_true", help="Skip season linking")
    args = parser.parse_args()

    setup_document_constraints()
    _link_players()

    if not args.skip_seasons:
        _link_seasons()

    log.info("Done.")


if __name__ == "__main__":
    main()
