import argparse
import hashlib
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.neo4j_client import run_write, setup_document_constraints
from lib.wiki_fetcher import extract_sections, fetch_parsed_html, get_season_titles

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
WIKI_RAW_DIR = DATA_DIR / "wiki_raw"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

_SEASON_NUM_RE = re.compile(r"\d+")


def _season_number(title):
    for m in _SEASON_NUM_RE.findall(title):
        val = int(m)
        if 1 <= val <= 99:
            return val
    return 0


def _doc_id(title):
    slug = title.replace(" ", "_").replace(":", "")
    return f"wikipedia:{slug}"


def _content_hash(sections):
    combined = "\n\n".join(s["text"] for s in sections)
    return hashlib.sha256(combined.encode()).hexdigest()


def ingest_one(title):
    log.info("Processing: %s", title)
    _pageid, html = fetch_parsed_html(title)
    sections = extract_sections(html)

    doc_id = _doc_id(title)
    url = "https://en.wikipedia.org/wiki/" + title.replace(" ", "_")
    content_hash = _content_hash(sections)

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
            "hash": content_hash,
        },
    )

    WIKI_RAW_DIR.mkdir(parents=True, exist_ok=True)
    safe = doc_id.replace(":", "_").replace("/", "_")
    plain = "\n\n".join(f"## {s['heading']}\n{s['text']}" for s in sections)
    (WIKI_RAW_DIR / f"{safe}.txt").write_text(plain, encoding="utf-8")
    (WIKI_RAW_DIR / f"{safe}.json").write_text(
        json.dumps(sections, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("  doc_id=%s  sections=%d  hash=%s", doc_id, len(sections), content_hash[:12])
    return doc_id


def main():
    parser = argparse.ArgumentParser(description="Ingest Wikipedia season pages as Document nodes")
    parser.add_argument("--seasons", nargs="+", type=int, help="Season numbers to ingest")
    parser.add_argument("--limit", type=int, default=None, help="Limit to first N seasons")
    parser.add_argument("--all", action="store_true", help="Ingest all seasons (default)")
    args = parser.parse_args()

    setup_document_constraints()

    titles = get_season_titles()

    if args.seasons:
        wanted = set(args.seasons)
        titles = [t for t in titles if _season_number(t) in wanted]
    elif args.limit:
        titles = titles[: args.limit]

    log.info("Will ingest %d season document(s)", len(titles))
    created = []
    for title in titles:
        doc_id = ingest_one(title)
        created.append(doc_id)

    log.info("Done. Created/updated %d Document node(s).", len(created))


if __name__ == "__main__":
    main()
