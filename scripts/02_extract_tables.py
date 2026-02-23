import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.llm import groq_strict
from lib.table_parser import extract_tables_from_html
from lib.vote_parser import parse_jury_vote, parse_voting_history

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TABLES_DIR = DATA_DIR / "tables"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

TABLE_TYPES = ["contestants", "episodes", "voting_history", "jury_vote", "episodes_detail", "other"]

CLASSIFY_SYSTEM = """You are a data extraction assistant. You will receive metadata about a table
extracted from a Survivor season Wikipedia page. Classify the table into exactly one type
and map its columns to a canonical schema.

Table types:
- "contestants": player roster (canonical columns: name, age, hometown, original_tribe, merged_tribe, placement, day_out, exit_type)
- "episodes": challenge winners and eliminations by episode (canonical columns: episode_number, title, air_date, reward_winner, immunity_winner, eliminated, tribe)
- "voting_history": who voted for whom each tribal council (canonical columns: voter, episode_1, episode_2, ... with vote targets as values)
- "jury_vote": final jury vote for winner (canonical columns: juror, finalist_1_name, finalist_2_name, ...)
- "episodes_detail": episode list with metadata like viewers, ratings (canonical columns: overall_number, season_number, title, air_date, viewers_millions)
- "other": none of the above

Return a JSON object with:
- table_type: one of the types above
- mappings: an array where each element maps a source column to a canonical column
- notes: any issues or ambiguities (or null if none)
"""

CLASSIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "table_type": {
            "type": "string",
            "enum": ["contestants", "episodes", "voting_history", "jury_vote", "episodes_detail", "other"],
        },
        "mappings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "source_column": {"type": "string"},
                    "canonical_column": {"type": "string"},
                },
                "required": ["source_column", "canonical_column"],
                "additionalProperties": False,
            },
        },
        "notes": {"type": ["string", "null"]},
    },
    "required": ["table_type", "mappings", "notes"],
    "additionalProperties": False,
}


def _looks_like_voting_table(table):
    caption = (table.get("caption") or "").lower()
    if "voting history" in caption:
        return True
    cols = " ".join(str(c).lower() for c in table.get("columns", []))
    return "voter" in cols and ("episode" in cols or "vote" in cols)


def _looks_like_jury_table(table):
    caption = (table.get("caption") or "").lower()
    if "jury vote" in caption:
        return True
    cols = " ".join(str(c).lower() for c in table.get("columns", []))
    return "juror" in cols and ("finalist" in cols or "vote" in cols)


def classify_table(table_meta, season_title):
    user_prompt = f"""Season: {season_title}
Caption: {table_meta.get('caption', 'None')}
Columns: {table_meta['columns']}
Number of rows: {table_meta['num_rows']}
Sample rows:
{table_meta['sample_rows']}
"""
    result = groq_strict(CLASSIFY_SYSTEM, user_prompt, CLASSIFY_SCHEMA, schema_name="table_classification")
    mapping_dict = {m["source_column"]: m["canonical_column"] for m in result.get("mappings", [])}
    result["column_mapping"] = mapping_dict
    return result


def process_season(html_path, season_title):
    html = Path(html_path).read_text(encoding="utf-8")
    raw_tables = extract_tables_from_html(html)

    has_custom_voting = False
    has_custom_jury = False

    voting = parse_voting_history(html)
    if voting and voting.get("votes"):
        has_custom_voting = True
        log.info("Custom parser extracted %d votes for %s", len(voting["votes"]), season_title)

    jury = parse_jury_vote(html)
    if jury and jury.get("jury_votes"):
        has_custom_jury = True
        log.info("Custom parser extracted %d jury votes for %s", len(jury["jury_votes"]), season_title)

    classified = []
    for i, table in enumerate(raw_tables):
        if len(table.get("columns", [])) > 50:
            log.info("  Skipping table %d with %d columns (likely viewership/ratings)", i, len(table["columns"]))
            classification = {"table_type": "other", "column_mapping": {}, "notes": "Skipped: too many columns"}
        elif has_custom_voting and _looks_like_voting_table(table):
            log.info("  Skipping Groq call for table %d (voting history already custom-parsed)", i)
            classification = {"table_type": "other", "column_mapping": {}, "notes": "Skipped: custom parser already extracted voting data"}
        elif has_custom_jury and _looks_like_jury_table(table):
            log.info("  Skipping Groq call for table %d (jury vote already custom-parsed)", i)
            classification = {"table_type": "other", "column_mapping": {}, "notes": "Skipped: custom parser already extracted jury data"}
        else:
            try:
                classification = classify_table(table, season_title)
            except Exception as e:
                log.warning("Table %d classification failed for %s: %s", i, season_title, e)
                classification = {"table_type": "other", "column_mapping": {}, "notes": str(e)}

        classified.append({
            "index": i,
            "caption": table.get("caption"),
            "table_type": classification.get("table_type", "other"),
            "column_mapping": classification.get("column_mapping", {}),
            "notes": classification.get("notes", ""),
            "columns": table["columns"],
            "num_rows": table["num_rows"],
            "rows": table["rows"],
        })

    if has_custom_voting:
        classified.append({
            "index": len(classified),
            "caption": "Voting history (custom parser)",
            "table_type": "voting_history",
            "column_mapping": {},
            "notes": "Extracted by custom BeautifulSoup parser",
            "columns": ["voter", "episode_number", "target"],
            "num_rows": len(voting["votes"]),
            "rows": [],
            "votes": voting["votes"],
        })

    if has_custom_jury:
        classified.append({
            "index": len(classified),
            "caption": "Jury vote (custom parser)",
            "table_type": "jury_vote",
            "column_mapping": {},
            "notes": "Extracted by custom BeautifulSoup parser",
            "columns": ["juror", "voted_for"],
            "num_rows": len(jury["jury_votes"]),
            "rows": [],
            "jury_votes": jury["jury_votes"],
        })

    return classified


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true", help="Re-extract even if table JSON exists (no cache)")
    args = parser.parse_args()

    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    if not manifest_path.exists():
        log.error("Run 01_download_seasons.py first.")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    log.info("Extracting and classifying tables for %d seasons...", len(manifest))

    for season in tqdm(manifest, desc="Processing seasons"):
        title = season["title"]
        html_path = season["html_path"]
        safe_name = Path(html_path).stem
        out_path = TABLES_DIR / f"{safe_name}.json"

        if not args.fresh and out_path.exists():
            continue

        classified = process_season(html_path, title)
        out_path.write_text(
            json.dumps({"title": title, "tables": classified}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    log.info("Done. Table JSONs saved to %s", TABLES_DIR)


if __name__ == "__main__":
    main()
