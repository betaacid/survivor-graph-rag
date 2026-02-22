import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.llm import chat_json
from lib.table_parser import extract_tables_from_html

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TABLES_DIR = DATA_DIR / "tables"

TABLE_TYPES = ["contestants", "episodes", "voting_history", "jury_vote", "episodes_detail", "other"]

CLASSIFY_SYSTEM = """You are a data extraction assistant. You will receive metadata about a table
extracted from a Survivor season Wikipedia page. Classify the table into exactly one type
and map its columns to a canonical schema.

Table types:
- "contestants": player roster (columns: name, age, hometown, original_tribe, merged_tribe, placement, day_out, exit_type)
- "episodes": challenge winners and eliminations by episode (columns: episode_number, title, air_date, reward_winner, immunity_winner, eliminated, tribe)
- "voting_history": who voted for whom each tribal council (columns: voter, episode_1, episode_2, ... with vote targets as values)
- "jury_vote": final jury vote for winner (columns: juror, finalist_1_name, finalist_2_name, ...)
- "episodes_detail": episode list with metadata like viewers, ratings (columns: overall_number, season_number, title, air_date, viewers_millions)
- "other": none of the above

Return JSON with:
{
  "table_type": "<one of the types above>",
  "column_mapping": {"<original_column>": "<canonical_column>", ...},
  "notes": "<any issues or ambiguities>"
}
"""


def classify_table(table_meta, season_title):
    user_prompt = f"""Season: {season_title}
Caption: {table_meta.get('caption', 'None')}
Columns: {table_meta['columns']}
Number of rows: {table_meta['num_rows']}
Sample rows:
{table_meta['sample_rows']}
"""
    return chat_json(CLASSIFY_SYSTEM, user_prompt)


def process_season(html_path, season_title):
    html = Path(html_path).read_text(encoding="utf-8")
    raw_tables = extract_tables_from_html(html)

    classified = []
    for i, table in enumerate(raw_tables):
        try:
            classification = classify_table(table, season_title)
        except Exception as e:
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

    return classified


def main():
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    if not manifest_path.exists():
        print("Run 01_download_seasons.py first.")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    print(f"Extracting and classifying tables for {len(manifest)} seasons...")

    for season in tqdm(manifest, desc="Processing seasons"):
        title = season["title"]
        html_path = season["html_path"]
        safe_name = Path(html_path).stem
        out_path = TABLES_DIR / f"{safe_name}.json"

        if out_path.exists():
            continue

        classified = process_season(html_path, title)
        out_path.write_text(
            json.dumps({"title": title, "tables": classified}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    print(f"Done. Table JSONs saved to {TABLES_DIR}")


if __name__ == "__main__":
    main()
