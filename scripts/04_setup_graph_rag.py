import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.llm import chat_json
from lib.neo4j_client import (
    add_vote,
    get_node_counts,
    link_episode_eliminated,
    link_episode_immunity,
    link_episode_reward,
    link_player_tribe,
    setup_constraints,
    upsert_episode,
    upsert_player,
    upsert_player_season,
    upsert_season,
    upsert_tribe,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TABLES_DIR = DATA_DIR / "tables"

SEASON_NUMBER_PATTERN = re.compile(r"[Ss]eason\s*(\d+)|(\d+)(?:st|nd|rd|th)\s+season|^Survivor\s+(\d+)")

NORMALIZE_SYSTEM = """You are a data normalization assistant for Survivor TV show data.
Given a classified table and its column mapping, produce clean canonical rows.

For "contestants" tables, return JSON:
{
  "players": [
    {
      "name": "Full Name",
      "age": 25,
      "hometown": "City, State",
      "original_tribe": "TribeName",
      "placement": "1st voted out",
      "day_out": 3,
      "exit_type": "voted_out|medevac|quit|winner|runner_up",
      "jury_member": true/false
    }
  ]
}

For "episodes" tables (challenge winners and eliminations), return JSON:
{
  "episodes": [
    {
      "episode_number": 1,
      "title": "Episode Title",
      "air_date": "January 28, 2001",
      "reward_winners": ["Name1"],
      "immunity_winners": ["Name1"],
      "eliminated": "PlayerName",
      "eliminated_tribe": "TribeName"
    }
  ]
}

For "voting_history" tables, return JSON:
{
  "votes": [
    {"voter": "Name", "episode_number": 1, "target": "OtherName"}
  ]
}

For "jury_vote" tables, return JSON:
{
  "jury_votes": [
    {"juror": "Name", "voted_for": "FinalistName"}
  ]
}

For "episodes_detail" tables, return JSON:
{
  "episode_details": [
    {"episode_number": 1, "title": "...", "air_date": "...", "viewers_millions": 45.37}
  ]
}

Rules:
- Use full first and last names when available
- Clean up artifacts like footnote markers, brackets, etc.
- If a value is missing or unclear, use null
- For exit_type, the winner should be "winner" and runner-up(s) should be "runner_up"
- jury_member is true if the player served on the final jury
- Skip recap/reunion episodes that have no tribal council
- Return ONLY valid JSON, no explanation
"""


def extract_season_number(title):
    match = re.search(r"[Ss]eason\s*(\d+)", title)
    if match:
        return int(match.group(1))

    known = {
        "Borneo": 1, "The Australian Outback": 2, "Africa": 3, "Marquesas": 4,
        "Thailand": 5, "The Amazon": 6, "Pearl Islands": 7, "All-Stars": 8,
        "Vanuatu": 9, "Palau": 10, "Guatemala": 11, "Panama": 12,
        "Cook Islands": 13, "Fiji": 14, "China": 15, "Micronesia": 16,
        "Gabon": 17, "Tocantins": 18, "Samoa": 19, "Heroes vs. Villains": 20,
        "Nicaragua": 21, "Redemption Island": 22, "South Pacific": 23,
        "One World": 24, "Philippines": 25, "Caramoan": 26, "Blood vs. Water": 27,
        "Cagayan": 28, "San Juan del Sur": 29, "Worlds Apart": 30,
        "Cambodia": 31, "Kaôh Rōng": 32, "Millennials vs. Gen X": 33,
        "Game Changers": 34, "Heroes vs. Healers vs. Hustlers": 35,
        "Ghost Island": 36, "David vs. Goliath": 37, "Edge of Extinction": 38,
        "Island of the Idols": 39, "Winners at War": 40, "41": 41, "42": 42,
        "43": 43, "44": 44, "45": 45, "46": 46, "47": 47, "48": 48,
        "49": 49, "50": 50,
    }
    for subtitle, num in known.items():
        if subtitle.lower() in title.lower():
            return num

    nums = re.findall(r"(\d+)", title)
    for n in nums:
        val = int(n)
        if 1 <= val <= 50:
            return val

    return None


def normalize_table(table, season_title):
    table_type = table.get("table_type", "other")
    if table_type == "other":
        return None, None

    col_mapping = table.get("column_mapping", {})
    mapping_info = json.dumps(col_mapping) if col_mapping else "no mapping"

    rows_sample = table["rows"][:20] if len(table["rows"]) > 20 else table["rows"]
    user_prompt = f"""Season: {season_title}
Table type: {table_type}
Column mapping: {mapping_info}
Columns: {table['columns']}
Rows (may be truncated):
{json.dumps(rows_sample, ensure_ascii=False)}
"""
    if len(table["rows"]) > 20:
        all_rows = table["rows"]
        remaining = json.dumps(all_rows[20:], ensure_ascii=False)
        user_prompt += f"\nRemaining rows:\n{remaining}"

    try:
        result = chat_json(NORMALIZE_SYSTEM, user_prompt)
        return table_type, result
    except Exception as e:
        print(f"    LLM normalization failed for {table_type}: {e}")
        return table_type, None


def ingest_season(season_title, season_number, tables_data):
    upsert_season({"title": season_title, "number": season_number})

    contestants_data = None
    episodes_data = None
    votes_data = None
    jury_data = None
    episode_details = None

    for table in tables_data:
        table_type, normalized = normalize_table(table, season_title)
        if normalized is None:
            continue

        if table_type == "contestants":
            contestants_data = normalized.get("players", [])
        elif table_type == "episodes":
            episodes_data = normalized.get("episodes", [])
        elif table_type == "voting_history":
            votes_data = normalized.get("votes", [])
        elif table_type == "jury_vote":
            jury_data = normalized.get("jury_votes", [])
        elif table_type == "episodes_detail":
            episode_details = normalized.get("episode_details", [])

    tribes_seen = set()
    if contestants_data:
        for p in contestants_data:
            name = p.get("name")
            if not name:
                continue
            upsert_player(name)

            exit_type = p.get("exit_type", "voted_out")
            jury_member = p.get("jury_member", False)
            if isinstance(jury_member, str):
                jury_member = jury_member.lower() in ("true", "yes", "1")

            day_out = p.get("day_out")
            if day_out is not None:
                try:
                    day_out = int(day_out)
                except (ValueError, TypeError):
                    day_out = None

            age = p.get("age")
            if age is not None:
                try:
                    age = int(age)
                except (ValueError, TypeError):
                    age = None

            ps_props = {
                "player_name": name,
                "season_number": season_number,
                "age": age,
                "hometown": p.get("hometown"),
                "placement": p.get("placement"),
                "day_out": day_out,
                "exit_type": exit_type,
                "jury_member": jury_member,
            }
            upsert_player_season(name, season_number, ps_props)

            tribe = p.get("original_tribe")
            if tribe and tribe.lower() not in ("", "none", "null"):
                if tribe not in tribes_seen:
                    upsert_tribe(tribe, season_number, "premerge")
                    tribes_seen.add(tribe)
                link_player_tribe(name, season_number, tribe)

    if episodes_data:
        for ep in episodes_data:
            ep_num = ep.get("episode_number")
            if ep_num is None:
                continue
            try:
                ep_num = int(ep_num)
            except (ValueError, TypeError):
                continue

            ep_props = {
                "season_number": season_number,
                "episode_number": ep_num,
                "title": ep.get("title"),
                "air_date": ep.get("air_date"),
            }
            upsert_episode(season_number, ep_num, ep_props)

            for winner in ep.get("immunity_winners") or []:
                if winner and winner.lower() not in ("none", "null", ""):
                    link_episode_immunity(season_number, ep_num, winner)

            for winner in ep.get("reward_winners") or []:
                if winner and winner.lower() not in ("none", "null", ""):
                    link_episode_reward(season_number, ep_num, winner)

            eliminated = ep.get("eliminated")
            if eliminated and eliminated.lower() not in ("none", "null", ""):
                link_episode_eliminated(season_number, ep_num, eliminated)

    if episode_details:
        for ed in episode_details:
            ep_num = ed.get("episode_number")
            if ep_num is None:
                continue
            try:
                ep_num = int(ep_num)
            except (ValueError, TypeError):
                continue

            viewers = ed.get("viewers_millions")
            if viewers is not None:
                try:
                    viewers = float(viewers)
                except (ValueError, TypeError):
                    viewers = None

            upsert_episode(season_number, ep_num, {
                "season_number": season_number,
                "episode_number": ep_num,
                "title": ed.get("title"),
                "air_date": ed.get("air_date"),
                "viewers_millions": viewers,
            })

    if votes_data:
        for vote in votes_data:
            voter = vote.get("voter")
            target = vote.get("target")
            ep_num = vote.get("episode_number")
            if voter and target and ep_num:
                try:
                    ep_num = int(ep_num)
                except (ValueError, TypeError):
                    continue
                add_vote(voter, target, season_number, ep_num)


def main():
    tables_dir = TABLES_DIR
    if not tables_dir.exists():
        print("Run 02_extract_tables.py first.")
        sys.exit(1)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    print("Setting up Neo4j constraints and indexes...")
    setup_constraints()

    print(f"Ingesting {len(manifest)} seasons into Neo4j...")

    for season in tqdm(manifest, desc="Ingesting"):
        title = season["title"]
        season_number = extract_season_number(title)
        if season_number is None:
            print(f"  Could not determine season number for: {title}, skipping.")
            continue

        safe_name = Path(season["html_path"]).stem
        table_path = tables_dir / f"{safe_name}.json"
        if not table_path.exists():
            print(f"  No table file for {title}, skipping.")
            continue

        table_doc = json.loads(table_path.read_text(encoding="utf-8"))
        ingest_season(title, season_number, table_doc["tables"])

    counts = get_node_counts()
    print(f"\nDone. Node counts: {counts}")


if __name__ == "__main__":
    main()
