import argparse
import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.llm import groq_strict
from lib.neo4j_client import (
    add_jury_vote,
    add_vote,
    get_node_counts,
    link_episode_eliminated,
    link_episode_immunity,
    link_episode_reward,
    link_episode_tribe,
    link_player_tribe,
    link_tribal_attendee,
    setup_constraints,
    upsert_episode,
    upsert_player,
    upsert_player_season,
    upsert_season,
    upsert_tribal_council,
    upsert_tribe,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TABLES_DIR = DATA_DIR / "tables"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

SEASON_NUMBER_PATTERN = re.compile(r"[Ss]eason\s*(\d+)|(\d+)(?:st|nd|rd|th)\s+season|^Survivor\s+(\d+)")

NORMALIZE_SYSTEM = """You are a data normalization assistant for Survivor TV show data.
Given a classified table and its column mapping, produce clean canonical rows.
Return ONLY valid JSON matching the requested schema.

Rules:
- Use full first and last names when available
- Clean up artifacts like footnote markers, brackets, etc.
- If a value is missing or unclear, use null
- For exit_type, the winner should be "winner" and runner-up(s) should be "runner_up"
- jury_member is true if the player served on the final jury
- Skip recap/reunion episodes that have no tribal council
"""

CONTESTANTS_SCHEMA = {
    "type": "object",
    "properties": {
        "players": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "age": {"type": ["integer", "null"]},
                    "hometown": {"type": ["string", "null"]},
                    "original_tribe": {"type": ["string", "null"]},
                    "placement": {"type": ["string", "null"]},
                    "day_out": {"type": ["integer", "null"]},
                    "exit_type": {"type": ["string", "null"]},
                    "jury_member": {"type": ["boolean", "null"]},
                    "merged_tribe": {"type": ["string", "null"]},
                },
                "required": ["name", "age", "hometown", "original_tribe", "placement", "day_out", "exit_type", "jury_member", "merged_tribe"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["players"],
    "additionalProperties": False,
}

EPISODES_SCHEMA = {
    "type": "object",
    "properties": {
        "episodes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "episode_number": {"type": ["integer", "null"]},
                    "title": {"type": ["string", "null"]},
                    "air_date": {"type": ["string", "null"]},
                    "reward_winners": {"type": ["array", "null"], "items": {"type": "string"}},
                    "immunity_winners": {"type": ["array", "null"], "items": {"type": "string"}},
                    "eliminated": {"type": ["string", "null"]},
                    "eliminated_tribe": {"type": ["string", "null"]},
                },
                "required": ["episode_number", "title", "air_date", "reward_winners", "immunity_winners", "eliminated", "eliminated_tribe"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["episodes"],
    "additionalProperties": False,
}

VOTING_HISTORY_SCHEMA = {
    "type": "object",
    "properties": {
        "votes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "voter": {"type": ["string", "null"]},
                    "episode_number": {"type": ["integer", "null"]},
                    "target": {"type": ["string", "null"]},
                },
                "required": ["voter", "episode_number", "target"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["votes"],
    "additionalProperties": False,
}

JURY_VOTE_SCHEMA = {
    "type": "object",
    "properties": {
        "jury_votes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "juror": {"type": ["string", "null"]},
                    "voted_for": {"type": ["string", "null"]},
                },
                "required": ["juror", "voted_for"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["jury_votes"],
    "additionalProperties": False,
}

EPISODES_DETAIL_SCHEMA = {
    "type": "object",
    "properties": {
        "episode_details": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "episode_number": {"type": ["integer", "null"]},
                    "title": {"type": ["string", "null"]},
                    "air_date": {"type": ["string", "null"]},
                    "viewers_millions": {"type": ["number", "null"]},
                },
                "required": ["episode_number", "title", "air_date", "viewers_millions"],
                "additionalProperties": False,
            },
        },
    },
    "required": ["episode_details"],
    "additionalProperties": False,
}

TABLE_TYPE_SCHEMAS = {
    "contestants": ("contestants_normalize", CONTESTANTS_SCHEMA),
    "episodes": ("episodes_normalize", EPISODES_SCHEMA),
    "voting_history": ("voting_history_normalize", VOTING_HISTORY_SCHEMA),
    "jury_vote": ("jury_vote_normalize", JURY_VOTE_SCHEMA),
    "episodes_detail": ("episodes_detail_normalize", EPISODES_DETAIL_SCHEMA),
}


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
        "49": 49,
    }
    for subtitle, num in known.items():
        if subtitle.lower() in title.lower():
            return num

    nums = re.findall(r"(\d+)", title)
    for n in nums:
        val = int(n)
        if 1 <= val <= 49:
            return val

    return None


def normalize_table(table, season_title, smoke=False):
    table_type = table.get("table_type", "other")
    if table_type == "other":
        return None, None

    schema_entry = TABLE_TYPE_SCHEMAS.get(table_type)
    if schema_entry is None:
        log.warning("No strict schema for table type %s, skipping.", table_type)
        return table_type, None

    schema_name, schema = schema_entry
    col_mapping = table.get("column_mapping", {})
    mapping_info = json.dumps(col_mapping) if col_mapping else "no mapping"

    rows = table["rows"]
    if smoke and len(rows) > 20:
        rows = rows[:20]
        log.info("Smoke mode: truncated %s table from %d to 20 rows", table_type, len(table["rows"]))

    user_prompt = f"""Season: {season_title}
Table type: {table_type}
Column mapping: {mapping_info}
Columns: {table['columns']}
Rows:
{json.dumps(rows, ensure_ascii=False)}
"""

    try:
        result = groq_strict(NORMALIZE_SYSTEM, user_prompt, schema, schema_name=schema_name)
        return table_type, result
    except Exception as e:
        log.warning("Groq normalization failed for %s / %s: %s", season_title, table_type, e)
        return table_type, None


def ingest_season(season_title, season_number, tables_data, smoke=False):
    upsert_season({"title": season_title, "number": season_number})

    contestants_data = None
    episodes_data = None
    votes_data = None
    jury_data = None
    episode_details = None

    for table in tables_data:
        table_type = table.get("table_type", "other")

        if table_type == "voting_history" and "votes" in table:
            log.info("  Using pre-parsed voting data (%d votes) for %s", len(table["votes"]), season_title)
            votes_data = table["votes"]
            continue

        if table_type == "jury_vote" and "jury_votes" in table:
            log.info("  Using pre-parsed jury data (%d votes) for %s", len(table["jury_votes"]), season_title)
            jury_data = table["jury_votes"]
            continue

        table_type, normalized = normalize_table(table, season_title, smoke=smoke)
        if normalized is None:
            continue

        if table_type == "contestants":
            contestants_data = normalized.get("players", [])
        elif table_type == "episodes":
            episodes_data = normalized.get("episodes", [])
        elif table_type == "voting_history" and votes_data is None:
            votes_data = normalized.get("votes", [])
        elif table_type == "jury_vote" and jury_data is None:
            jury_data = normalized.get("jury_votes", [])
        elif table_type == "episodes_detail":
            episode_details = normalized.get("episode_details", [])

    tribes_seen = set()
    if contestants_data:
        log.info("  Ingesting %d contestants for %s", len(contestants_data), season_title)
        for p in contestants_data:
            name = p.get("name")
            if not name:
                continue
            upsert_player(name)

            exit_type = p.get("exit_type") or "voted_out"
            jury_member = p.get("jury_member")
            if jury_member is None:
                jury_member = False
            elif isinstance(jury_member, str):
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

            merged = p.get("merged_tribe")
            if merged and merged.lower() not in ("", "none", "null"):
                merged_key = f"merged:{merged}"
                if merged_key not in tribes_seen:
                    upsert_tribe(merged, season_number, "merged")
                    tribes_seen.add(merged_key)
                link_player_tribe(name, season_number, merged)

    if episodes_data:
        log.info("  Ingesting %d episodes for %s", len(episodes_data), season_title)
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

            elim_tribe = ep.get("eliminated_tribe")
            if elim_tribe and elim_tribe.lower() not in ("none", "null", ""):
                link_episode_tribe(season_number, ep_num, elim_tribe)

    if episode_details:
        log.info("  Ingesting %d episode details for %s", len(episode_details), season_title)
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
        log.info("  Ingesting %d votes for %s", len(votes_data), season_title)
        from collections import defaultdict
        tribal_attendees = defaultdict(set)
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
                tribal_attendees[ep_num].add(voter)

        eliminated_by_ep = {}
        if episodes_data:
            for ep in episodes_data:
                elim = ep.get("eliminated")
                ep_n = ep.get("episode_number")
                if elim and ep_n:
                    try:
                        eliminated_by_ep[int(ep_n)] = elim
                    except (ValueError, TypeError):
                        pass

        for ep_num, attendees in tribal_attendees.items():
            upsert_tribal_council(season_number, ep_num)
            for player in attendees:
                link_tribal_attendee(season_number, ep_num, player)
            elim = eliminated_by_ep.get(ep_num)
            if elim and elim not in attendees:
                link_tribal_attendee(season_number, ep_num, elim)
        log.info("  Created %d tribal councils for %s", len(tribal_attendees), season_title)

    if jury_data:
        log.info("  Ingesting %d jury votes for %s", len(jury_data), season_title)
        for jv in jury_data:
            juror = jv.get("juror")
            voted_for = jv.get("voted_for")
            if juror and voted_for:
                add_jury_vote(juror, voted_for, season_number)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--smoke", action="store_true", help="Smoke-test mode: truncate large tables to 20 rows")
    args = parser.parse_args()

    tables_dir = TABLES_DIR
    if not tables_dir.exists():
        log.error("Run 02_extract_tables.py first.")
        sys.exit(1)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    log.info("Setting up Neo4j constraints and indexes...")
    setup_constraints()

    mode_label = "smoke" if args.smoke else "full"
    log.info("Ingesting %d seasons into Neo4j (%s mode)...", len(manifest), mode_label)

    for season in tqdm(manifest, desc="Ingesting"):
        title = season["title"]
        season_number = extract_season_number(title)
        if season_number is None:
            log.warning("Could not determine season number for: %s, skipping.", title)
            continue

        safe_name = Path(season["html_path"]).stem
        table_path = tables_dir / f"{safe_name}.json"
        if not table_path.exists():
            log.warning("No table file for %s, skipping.", title)
            continue

        table_doc = json.loads(table_path.read_text(encoding="utf-8"))
        ingest_season(title, season_number, table_doc["tables"], smoke=args.smoke)

    counts = get_node_counts()
    log.info("Done. Node counts: %s", counts)


if __name__ == "__main__":
    main()
