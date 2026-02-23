import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.neo4j_client import (
    add_jury_vote,
    add_vote,
    clear_graph,
    get_node_counts,
    link_episode_eliminated,
    link_episode_immunity,
    link_episode_reward,
    link_player_tribe,
    link_tribal_attendee,
    run_query,
    setup_constraints,
    upsert_episode,
    upsert_player,
    upsert_player_season,
    upsert_season,
    upsert_tribal_council,
    upsert_tribe,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

MAX_SEASON = 49


def load_json(data_dir, filename):
    path = data_dir / filename
    data = json.loads(path.read_text(encoding="utf-8"))
    return [r for r in data if r.get("version") == "US" and (r.get("season") or 0) <= MAX_SEASON]


def build_name_lookup(castaways):
    lookup = {}
    for r in castaways:
        key = (r["season"], r["castaway"])
        lookup[key] = r.get("full_name", r["castaway"])
    return lookup


def resolve(lookup, season, short_name):
    return lookup.get((season, short_name), short_name)


def ingest_seasons(data_dir):
    raw = load_json(data_dir, "season_summary.json")
    log.info("Ingesting %d seasons...", len(raw))
    for r in tqdm(raw, desc="Seasons"):
        upsert_season({"title": r["season_name"], "number": r["season"]})
    return len(raw)


def ingest_players(data_dir):
    castaways = load_json(data_dir, "castaways.json")
    log.info("Ingesting %d player-seasons...", len(castaways))
    for r in tqdm(castaways, desc="Players"):
        full_name = r.get("full_name", r["castaway"])
        season = r["season"]

        upsert_player(full_name)

        hometown_parts = [r.get("city"), r.get("state")]
        hometown = ", ".join(p for p in hometown_parts if p) or None

        exit_type = r.get("result", "voted_out")
        if r.get("winner"):
            exit_type = "winner"
        elif r.get("finalist"):
            exit_type = "runner_up"

        props = {
            "player_name": full_name,
            "season_number": season,
            "age": r.get("age"),
            "hometown": hometown,
            "placement": str(r["place"]) if r.get("place") is not None else None,
            "day_out": r.get("day"),
            "exit_type": exit_type,
            "jury_member": bool(r.get("jury", False)),
        }
        upsert_player_season(full_name, season, props)
    return castaways


def ingest_episodes(data_dir):
    episodes = load_json(data_dir, "episodes.json")
    log.info("Ingesting %d episodes...", len(episodes))
    for r in tqdm(episodes, desc="Episodes"):
        season = r["season"]
        ep_num = r["episode"]
        viewers = r.get("viewers")
        if viewers is not None:
            try:
                viewers = float(viewers) / 1_000_000
            except (ValueError, TypeError):
                viewers = None

        props = {
            "season_number": season,
            "episode_number": ep_num,
            "title": r.get("episode_title"),
            "air_date": r.get("episode_date"),
            "viewers_millions": viewers,
        }
        upsert_episode(season, ep_num, props)
    return len(episodes)


def ingest_tribes(data_dir, name_lookup):
    tribe_mapping = load_json(data_dir, "tribe_mapping.json")
    log.info("Processing %d tribe assignments...", len(tribe_mapping))

    tribes_created = set()
    player_tribes = set()

    for r in tqdm(tribe_mapping, desc="Tribes"):
        season = r["season"]
        tribe = r.get("tribe")
        if not tribe:
            continue

        status = r.get("tribe_status", "Original")
        phase = "merged" if status == "Merged" else "premerge"

        tribe_key = (tribe, season)
        if tribe_key not in tribes_created:
            upsert_tribe(tribe, season, phase)
            tribes_created.add(tribe_key)

        full_name = resolve(name_lookup, season, r["castaway"])
        pt_key = (full_name, season, tribe)
        if pt_key not in player_tribes:
            link_player_tribe(full_name, season, tribe)
            player_tribes.add(pt_key)

    log.info("Created %d tribes, %d player-tribe links", len(tribes_created), len(player_tribes))


def ingest_votes(data_dir, name_lookup):
    votes = load_json(data_dir, "vote_history.json")
    log.info("Processing %d vote records...", len(votes))

    tribal_attendees = defaultdict(set)
    vote_count = 0
    skipped = 0

    for r in tqdm(votes, desc="Votes"):
        season = r["season"]
        episode = r["episode"]
        voter_short = r["castaway"]
        target_short = r.get("vote")

        if not target_short:
            skipped += 1
            continue

        voter_full = resolve(name_lookup, season, voter_short)
        target_full = resolve(name_lookup, season, target_short)

        add_vote(voter_full, target_full, season, episode)
        tribal_attendees[(season, episode)].add(voter_full)
        vote_count += 1

    log.info("Ingested %d votes (%d skipped, no target), %d tribal councils",
             vote_count, skipped, len(tribal_attendees))

    log.info("Creating tribal council nodes and attendee links...")
    for (season, episode), attendees in tqdm(tribal_attendees.items(), desc="Tribals"):
        upsert_tribal_council(season, episode)
        for player in attendees:
            link_tribal_attendee(season, episode, player)


def ingest_challenges(data_dir, name_lookup):
    results = load_json(data_dir, "challenge_results.json")

    imm_wins = [r for r in results if r.get("won_individual_immunity") == 1]
    rew_wins = [r for r in results if r.get("won_individual_reward") == 1]

    log.info("Ingesting %d individual immunity wins...", len(imm_wins))
    for r in tqdm(imm_wins, desc="Immunity"):
        full_name = resolve(name_lookup, r["season"], r["castaway"])
        link_episode_immunity(r["season"], r["episode"], full_name)

    log.info("Ingesting %d individual reward wins...", len(rew_wins))
    for r in tqdm(rew_wins, desc="Reward"):
        full_name = resolve(name_lookup, r["season"], r["castaway"])
        link_episode_reward(r["season"], r["episode"], full_name)


def ingest_eliminations(data_dir, name_lookup):
    boots = load_json(data_dir, "boot_order.json")
    log.info("Ingesting %d eliminations...", len(boots))
    for r in tqdm(boots, desc="Eliminations"):
        full_name = resolve(name_lookup, r["season"], r["castaway"])
        ep = r.get("episode")
        if ep is None:
            continue
        link_episode_eliminated(r["season"], ep, full_name)


def ingest_jury_votes(data_dir, name_lookup):
    jury = load_json(data_dir, "jury_votes.json")
    voted_for = [r for r in jury if r.get("vote") == 1]
    log.info("Ingesting %d jury votes (of %d total records)...", len(voted_for), len(jury))
    for r in tqdm(voted_for, desc="Jury"):
        juror_full = resolve(name_lookup, r["season"], r["castaway"])
        finalist_full = resolve(name_lookup, r["season"], r["finalist"])
        add_jury_vote(juror_full, finalist_full, r["season"])


def print_coverage():
    print("\n" + "=" * 50)
    print("COVERAGE REPORT")
    print("=" * 50)

    counts = get_node_counts()
    print("\nNode counts:")
    for label, cnt in sorted(counts.items()):
        print(f"  {label}: {cnt}")

    rel_queries = {
        "CAST_VOTE": "MATCH ()-[r:CAST_VOTE]->() RETURN count(r) AS cnt",
        "ATTENDED_TRIBAL": "MATCH ()-[r:ATTENDED_TRIBAL]->() RETURN count(r) AS cnt",
        "IMMUNITY_WON_BY": "MATCH ()-[r:IMMUNITY_WON_BY]->() RETURN count(r) AS cnt",
        "REWARD_WON_BY": "MATCH ()-[r:REWARD_WON_BY]->() RETURN count(r) AS cnt",
        "ELIMINATED": "MATCH ()-[r:ELIMINATED]->() RETURN count(r) AS cnt",
        "JURY_VOTE_FOR": "MATCH ()-[r:JURY_VOTE_FOR]->() RETURN count(r) AS cnt",
        "MEMBER_OF": "MATCH ()-[r:MEMBER_OF]->() RETURN count(r) AS cnt",
    }

    print("\nRelationship counts:")
    for rel_type, q in rel_queries.items():
        result = run_query(q)
        cnt = result[0]["cnt"] if result else 0
        print(f"  {rel_type}: {cnt}")

    season_coverage = {
        "CAST_VOTE": "MATCH (a:PlayerSeason)-[:CAST_VOTE]->() RETURN DISTINCT a.season_number AS s ORDER BY s",
        "IMMUNITY_WON_BY": "MATCH (e:Episode)-[:IMMUNITY_WON_BY]->() RETURN DISTINCT e.season_number AS s ORDER BY s",
        "REWARD_WON_BY": "MATCH (e:Episode)-[:REWARD_WON_BY]->() RETURN DISTINCT e.season_number AS s ORDER BY s",
        "ELIMINATED": "MATCH (e:Episode)-[:ELIMINATED]->() RETURN DISTINCT e.season_number AS s ORDER BY s",
        "JURY_VOTE_FOR": "MATCH (a:PlayerSeason)-[:JURY_VOTE_FOR]->() RETURN DISTINCT a.season_number AS s ORDER BY s",
    }

    print("\nPer-season coverage:")
    for rel_type, q in season_coverage.items():
        result = run_query(q)
        seasons = [r["s"] for r in result]
        missing = sorted(set(range(1, MAX_SEASON + 1)) - set(seasons))
        status = f"{len(seasons)}/{MAX_SEASON} seasons"
        if missing:
            status += f"  (missing: {missing})"
        print(f"  {rel_type}: {status}")


def main():
    parser = argparse.ArgumentParser(description="Rebuild Neo4j graph from survivoR dataset")
    parser.add_argument("--seasons", type=str, help="Comma-separated season numbers (skip clear_graph)")
    args = parser.parse_args()

    data_dir = Path(os.getenv("SURVIVOR_DATA_DIR", "data/survivoR"))
    if not data_dir.is_absolute():
        data_dir = Path(__file__).resolve().parent.parent / data_dir

    if not data_dir.exists():
        log.error("survivoR data directory not found: %s", data_dir)
        sys.exit(1)

    log.info("Loading survivoR data from %s", data_dir)

    if not args.seasons:
        log.info("Full rebuild: wiping existing graph...")
        clear_graph()

    setup_constraints()

    ingest_seasons(data_dir)
    castaways = ingest_players(data_dir)
    name_lookup = build_name_lookup(castaways)
    ingest_episodes(data_dir)
    ingest_tribes(data_dir, name_lookup)
    ingest_votes(data_dir, name_lookup)
    ingest_challenges(data_dir, name_lookup)
    ingest_eliminations(data_dir, name_lookup)
    ingest_jury_votes(data_dir, name_lookup)

    print_coverage()
    log.info("Done.")


if __name__ == "__main__":
    main()
