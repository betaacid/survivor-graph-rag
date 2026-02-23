import argparse
import json
import logging
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

SURVIVOR_DATA_BASE = "https://raw.githubusercontent.com/doehm/survivoR/master/dev/json"

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


def load_json(filename):
    url = f"{SURVIVOR_DATA_BASE}/{filename}"
    with urllib.request.urlopen(url, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return [r for r in data if r.get("version") == "US" and (r.get("season") or 0) <= MAX_SEASON]


def build_name_lookup(castaways):
    lookup = {}
    for r in castaways:
        key = (r["season"], r["castaway"])
        lookup[key] = r.get("full_name", r["castaway"])
    return lookup


def resolve(lookup, season, short_name):
    return lookup.get((season, short_name), short_name)


def ingest_seasons(seasons):
    raw = load_json("season_summary.json")
    if seasons is not None:
        raw = [r for r in raw if r.get("season") in seasons]
    log.info("Ingesting %d seasons...", len(raw))
    for r in tqdm(raw, desc="Seasons"):
        upsert_season({"title": r["season_name"], "number": r["season"]})
    return len(raw)


def ingest_players(seasons):
    castaways = load_json("castaways.json")
    if seasons is not None:
        castaways = [r for r in castaways if r.get("season") in seasons]
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


def ingest_episodes(seasons):
    episodes = load_json("episodes.json")
    if seasons is not None:
        episodes = [r for r in episodes if r.get("season") in seasons]
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


def ingest_tribes(seasons, name_lookup):
    tribe_mapping = load_json("tribe_mapping.json")
    if seasons is not None:
        tribe_mapping = [r for r in tribe_mapping if r.get("season") in seasons]
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


def ingest_votes(seasons, name_lookup):
    votes = load_json("vote_history.json")
    if seasons is not None:
        votes = [r for r in votes if r.get("season") in seasons]
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


def ingest_challenges(seasons, name_lookup):
    results = load_json("challenge_results.json")
    if seasons is not None:
        results = [r for r in results if r.get("season") in seasons]

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


def ingest_eliminations(seasons, name_lookup):
    boots = load_json("boot_order.json")
    if seasons is not None:
        boots = [r for r in boots if r.get("season") in seasons]
    log.info("Ingesting %d eliminations...", len(boots))
    for r in tqdm(boots, desc="Eliminations"):
        full_name = resolve(name_lookup, r["season"], r["castaway"])
        ep = r.get("episode")
        if ep is None:
            continue
        link_episode_eliminated(r["season"], ep, full_name)


def ingest_jury_votes(seasons, name_lookup):
    jury = load_json("jury_votes.json")
    if seasons is not None:
        jury = [r for r in jury if r.get("season") in seasons]
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

    seasons = None
    if args.seasons:
        seasons = {int(s.strip()) for s in args.seasons.split(",") if s.strip()}
        seasons = {s for s in seasons if 1 <= s <= MAX_SEASON}

    log.info("Loading survivoR data from %s", SURVIVOR_DATA_BASE)

    if not args.seasons:
        log.info("Full rebuild: wiping existing graph...")
        clear_graph()

    setup_constraints()

    ingest_seasons(seasons)
    castaways = ingest_players(seasons)
    name_lookup = build_name_lookup(castaways)
    ingest_episodes(seasons)
    ingest_tribes(seasons, name_lookup)
    ingest_votes(seasons, name_lookup)
    ingest_challenges(seasons, name_lookup)
    ingest_eliminations(seasons, name_lookup)
    ingest_jury_votes(seasons, name_lookup)

    print_coverage()
    log.info("Done.")


if __name__ == "__main__":
    main()
