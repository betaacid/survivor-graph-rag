import os

from neo4j import GraphDatabase

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            os.getenv("NEO4J_URI", "bolt://localhost:7687"),
            auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "survivor")),
        )
    return _driver


def run_query(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return [dict(record) for record in result]


def run_write(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        session.run(cypher, params or {})


def setup_constraints():
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.title IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tribe) REQUIRE (t.name, t.season_number) IS UNIQUE",
    ]
    for c in constraints:
        try:
            run_write(c)
        except Exception:
            pass

    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (e:Episode) ON (e.season_number, e.episode_number)",
        "CREATE INDEX IF NOT EXISTS FOR (ps:PlayerSeason) ON (ps.player_name, ps.season_number)",
    ]
    for idx in indexes:
        try:
            run_write(idx)
        except Exception:
            pass


def clear_graph():
    run_write("MATCH (n) DETACH DELETE n")


def upsert_season(props):
    run_write("""
        MERGE (s:Season {title: $title})
        SET s += $props
    """, {"title": props["title"], "props": props})


def upsert_player(name):
    run_write("MERGE (p:Player {name: $name})", {"name": name})


def upsert_player_season(player_name, season_number, props):
    run_write("""
        MERGE (ps:PlayerSeason {player_name: $player_name, season_number: $season_number})
        SET ps += $props
        WITH ps
        MERGE (p:Player {name: $player_name})
        MERGE (p)-[:PLAYED_IN]->(ps)
        WITH ps
        MATCH (s:Season {number: $season_number})
        MERGE (ps)-[:IN_SEASON]->(s)
    """, {"player_name": player_name, "season_number": season_number, "props": props})


def upsert_tribe(name, season_number, phase="premerge"):
    run_write("""
        MERGE (t:Tribe {name: $name, season_number: $season_number})
        SET t.phase = $phase
        WITH t
        MATCH (s:Season {number: $season_number})
        MERGE (s)-[:HAS_TRIBE]->(t)
    """, {"name": name, "season_number": season_number, "phase": phase})


def link_player_tribe(player_name, season_number, tribe_name):
    run_write("""
        MATCH (ps:PlayerSeason {player_name: $player_name, season_number: $season_number})
        MATCH (t:Tribe {name: $tribe_name, season_number: $season_number})
        MERGE (ps)-[:MEMBER_OF]->(t)
    """, {"player_name": player_name, "season_number": season_number, "tribe_name": tribe_name})


def upsert_episode(season_number, episode_number, props):
    run_write("""
        MERGE (e:Episode {season_number: $season_number, episode_number: $ep_num})
        SET e += $props
        WITH e
        MATCH (s:Season {number: $season_number})
        MERGE (s)-[:HAS_EPISODE]->(e)
    """, {"season_number": season_number, "ep_num": episode_number, "props": props})


def link_episode_immunity(season_number, episode_number, winner_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:IMMUNITY_WON_BY]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": winner_name})


def link_episode_reward(season_number, episode_number, winner_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:REWARD_WON_BY]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": winner_name})


def link_episode_eliminated(season_number, episode_number, player_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (ps:PlayerSeason {player_name: $name, season_number: $sn})
        MERGE (e)-[:ELIMINATED]->(ps)
    """, {"sn": season_number, "ep": episode_number, "name": player_name})


def add_vote(voter_name, target_name, season_number, episode_number):
    run_write("""
        MATCH (voter:PlayerSeason {player_name: $voter, season_number: $sn})
        MATCH (target:PlayerSeason {player_name: $target, season_number: $sn})
        MERGE (voter)-[v:CAST_VOTE {episode_number: $ep}]->(target)
    """, {"voter": voter_name, "target": target_name, "sn": season_number, "ep": episode_number})


def get_node_counts():
    result = run_query("""
        CALL {
            MATCH (s:Season) RETURN 'Season' AS label, count(s) AS cnt
            UNION ALL
            MATCH (p:Player) RETURN 'Player' AS label, count(p) AS cnt
            UNION ALL
            MATCH (ps:PlayerSeason) RETURN 'PlayerSeason' AS label, count(ps) AS cnt
            UNION ALL
            MATCH (e:Episode) RETURN 'Episode' AS label, count(e) AS cnt
            UNION ALL
            MATCH (t:Tribe) RETURN 'Tribe' AS label, count(t) AS cnt
        }
        RETURN label, cnt
    """)
    return {r["label"]: r["cnt"] for r in result}
