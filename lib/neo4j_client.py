import logging
import os

from neo4j import GraphDatabase

_driver = None
log = logging.getLogger(__name__)


def get_driver():
    global _driver
    if _driver is None:
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        log.debug("Connecting to Neo4j: %s", uri)
        _driver = GraphDatabase.driver(
            uri,
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
    log.info("Setting up Neo4j constraints and indexes...")
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Season) REQUIRE s.title IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tribe) REQUIRE (t.name, t.season_number) IS UNIQUE",
    ]
    for c in constraints:
        try:
            run_write(c)
        except Exception as e:
            log.debug("Constraint (may already exist): %s", e)

    indexes = [
        "CREATE INDEX IF NOT EXISTS FOR (e:Episode) ON (e.season_number, e.episode_number)",
        "CREATE INDEX IF NOT EXISTS FOR (ps:PlayerSeason) ON (ps.player_name, ps.season_number)",
        "CREATE INDEX IF NOT EXISTS FOR (tc:TribalCouncil) ON (tc.season_number, tc.episode_number)",
    ]
    for idx in indexes:
        try:
            run_write(idx)
        except Exception as e:
            log.debug("Index (may already exist): %s", e)


def setup_document_constraints():
    log.info("Setting up Document/Chunk constraints and indexes...")
    stmts = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.doc_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE",
        "CREATE FULLTEXT INDEX chunkTextIndex IF NOT EXISTS FOR (c:Chunk) ON EACH [c.text]",
    ]
    for s in stmts:
        try:
            run_write(s)
        except Exception as e:
            log.debug("Document/Chunk schema (may already exist): %s", e)


def search_chunks_fulltext(query, k=8):
    return run_query(
        """
        CALL db.index.fulltext.queryNodes('chunkTextIndex', $query)
        YIELD node, score
        RETURN node.chunk_id AS chunk_id, node.text AS text,
               node.section AS section, node.doc_id AS doc_id, score
        ORDER BY score DESC
        LIMIT $k
        """,
        {"query": query, "k": k},
    )


def clear_graph():
    run_write("MATCH (n) DETACH DELETE n")


def get_node_counts():
    labels = {row["label"] for row in run_query("CALL db.labels() YIELD label RETURN label")}
    queries = {
        "Season": "MATCH (n:Season) RETURN count(n) AS cnt",
        "Player": "MATCH (n:Player) RETURN count(n) AS cnt",
        "PlayerSeason": "MATCH (n:PlayerSeason) RETURN count(n) AS cnt",
        "Episode": "MATCH (n:Episode) RETURN count(n) AS cnt",
        "Tribe": "MATCH (n:Tribe) RETURN count(n) AS cnt",
        "TribalCouncil": "MATCH (n:TribalCouncil) RETURN count(n) AS cnt",
        "Document": "MATCH (n:Document) RETURN count(n) AS cnt",
        "Chunk": "MATCH (n:Chunk) RETURN count(n) AS cnt",
    }
    counts = {}
    for label, query in queries.items():
        if label not in labels:
            counts[label] = 0
            continue
        result = run_query(query)
        counts[label] = result[0]["cnt"] if result else 0
    return counts
