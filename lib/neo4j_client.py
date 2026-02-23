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
        MATCH (voter:PlayerSeason {season_number: $sn})
        WHERE voter.player_name = $voter OR voter.player_name STARTS WITH $voter
        WITH voter LIMIT 1
        MATCH (target:PlayerSeason {season_number: $sn})
        WHERE target.player_name = $target OR target.player_name STARTS WITH $target
        WITH voter, target LIMIT 1
        MERGE (voter)-[v:CAST_VOTE {episode_number: $ep}]->(target)
    """, {"voter": voter_name, "target": target_name, "sn": season_number, "ep": episode_number})


def upsert_tribal_council(season_number, episode_number):
    run_write("""
        MERGE (tc:TribalCouncil {season_number: $sn, episode_number: $ep})
        WITH tc
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MERGE (e)-[:HAS_TRIBAL]->(tc)
    """, {"sn": season_number, "ep": episode_number})


def link_tribal_attendee(season_number, episode_number, player_name):
    run_write("""
        MATCH (ps:PlayerSeason {season_number: $sn})
        WHERE ps.player_name = $name OR ps.player_name STARTS WITH $name
        WITH ps LIMIT 1
        MATCH (tc:TribalCouncil {season_number: $sn, episode_number: $ep})
        MERGE (ps)-[:ATTENDED_TRIBAL]->(tc)
    """, {"name": player_name, "sn": season_number, "ep": episode_number})


def add_jury_vote(juror_name, voted_for_name, season_number):
    run_write("""
        MATCH (juror:PlayerSeason {season_number: $sn})
        WHERE juror.player_name = $juror OR juror.player_name STARTS WITH $juror
        MATCH (winner:PlayerSeason {season_number: $sn})
        WHERE winner.player_name = $voted_for OR winner.player_name STARTS WITH $voted_for
        WITH juror, winner LIMIT 1
        MERGE (juror)-[:JURY_VOTE_FOR]->(winner)
    """, {"juror": juror_name, "voted_for": voted_for_name, "sn": season_number})


def link_episode_tribe(season_number, episode_number, tribe_name):
    run_write("""
        MATCH (e:Episode {season_number: $sn, episode_number: $ep})
        MATCH (t:Tribe {name: $tribe, season_number: $sn})
        MERGE (e)-[:TRIBAL_COUNCIL_FOR]->(t)
    """, {"sn": season_number, "ep": episode_number, "tribe": tribe_name})


def get_graph_schema():
    try:
        return _schema_via_apoc()
    except Exception:
        log.debug("APOC not available, falling back to manual schema inference")
        return _schema_manual()


def _schema_via_apoc():
    node_props_raw = run_query("""
        CALL apoc.meta.data()
        YIELD label, other, elementType, type, property
        WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
        WITH label AS nodeLabels, collect({property: property, type: type}) AS properties
        RETURN {labels: nodeLabels, properties: properties} AS output
    """)
    rel_props_raw = run_query("""
        CALL apoc.meta.data()
        YIELD label, other, elementType, type, property
        WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
        WITH label AS relType, collect({property: property, type: type}) AS properties
        RETURN {type: relType, properties: properties} AS output
    """)
    rels_raw = run_query("""
        CALL apoc.meta.data()
        YIELD label, other, elementType, type, property
        WHERE type = "RELATIONSHIP" AND elementType = "node"
        UNWIND other AS other_node
        RETURN {start: label, type: property, end: toString(other_node)} AS output
    """)
    return _format_schema(
        node_props={r["output"]["labels"]: r["output"]["properties"] for r in node_props_raw},
        rel_props={r["output"]["type"]: r["output"]["properties"] for r in rel_props_raw},
        relationships=[r["output"] for r in rels_raw],
    )


def _schema_manual():
    node_rows = run_query("""
        CALL db.schema.nodeTypeProperties()
        YIELD nodeLabels, propertyName, propertyTypes
        RETURN nodeLabels, propertyName, propertyTypes
    """)
    node_props = {}
    for row in node_rows:
        for label in row["nodeLabels"]:
            node_props.setdefault(label, [])
            if row["propertyName"]:
                ptype = row["propertyTypes"][0] if row["propertyTypes"] else "STRING"
                node_props[label].append({"property": row["propertyName"], "type": ptype})

    rel_rows = run_query("""
        CALL db.schema.relTypeProperties()
        YIELD relType, propertyName, propertyTypes
        RETURN relType, propertyName, propertyTypes
    """)
    rel_props = {}
    for row in rel_rows:
        rtype = row["relType"].strip(":`")
        rel_props.setdefault(rtype, [])
        if row["propertyName"]:
            ptype = row["propertyTypes"][0] if row["propertyTypes"] else "STRING"
            rel_props[rtype].append({"property": row["propertyName"], "type": ptype})

    vis = run_query("CALL db.schema.visualization()")
    relationships = []
    if vis:
        for row in vis:
            for rel in row.get("relationships", []):
                relationships.append({
                    "start": list(rel.start_node.labels)[0],
                    "type": rel.type,
                    "end": list(rel.end_node.labels)[0],
                })

    return _format_schema(node_props, rel_props, relationships)


def _format_schema(node_props, rel_props, relationships):
    def _fmt(props):
        return ", ".join(f"{p['property']}: {p['type']}" for p in props)

    lines = ["Node labels and properties:"]
    for label, props in node_props.items():
        lines.append(f"  {label} {{{_fmt(props)}}}")

    lines.append("Relationship types and properties:")
    for rtype, props in rel_props.items():
        detail = f" {{{_fmt(props)}}}" if props else ""
        lines.append(f"  {rtype}{detail}")

    lines.append("The relationships:")
    for r in relationships:
        lines.append(f"  (:{r['start']})-[:{r['type']}]->(:{r['end']})")

    return "\n".join(lines)


def run_query_graph(cypher, params=None):
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, params or {})
        nodes = {}
        edges = []
        for record in result:
            for value in record.values():
                _collect_graph_objects(value, nodes, edges)
        return list(nodes.values()), edges


def _collect_graph_objects(value, nodes, edges):
    from neo4j.graph import Node as Neo4jNode, Relationship as Neo4jRelationship, Path as Neo4jPath
    if isinstance(value, Neo4jNode):
        if value.element_id not in nodes:
            nodes[value.element_id] = {
                "id": value.element_id,
                "labels": list(value.labels),
                "props": dict(value),
            }
    elif isinstance(value, Neo4jRelationship):
        edges.append({
            "source": value.start_node.element_id,
            "target": value.end_node.element_id,
            "type": value.type,
            "props": dict(value),
        })
        for node in (value.start_node, value.end_node):
            if node.element_id not in nodes:
                nodes[node.element_id] = {
                    "id": node.element_id,
                    "labels": list(node.labels),
                    "props": dict(node),
                }
    elif isinstance(value, Neo4jPath):
        for node in value.nodes:
            _collect_graph_objects(node, nodes, edges)
        for rel in value.relationships:
            _collect_graph_objects(rel, nodes, edges)
    elif isinstance(value, list):
        for item in value:
            _collect_graph_objects(item, nodes, edges)


def fetch_subgraph_for_results(graph_rows, limit=100):
    names = set()
    numbers = set()
    for row in graph_rows:
        for key, val in row.items():
            if val is None:
                continue
            k = key.lower()
            if isinstance(val, str) and len(val) > 1:
                names.add(val)
            elif isinstance(val, (int, float)) and ("season" in k or "number" in k):
                numbers.add(int(val))

    if not names and not numbers:
        return [], []

    params = {"names": list(names), "numbers": list(numbers), "limit": limit}
    cypher = """
        MATCH (a)-[r]->(b)
        WHERE a.player_name IN $names OR a.name IN $names OR a.title IN $names
           OR a.number IN $numbers OR a.season_number IN $numbers
           OR b.player_name IN $names OR b.name IN $names OR b.title IN $names
           OR b.number IN $numbers OR b.season_number IN $numbers
        RETURN a, r, b
        LIMIT $limit
    """
    try:
        return run_query_graph(cypher, params)
    except Exception as e:
        log.warning("Subgraph fetch failed: %s", e)
        return [], []


def get_node_counts():
    result = run_query("""
        CALL () {
            MATCH (s:Season) RETURN 'Season' AS label, count(s) AS cnt
            UNION ALL
            MATCH (p:Player) RETURN 'Player' AS label, count(p) AS cnt
            UNION ALL
            MATCH (ps:PlayerSeason) RETURN 'PlayerSeason' AS label, count(ps) AS cnt
            UNION ALL
            MATCH (e:Episode) RETURN 'Episode' AS label, count(e) AS cnt
            UNION ALL
            MATCH (t:Tribe) RETURN 'Tribe' AS label, count(t) AS cnt
            UNION ALL
            MATCH (tc:TribalCouncil) RETURN 'TribalCouncil' AS label, count(tc) AS cnt
        }
        RETURN label, cnt
    """)
    return {r["label"]: r["cnt"] for r in result}
