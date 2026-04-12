import logging

from lib.neo4j_client import get_driver

log = logging.getLogger(__name__)


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
