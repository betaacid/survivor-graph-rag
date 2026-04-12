import logging

from lib.neo4j_client import run_query

log = logging.getLogger(__name__)


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
        raw = row["relType"]
        rtype = raw
        if raw.startswith(":"):
            rtype = raw[1:]
        rtype = rtype.strip("`")
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
