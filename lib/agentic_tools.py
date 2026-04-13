import re

from lib.neo4j_client import run_query, search_chunks_fulltext


def season_winner(season_number: int):
    cypher = (
        "MATCH (ps:PlayerSeason {season_number: $sn}) "
        "WHERE ps.exit_type = 'winner' "
        "RETURN ps.player_name AS winner, ps.season_number AS season"
    )
    return cypher, run_query(cypher, {"sn": season_number})


def player_seasons(player_name: str):
    cypher = (
        "MATCH (p:Player)-[:PLAYED_IN]->(ps:PlayerSeason) "
        "WHERE toLower(p.name) CONTAINS $name "
        "RETURN p.name AS player, ps.season_number AS season, "
        "ps.placement AS placement, ps.exit_type AS exit_type "
        "ORDER BY ps.season_number"
    )
    return cypher, run_query(cypher, {"name": player_name.lower()})


def season_tribes(season_number: int):
    cypher = (
        "MATCH (t:Tribe {season_number: $sn}) "
        "RETURN t.name AS tribe, t.phase AS phase ORDER BY t.phase"
    )
    return cypher, run_query(cypher, {"sn": season_number})


def top_immunity_winners(limit: int = 10):
    cypher = (
        "MATCH (e:Episode)-[:IMMUNITY_WON_BY]->(ps:PlayerSeason) "
        "RETURN ps.player_name AS player, count(e) AS wins "
        "ORDER BY wins DESC LIMIT $limit"
    )
    return cypher, run_query(cypher, {"limit": limit})


def top_reward_winners(limit: int = 10):
    cypher = (
        "MATCH (e:Episode)-[:REWARD_WON_BY]->(ps:PlayerSeason) "
        "RETURN ps.player_name AS player, count(e) AS wins "
        "ORDER BY wins DESC LIMIT $limit"
    )
    return cypher, run_query(cypher, {"limit": limit})


def jury_members(season_number: int):
    cypher = (
        "MATCH (ps:PlayerSeason {season_number: $sn}) "
        "WHERE ps.jury_member = true "
        "RETURN ps.player_name AS juror, ps.placement AS placement "
        "ORDER BY ps.placement"
    )
    return cypher, run_query(cypher, {"sn": season_number})


def elimination_by_episode():
    cypher = (
        "MATCH (e:Episode)-[:ELIMINATED]->(ps:PlayerSeason) "
        "RETURN e.episode_number AS episode, count(ps) AS eliminations "
        "ORDER BY eliminations DESC LIMIT 15"
    )
    return cypher, run_query(cypher)


def players_multiple_seasons(min_seasons: int = 2):
    cypher = (
        "MATCH (p:Player)-[:PLAYED_IN]->(ps:PlayerSeason) "
        "WITH p, count(ps) AS seasons "
        "WHERE seasons >= $min "
        "RETURN p.name AS player, seasons "
        "ORDER BY seasons DESC"
    )
    return cypher, run_query(cypher, {"min": min_seasons})


def multi_time_winners():
    cypher = (
        "MATCH (ps:PlayerSeason) WHERE ps.exit_type = 'winner' "
        "WITH ps.player_name AS player, "
        "collect(ps.season_number) AS winning_seasons, count(*) AS wins "
        "WHERE wins > 1 "
        "RETURN player, winning_seasons, wins ORDER BY wins DESC"
    )
    return cypher, run_query(cypher)


def back_to_back_winners():
    cypher = (
        "MATCH (p:Player)-[:PLAYED_IN]->(ps:PlayerSeason) "
        "WITH p.name AS player, ps ORDER BY ps.season_number "
        "WITH player, collect({season: ps.season_number, exit: ps.exit_type}) AS apps "
        "WHERE size(apps) >= 2 AND apps[0].exit = 'winner' AND apps[1].exit = 'winner' "
        "RETURN player, apps[0].season AS first_win, apps[1].season AS second_win"
    )
    return cypher, run_query(cypher)


def top_tribal_attendance(limit: int = 10):
    cypher = (
        "MATCH (ps:PlayerSeason)-[:ATTENDED_TRIBAL]->(tc:TribalCouncil) "
        "WITH ps.player_name AS player, count(tc) AS tribals_attended "
        "ORDER BY tribals_attended DESC "
        "RETURN player, tribals_attended LIMIT $limit"
    )
    return cypher, run_query(cypher, {"limit": limit})


def search_chunks(query: str, limit: int = 8):
    season_match = re.search(r"\b(?:survivor|season)\s+(\d{1,2})\b", query, re.IGNORECASE)
    if season_match:
        season_number = int(season_match.group(1))
        doc_id = f"wikipedia:Survivor_{season_number}"
        cypher = (
            "CALL db.index.fulltext.queryNodes('chunkTextIndex', $query) "
            "YIELD node, score "
            "WHERE node.doc_id = $doc_id "
            "   OR EXISTS { MATCH (node)-[:MENTIONS]->(:Season {number: $season_number}) } "
            "RETURN node.chunk_id AS chunk_id, node.text AS text, "
            "node.section AS section, node.doc_id AS doc_id, score "
            "ORDER BY score DESC "
            f"LIMIT {limit}"
        )
        results = run_query(cypher, {"query": query, "doc_id": doc_id, "season_number": season_number})
        return cypher, results

    results = search_chunks_fulltext(query, k=limit)
    cypher = (
        "CALL db.index.fulltext.queryNodes('chunkTextIndex', $query) "
        "YIELD node, score "
        "RETURN node.chunk_id, node.text, node.section, node.doc_id, score "
        f"ORDER BY score DESC LIMIT {limit}"
    )
    return cypher, results


TOOLS = {
    "season_winner": {
        "function": season_winner,
        "description": {
            "type": "function",
            "function": {
                "name": "season_winner",
                "description": "Get the winner of a specific Survivor season by season number.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "season_number": {"type": "integer", "description": "The season number (e.g. 41)"},
                    },
                    "required": ["season_number"],
                },
            },
        },
    },
    "player_seasons": {
        "function": player_seasons,
        "description": {
            "type": "function",
            "function": {
                "name": "player_seasons",
                "description": "Get all seasons a player competed in, including placement and exit type.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "player_name": {"type": "string", "description": "The player name (or partial name)"},
                    },
                    "required": ["player_name"],
                },
            },
        },
    },
    "season_tribes": {
        "function": season_tribes,
        "description": {
            "type": "function",
            "function": {
                "name": "season_tribes",
                "description": "Get all tribes in a specific Survivor season.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "season_number": {"type": "integer", "description": "The season number"},
                    },
                    "required": ["season_number"],
                },
            },
        },
    },
    "top_immunity_winners": {
        "function": top_immunity_winners,
        "description": {
            "type": "function",
            "function": {
                "name": "top_immunity_winners",
                "description": "Get the players with the most individual immunity challenge wins across all seasons.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "How many top players to return (default 10)"},
                    },
                    "required": [],
                },
            },
        },
    },
    "top_reward_winners": {
        "function": top_reward_winners,
        "description": {
            "type": "function",
            "function": {
                "name": "top_reward_winners",
                "description": "Get the players with the most reward challenge wins across all seasons.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "How many top players to return (default 10)"},
                    },
                    "required": [],
                },
            },
        },
    },
    "jury_members": {
        "function": jury_members,
        "description": {
            "type": "function",
            "function": {
                "name": "jury_members",
                "description": "Get all jury members for a specific Survivor season.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "season_number": {"type": "integer", "description": "The season number"},
                    },
                    "required": ["season_number"],
                },
            },
        },
    },
    "elimination_by_episode": {
        "function": elimination_by_episode,
        "description": {
            "type": "function",
            "function": {
                "name": "elimination_by_episode",
                "description": "Get the most common episode numbers for player eliminations across all seasons.",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
        },
    },
    "players_multiple_seasons": {
        "function": players_multiple_seasons,
        "description": {
            "type": "function",
            "function": {
                "name": "players_multiple_seasons",
                "description": "Get all players who competed in a minimum number of seasons.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "min_seasons": {
                            "type": "integer",
                            "description": "Minimum number of seasons played (default 2)",
                        },
                    },
                    "required": [],
                },
            },
        },
    },
    "multi_time_winners": {
        "function": multi_time_winners,
        "description": {
            "type": "function",
            "function": {
                "name": "multi_time_winners",
                "description": "Get all players who have won Survivor more than once, with the seasons they won.",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
        },
    },
    "back_to_back_winners": {
        "function": back_to_back_winners,
        "description": {
            "type": "function",
            "function": {
                "name": "back_to_back_winners",
                "description": (
                    "Get players who won on consecutive appearances (their first two times playing both resulted in wins). "
                    "Use for questions about 'back to back' winners or players who won every time they played."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
            },
        },
    },
    "top_tribal_attendance": {
        "function": top_tribal_attendance,
        "description": {
            "type": "function",
            "function": {
                "name": "top_tribal_attendance",
                "description": "Get players with the most total tribal council attendance across all seasons.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "How many top players to return (default 10)"},
                    },
                    "required": [],
                },
            },
        },
    },
    "search_chunks": {
        "function": search_chunks,
        "description": {
            "type": "function",
            "function": {
                "name": "search_chunks",
                "description": (
                    "Search narrative text chunks stored in the graph using full-text keyword search. "
                    "Use this for descriptions, controversies, strategies, "
                    "or any question about what happened that is not captured in structured data."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Search keywords or phrase"},
                        "limit": {"type": "integer", "description": "Max chunks to return (default 8)"},
                    },
                    "required": ["query"],
                },
            },
        },
    },
    "text2cypher": {
        "function": None,
        "description": {
            "type": "function",
            "function": {
                "name": "text2cypher",
                "description": (
                    "Generate and run a custom Cypher query against the Survivor graph database. "
                    "Use this as a fallback when none of the other specialized tools fit the question."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {"type": "string", "description": "The question to answer via Cypher"},
                    },
                    "required": ["question"],
                },
            },
        },
    },
}


def build_tool_descriptions():
    return [t["description"] for t in TOOLS.values()]
