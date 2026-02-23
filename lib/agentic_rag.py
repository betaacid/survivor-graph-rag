import json
import logging

from lib.llm import chat, chat_json, chat_with_tools
from lib.neo4j_client import run_query
from lib.query import run_text2cypher

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prebuilt Cypher query tools
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Tool registry  (OpenAI function-calling format + callable)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Question rewriter
# ---------------------------------------------------------------------------

_REWRITE_PROMPT = """\
You are an expert at rewriting user questions to be more atomic, specific, and \
easier to route to the right database query.
Rewrite the question so that it is self-contained and precise.
Only rephrase; do not ask for more information than the original question.
If the question is already clear and specific, keep it unchanged.
Return JSON: {"question": "the rewritten question"}"""


def rewrite_question(question: str) -> str:
    try:
        result = chat_json(_REWRITE_PROMPT, question)
        return result.get("question", question)
    except Exception:
        log.warning("Question rewrite failed, using original")
        return question


# ---------------------------------------------------------------------------
# Classifier / Router  (OpenAI tool-calling)
# ---------------------------------------------------------------------------

_ROUTER_PROMPT = """\
Your job is to choose the right tool to answer the user's Survivor TV show question.
Pick the most specific tool that fits. Only fall back to text2cypher when no \
specialized tool matches.
Make sure to pass the correct and complete arguments to the chosen tool."""


def _build_tool_descriptions():
    return [t["description"] for t in TOOLS.values()]


def _handle_tool_call(tool_call):
    name = tool_call.function.name
    args = json.loads(tool_call.function.arguments)
    tool_entry = TOOLS.get(name)
    if tool_entry is None:
        raise ValueError(f"Unknown tool: {name}")

    if name == "text2cypher":
        question = args.get("question", "")
        cypher, rows = run_text2cypher(question)
        return name, args, cypher, rows

    func = tool_entry["function"]
    cypher, rows = func(**args)
    return name, args, cypher, rows


def route_question(question: str):
    messages = [
        {"role": "system", "content": _ROUTER_PROMPT},
        {"role": "user", "content": f"The user question to find a tool to answer: '{question}'"},
    ]
    response_msg = chat_with_tools(messages, _build_tool_descriptions())

    if not response_msg.tool_calls:
        log.warning("Router returned no tool calls, falling back to text2cypher")
        cypher, rows = run_text2cypher(question)
        return "text2cypher", {"question": question}, cypher, rows

    tool_call = response_msg.tool_calls[0]
    return _handle_tool_call(tool_call)


# ---------------------------------------------------------------------------
# Answer critic
# ---------------------------------------------------------------------------

_CRITIC_PROMPT = """\
You are an expert at evaluating whether a question has been fully answered.
The user will provide an original question and retrieved data.
If the data is sufficient to answer the original question, return an empty list.
If information is missing, return a short list of follow-up questions to fill the gap.
All follow-up questions must be complete, atomic, and specific.
Return JSON: {"questions": ["question1", ...]}"""


def critique_answer(original_question: str, tool_name: str, rows: list) -> list[str]:
    data_summary = json.dumps(rows[:30], default=str)
    user_msg = (
        f"Original question: {original_question}\n\n"
        f"Tool used: {tool_name}\n"
        f"Data retrieved:\n{data_summary}"
    )
    try:
        result = chat_json(_CRITIC_PROMPT, user_msg)
        return result.get("questions", [])
    except Exception:
        log.warning("Answer critique failed, skipping")
        return []


# ---------------------------------------------------------------------------
# Final answer generation
# ---------------------------------------------------------------------------

_ANSWER_PROMPT = """\
You are a Survivor TV show expert. Answer the user's question using ONLY the \
retrieved data below. Do not supplement with your own knowledge.
If the data is insufficient, say so. Format numbers, lists, and tables clearly."""


def _format_rows(rows: list) -> str:
    if not rows:
        return "(no results)"
    parts = []
    for i, row in enumerate(rows[:50]):
        parts.append(f"Row {i+1}: {row}")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def query_agentic_rag(question: str):
    steps = []

    rewritten = rewrite_question(question)
    steps.append({"stage": "rewriter", "original": question, "rewritten": rewritten})

    try:
        tool_name, tool_args, cypher, rows = route_question(rewritten)
    except Exception as e:
        steps.append({"stage": "router", "error": str(e)})
        return f"Agentic RAG failed during routing: {e}", steps

    steps.append({
        "stage": "router",
        "tool": tool_name,
        "args": tool_args,
        "cypher": cypher,
        "rows_returned": len(rows),
    })

    follow_ups = critique_answer(question, tool_name, rows)
    steps.append({"stage": "critic", "follow_ups": follow_ups})

    if follow_ups:
        combined = " ".join(follow_ups)
        try:
            fu_tool, fu_args, fu_cypher, fu_rows = route_question(combined)
            rows = rows + fu_rows
            steps.append({
                "stage": "critic_retry",
                "tool": fu_tool,
                "args": fu_args,
                "cypher": fu_cypher,
                "rows_returned": len(fu_rows),
            })
        except Exception as e:
            steps.append({"stage": "critic_retry", "error": str(e)})

    results_str = _format_rows(rows)
    answer = chat(
        _ANSWER_PROMPT,
        f"Retrieved data:\n{results_str}\n\nQuestion: {question}",
    )

    return answer, steps
