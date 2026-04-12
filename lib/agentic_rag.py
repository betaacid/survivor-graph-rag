import json
import logging

from lib.agentic_tools import TOOLS, build_tool_descriptions, season_winner
from lib.graph_rag import run_text2cypher
from lib.llm import chat, chat_json, chat_with_tools

log = logging.getLogger(__name__)


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
Use search_chunks for narrative questions, descriptions, or background information \
that isn't captured in structured graph data (e.g. "What happened at the merge?", \
"Tell me about the controversies in Season 39", "What was the strategy?").
Make sure to pass the correct and complete arguments to the chosen tool."""


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
    response_msg = chat_with_tools(messages, build_tool_descriptions())

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
Only ask follow-up questions that can be answered from the existing Survivor graph and chunk data.
Do not ask for user clarification, alternate interpretations, timeframes, platforms, or external sources.
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
        raise RuntimeError(f"Agentic RAG failed during routing: {e}") from e

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
        for follow_up in follow_ups:
            try:
                fu_tool, fu_args, fu_cypher, fu_rows = route_question(follow_up)
                rows.extend(fu_rows)
                steps.append({
                    "stage": "critic_retry",
                    "question": follow_up,
                    "tool": fu_tool,
                    "args": fu_args,
                    "cypher": fu_cypher,
                    "rows_returned": len(fu_rows),
                })
            except Exception as e:
                steps.append({"stage": "critic_retry", "question": follow_up, "error": str(e)})

    results_str = _format_rows(rows)
    answer = chat(
        _ANSWER_PROMPT,
        f"Retrieved data:\n{results_str}\n\nQuestion: {question}",
    )

    return answer, steps
