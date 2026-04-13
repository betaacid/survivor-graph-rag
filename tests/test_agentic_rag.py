from types import SimpleNamespace

import lib.agentic_rag as agentic_rag
import lib.agentic_tools as agentic_tools


def make_tool_call(name, arguments):
    return SimpleNamespace(function=SimpleNamespace(name=name, arguments=arguments))


def test_rewrite_question_falls_back_on_failure(monkeypatch):
    def fake_chat_json(system_prompt, user_prompt):
        raise Exception("boom")

    monkeypatch.setattr(agentic_rag, "chat_json", fake_chat_json)

    assert agentic_rag.rewrite_question("original question") == "original question"


def test_handle_tool_call_dispatches_prebuilt_tool(monkeypatch):
    monkeypatch.setattr(agentic_rag, "season_winner", lambda season_number: ("MATCH", [{"winner": "Dee"}]))
    tool_call = make_tool_call("season_winner", '{"season_number": 45}')
    original = agentic_rag.TOOLS["season_winner"]["function"]
    agentic_rag.TOOLS["season_winner"]["function"] = agentic_rag.season_winner

    try:
        name, args, cypher, rows = agentic_rag._handle_tool_call(tool_call)
    finally:
        agentic_rag.TOOLS["season_winner"]["function"] = original

    assert name == "season_winner"
    assert args == {"season_number": 45}
    assert cypher == "MATCH"
    assert rows == [{"winner": "Dee"}]


def test_route_question_falls_back_to_text2cypher(monkeypatch):
    monkeypatch.setattr(agentic_rag, "chat_with_tools", lambda messages, tools: SimpleNamespace(tool_calls=[]))
    monkeypatch.setattr(agentic_rag, "run_text2cypher", lambda question: ("MATCH fallback", [{"ok": True}]))

    name, args, cypher, rows = agentic_rag.route_question("fallback question")

    assert name == "text2cypher"
    assert args == {"question": "fallback question"}
    assert cypher == "MATCH fallback"
    assert rows == [{"ok": True}]


def test_critique_answer_returns_empty_list(monkeypatch):
    monkeypatch.setattr(agentic_rag, "chat_json", lambda system_prompt, user_prompt: {"questions": []})

    assert agentic_rag.critique_answer("question", "tool", [{"a": 1}]) == []


def test_query_agentic_rag_full_flow(monkeypatch):
    monkeypatch.setattr(agentic_rag, "rewrite_question", lambda question: "rewritten")
    routes = iter([
        ("season_winner", {"season_number": 45}, "MATCH 1", [{"winner": "Dee"}]),
        ("jury_members", {"season_number": 45}, "MATCH 2", [{"juror": "Austin"}]),
    ])
    monkeypatch.setattr(agentic_rag, "route_question", lambda question: next(routes))
    monkeypatch.setattr(agentic_rag, "critique_answer", lambda question, tool_name, rows: ["Who were the jury members in season 45?"])
    monkeypatch.setattr(agentic_rag, "chat", lambda system_prompt, user_prompt: "final answer")

    answer, steps = agentic_rag.query_agentic_rag("Who won Survivor 45 and who were the jury members?")

    assert answer == "final answer"
    assert steps[0]["stage"] == "rewriter"
    assert steps[1]["stage"] == "router"
    assert steps[2]["stage"] == "critic"
    assert steps[3]["stage"] == "critic_retry"
    assert steps[3]["question"] == "Who were the jury members in season 45?"


def test_handle_tool_call_dispatches_multi_time_winners(monkeypatch):
    monkeypatch.setattr(
        agentic_tools, "multi_time_winners",
        lambda: ("MATCH multi", [{"player": "Sandra Diaz-Twine", "winning_seasons": [7, 20], "wins": 2}]),
    )
    original = agentic_rag.TOOLS["multi_time_winners"]["function"]
    agentic_rag.TOOLS["multi_time_winners"]["function"] = agentic_tools.multi_time_winners

    try:
        name, args, cypher, rows = agentic_rag._handle_tool_call(
            make_tool_call("multi_time_winners", "{}"),
        )
    finally:
        agentic_rag.TOOLS["multi_time_winners"]["function"] = original

    assert name == "multi_time_winners"
    assert rows[0]["player"] == "Sandra Diaz-Twine"


def test_handle_tool_call_dispatches_back_to_back_winners(monkeypatch):
    monkeypatch.setattr(
        agentic_tools, "back_to_back_winners",
        lambda: ("MATCH b2b", [{"player": "Sandra Diaz-Twine", "first_win": 7, "second_win": 20}]),
    )
    original = agentic_rag.TOOLS["back_to_back_winners"]["function"]
    agentic_rag.TOOLS["back_to_back_winners"]["function"] = agentic_tools.back_to_back_winners

    try:
        name, args, cypher, rows = agentic_rag._handle_tool_call(
            make_tool_call("back_to_back_winners", "{}"),
        )
    finally:
        agentic_rag.TOOLS["back_to_back_winners"]["function"] = original

    assert name == "back_to_back_winners"
    assert rows[0]["first_win"] == 7
    assert rows[0]["second_win"] == 20


def test_handle_tool_call_dispatches_top_tribal_attendance(monkeypatch):
    monkeypatch.setattr(
        agentic_tools, "top_tribal_attendance",
        lambda limit=10: ("MATCH tribal", [{"player": "Someone", "tribals_attended": 50}]),
    )
    original = agentic_rag.TOOLS["top_tribal_attendance"]["function"]
    agentic_rag.TOOLS["top_tribal_attendance"]["function"] = agentic_tools.top_tribal_attendance

    try:
        name, args, cypher, rows = agentic_rag._handle_tool_call(
            make_tool_call("top_tribal_attendance", '{"limit": 10}'),
        )
    finally:
        agentic_rag.TOOLS["top_tribal_attendance"]["function"] = original

    assert name == "top_tribal_attendance"
    assert rows[0]["tribals_attended"] == 50


def test_search_chunks_anchors_numeric_season_queries(monkeypatch):
    captured = {}

    def fake_run_query(cypher, params):
        captured["cypher"] = cypher
        captured["params"] = params
        return [{"chunk_id": "wikipedia:Survivor_48#0001"}]

    monkeypatch.setattr(agentic_tools, "run_query", fake_run_query)

    cypher, rows = agentic_tools.search_chunks("What was the overall fan reception of Survivor 48?", limit=5)

    assert "node.doc_id = $doc_id" in cypher
    assert captured["params"]["season_number"] == 48
    assert captured["params"]["doc_id"] == "wikipedia:Survivor_48"
    assert rows == [{"chunk_id": "wikipedia:Survivor_48#0001"}]


def test_multi_time_winners_cypher_groups_by_player(monkeypatch):
    captured = {}

    def fake_run_query(cypher, params=None):
        captured["cypher"] = cypher
        return [{"player": "Sandra Diaz-Twine", "winning_seasons": [7, 20], "wins": 2}]

    monkeypatch.setattr(agentic_tools, "run_query", fake_run_query)
    cypher, rows = agentic_tools.multi_time_winners()

    assert "player_name AS player" in cypher
    assert "wins > 1" in cypher
    assert rows[0]["wins"] == 2


def test_back_to_back_winners_checks_appearance_order(monkeypatch):
    captured = {}

    def fake_run_query(cypher, params=None):
        captured["cypher"] = cypher
        return [{"player": "Sandra Diaz-Twine", "first_win": 7, "second_win": 20}]

    monkeypatch.setattr(agentic_tools, "run_query", fake_run_query)
    cypher, rows = agentic_tools.back_to_back_winners()

    assert "apps[0].exit = 'winner'" in cypher
    assert "apps[1].exit = 'winner'" in cypher
    assert "ORDER BY ps.season_number" in cypher


def test_top_tribal_attendance_aggregates_across_seasons(monkeypatch):
    captured = {}

    def fake_run_query(cypher, params=None):
        captured["cypher"] = cypher
        captured["params"] = params
        return [{"player": "Someone", "tribals_attended": 50}]

    monkeypatch.setattr(agentic_tools, "run_query", fake_run_query)
    cypher, rows = agentic_tools.top_tribal_attendance(limit=5)

    assert "player_name AS player" in cypher
    assert "count(tc) AS tribals_attended" in cypher
    assert captured["params"]["limit"] == 5
