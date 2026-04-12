import pytest

import lib.graph_rag as graph_rag


def test_clean_cypher_strips_wrappers():
    assert graph_rag.clean_cypher("```cypher\nMATCH (n) RETURN n\n```") == "MATCH (n) RETURN n"
    assert graph_rag.clean_cypher("cypher MATCH (n) RETURN n") == "MATCH (n) RETURN n"
    assert graph_rag.clean_cypher("`MATCH (n) RETURN n`") == "MATCH (n) RETURN n"


def test_run_text2cypher_retries_after_query_error(monkeypatch):
    prompts = []
    chats = iter([
        "MATCH (n) RETURN broken",
        "MATCH (n) RETURN n LIMIT 1",
    ])
    calls = {"count": 0}

    def fake_chat(system_prompt, user_prompt):
        prompts.append(user_prompt)
        return next(chats)

    def fake_run_query(cypher):
        calls["count"] += 1
        if calls["count"] == 1:
            raise Exception("bad query")
        return [{"name": "ok"}]

    monkeypatch.setattr(graph_rag, "chat", fake_chat)
    monkeypatch.setattr(graph_rag, "run_query", fake_run_query)
    monkeypatch.setattr(graph_rag, "get_graph_schema", lambda: "schema")

    cypher, rows = graph_rag.run_text2cypher("Who won?")

    assert cypher == "MATCH (n) RETURN n LIMIT 1"
    assert rows == [{"name": "ok"}]
    assert len(prompts) == 2
    assert "Fix the query." in prompts[1]


def test_run_text2cypher_retries_empty_once(monkeypatch):
    prompts = []
    chats = iter([
        "MATCH (n) WHERE n.name = 'missing' RETURN n",
        "MATCH (n) RETURN n LIMIT 1",
    ])
    queries = []

    def fake_chat(system_prompt, user_prompt):
        prompts.append(user_prompt)
        return next(chats)

    def fake_run_query(cypher):
        queries.append(cypher)
        if len(queries) == 1:
            return []
        return []

    monkeypatch.setattr(graph_rag, "chat", fake_chat)
    monkeypatch.setattr(graph_rag, "run_query", fake_run_query)
    monkeypatch.setattr(graph_rag, "get_graph_schema", lambda: "schema")

    cypher, rows = graph_rag.run_text2cypher("Who was never there?")

    assert cypher == "MATCH (n) RETURN n LIMIT 1"
    assert rows == []
    assert len(prompts) == 2
    assert "genuinely empty" in prompts[1]
    assert len(queries) == 2


def test_query_graph_rag_raises_on_text2cypher_failure(monkeypatch):
    def fake_run_text2cypher(question):
        raise RuntimeError("failed")

    monkeypatch.setattr(graph_rag, "run_text2cypher", fake_run_text2cypher)

    with pytest.raises(RuntimeError, match="failed"):
        graph_rag.query_graph_rag("question")
