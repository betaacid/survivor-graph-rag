import lib.traditional_rag as traditional_rag


def test_query_traditional_rag_builds_context_and_returns_chunks(monkeypatch):
    captured = {}

    def fake_embed_query(question):
        captured["question"] = question
        return [0.1, 0.2]

    def fake_search_similar(embedding, top_k=8):
        captured["embedding"] = embedding
        captured["top_k"] = top_k
        return [
            {"season_title": "Survivor: Borneo", "content": "Richard Hatch won.", "similarity": 0.91},
            {"season_title": "Survivor 45", "content": "Dee played a strong game.", "similarity": 0.77},
        ]

    def fake_chat(system_prompt, user_prompt):
        captured["system_prompt"] = system_prompt
        captured["user_prompt"] = user_prompt
        return "answer"

    monkeypatch.setattr(traditional_rag, "embed_query", fake_embed_query)
    monkeypatch.setattr(traditional_rag, "search_similar", fake_search_similar)
    monkeypatch.setattr(traditional_rag, "chat", fake_chat)

    answer, chunks = traditional_rag.query_traditional_rag("Who won the first season?")

    assert answer == "answer"
    assert len(chunks) == 2
    assert captured["question"] == "Who won the first season?"
    assert captured["embedding"] == [0.1, 0.2]
    assert captured["top_k"] == 6
    assert "Richard Hatch won." in captured["user_prompt"]
    assert "Dee played a strong game." in captured["user_prompt"]
    assert "Question: Who won the first season?" in captured["user_prompt"]
