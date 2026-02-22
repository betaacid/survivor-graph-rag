import json
from pathlib import Path

import lib.wiki_fetcher as wf


class DummyResponse:
    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


def test_html_to_plain_text_removes_tables_and_navbox():
    html = (Path(__file__).parent / "fixtures" / "sample_season.html").read_text(encoding="utf-8")
    text = wf.html_to_plain_text(html)
    assert "Challenge winners" not in text
    assert "ignore this nav" not in text
    assert "filmed in Queensland" in text


def test_get_season_titles_filters_non_season_pages(monkeypatch):
    payload = {
        "query": {
            "categorymembers": [
                {"title": "Survivor: Borneo"},
                {"title": "Survivor season rankings"},
                {"title": "List of Survivor contestants"},
                {"title": "Survivor: The Australian Outback"}
            ]
        }
    }

    def fake_get(*args, **kwargs):
        return DummyResponse(payload)

    monkeypatch.setattr(wf.requests, "get", fake_get)
    titles = wf.get_season_titles()
    assert titles == ["Survivor: Borneo", "Survivor: The Australian Outback"]


def test_fetch_parsed_html_returns_pageid_and_html(monkeypatch):
    payload = {
        "parse": {
            "pageid": 123,
            "text": {"*": "<div>ok</div>"}
        }
    }

    def fake_get(*args, **kwargs):
        return DummyResponse(payload)

    monkeypatch.setattr(wf.requests, "get", fake_get)
    pageid, html = wf.fetch_parsed_html("Survivor: Borneo")
    assert pageid == 123
    assert html == "<div>ok</div>"
