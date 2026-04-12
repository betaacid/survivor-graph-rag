import lib.wiki_fetcher as wf


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = ""

    def json(self):
        return self.payload

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise Exception(f"HTTP {self.status_code}")


def test_html_to_plain_text_removes_tables_and_navbox():
    html = """
    <html><body>
    <h1>Survivor: The Australian Outback</h1>
    <div class="navbox">ignore this nav</div>
    <table class="wikitable">
    <caption>Challenge winners and eliminations by episode</caption>
    <tr><th>No.</th><th>Title</th><th>Reward</th><th>Immunity</th><th>Eliminated</th></tr>
    <tr><td>1</td><td>Stranded</td><td>Ogakor</td><td>Kucha</td><td>Debb</td></tr>
    <tr><td>2</td><td>Suspicion</td><td>Ogakor</td><td>Kucha</td><td>Kel</td></tr>
    </table>
    <p>The season was filmed in Queensland.</p>
    </body></html>
    """
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
