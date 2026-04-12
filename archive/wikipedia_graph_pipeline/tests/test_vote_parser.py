from pathlib import Path

import pytest

from lib.vote_parser import parse_jury_vote, parse_voting_history

VOTING_BASIC_HTML = """
<html><body>
<h2 id="Voting_history">Voting history</h2>
<table class="wikitable nowrap" width="100%" style="text-align:center;">
<tbody>
<tr><th></th><th colspan="2">Original tribes</th></tr>
<tr><th>Episode</th><th>1</th><th>2</th></tr>
<tr><th>Day</th><td>3</td><td>6</td></tr>
<tr><th>Eliminated</th><td>Alice</td><td>Bob</td></tr>
<tr><td bgcolor="black" colspan="100"></td></tr>
<tr><th>Voter</th><th colspan="2">Vote</th></tr>
<tr><th>Charlie</th><td>Alice</td><td>Bob</td></tr>
<tr><th>Diana</th><td>Alice</td><td></td></tr>
<tr><th>Eve</th><td></td><td>Bob</td></tr>
</tbody></table>
</body></html>
"""

VOTING_COLSPAN_HTML = """
<html><body>
<h2 id="Voting_history">Voting history</h2>
<table class="wikitable nowrap" style="text-align:center;">
<tbody>
<tr><th></th><th colspan="3">Merged tribe</th></tr>
<tr><th>Episode</th><th colspan="2">5</th><th>6</th></tr>
<tr><th>Day</th><td colspan="2">14</td><td>16</td></tr>
<tr><th>Eliminated</th><td>Frank</td><td>Grace</td><td>Hank</td></tr>
<tr><td bgcolor="black" colspan="100"></td></tr>
<tr><th>Voter</th><th colspan="3">Vote</th></tr>
<tr><th>Ida</th><td>Frank</td><td>Grace</td><td>Hank</td></tr>
<tr><th>Jack</th><td colspan="2">Frank</td><td>Hank</td></tr>
</tbody></table>
</body></html>
"""

VOTING_FOOTNOTES_HTML = """
<html><body>
<h2 id="Voting_history">Voting history</h2>
<table class="wikitable nowrap" style="text-align:center;">
<tbody>
<tr><th></th><th colspan="1">Original tribes</th></tr>
<tr><th>Episode</th><th>1</th></tr>
<tr><th>Eliminated</th><td>Zara</td></tr>
<tr><td bgcolor="black" colspan="100"></td></tr>
<tr><th>Voter</th><th>Vote</th></tr>
<tr><th>Yvonne</th><td>Zara<sup class="reference"><a href="#note1">[a]</a></sup></td></tr>
<tr><th>Walter</th><td>Zara<sup id="cite_ref-1">[1]</sup></td></tr>
</tbody></table>
</body></html>
"""

VOTING_SPECIAL_VALUES_HTML = """
<html><body>
<h2 id="Voting_history">Voting history</h2>
<table class="wikitable nowrap" style="text-align:center;">
<tbody>
<tr><th>Episode</th><th>1</th><th>2</th><th>3</th><th>4</th></tr>
<tr><th>Eliminated</th><td>A</td><td>B</td><td>C</td><td>D</td></tr>
<tr><td bgcolor="black" colspan="100"></td></tr>
<tr><th>Voter</th><th colspan="4">Vote</th></tr>
<tr><th>Player1</th><td>A</td><td style="background:lightgrey;"><i>Immune</i></td><td style="background:lightgrey;"><i>Saved</i></td><td>D</td></tr>
<tr><th>Player2</th><td style="background:lightgrey;">None</td><td>B</td><td style="background:lightgrey;"><i>Won</i></td><td style="background:lightgrey;"><i>Lost</i></td></tr>
</tbody></table>
</body></html>
"""

NO_VOTING_HTML = """
<html><body>
<h2 id="Episodes">Episodes</h2>
<p>Some text without a voting history section.</p>
</body></html>
"""


JURY_BASIC_HTML = """
<html><body>
<table class="wikitable" style="text-align:center;">
<tbody>
<tr><th colspan="4">Jury vote</th></tr>
<tr><th>Finalist</th><td>Anna</td><td>Beth</td><td>Carl</td></tr>
<tr><td bgcolor="black" colspan="4"></td></tr>
<tr><th>Juror</th><th colspan="3">Vote</th></tr>
<tr><th>Dan</th><td data-sort-value="Yes">✓</td><td></td><td></td></tr>
<tr><th>Eva</th><td></td><td data-sort-value="Yes">✓</td><td></td></tr>
<tr><th>Fred</th><td></td><td></td><td data-sort-value="Yes">✓</td></tr>
<tr><th>Gina</th><td data-sort-value="Yes">✓</td><td></td><td></td></tr>
</tbody></table>
</body></html>
"""

JURY_IMAGE_HTML = """
<html><body>
<table class="wikitable" style="text-align:center;">
<tbody>
<tr><th colspan="3">Jury vote</th></tr>
<tr><th>Finalist</th><td>Maya</td><td>Nate</td></tr>
<tr><td bgcolor="black" colspan="3"></td></tr>
<tr><th>Juror</th><th colspan="2">Vote</th></tr>
<tr><th>Oscar</th><td><img alt="Yes" src="check.png"/></td><td></td></tr>
<tr><th>Patty</th><td></td><td><img alt="Yes" src="check.png"/></td></tr>
<tr><th>Quinn</th><td><img alt="Yes" src="check.png"/></td><td></td></tr>
</tbody></table>
</body></html>
"""

NO_JURY_HTML = """
<html><body>
<table class="wikitable"><tr><th>Something else</th></tr></table>
</body></html>
"""


class TestParseVotingHistory:
    def test_basic(self):
        result = parse_voting_history(VOTING_BASIC_HTML)
        assert result is not None
        votes = result["votes"]
        assert len(votes) == 4
        assert {"voter": "Charlie", "episode_number": 1, "target": "Alice"} in votes
        assert {"voter": "Charlie", "episode_number": 2, "target": "Bob"} in votes
        assert {"voter": "Diana", "episode_number": 1, "target": "Alice"} in votes
        assert {"voter": "Eve", "episode_number": 2, "target": "Bob"} in votes

    def test_colspan(self):
        result = parse_voting_history(VOTING_COLSPAN_HTML)
        assert result is not None
        votes = result["votes"]
        ida_votes = [v for v in votes if v["voter"] == "Ida"]
        jack_votes = [v for v in votes if v["voter"] == "Jack"]
        assert len(ida_votes) == 3
        assert {"voter": "Ida", "episode_number": 5, "target": "Frank"} in ida_votes
        assert {"voter": "Ida", "episode_number": 5, "target": "Grace"} in ida_votes
        assert {"voter": "Ida", "episode_number": 6, "target": "Hank"} in ida_votes
        assert len(jack_votes) == 2
        assert {"voter": "Jack", "episode_number": 5, "target": "Frank"} in jack_votes
        assert {"voter": "Jack", "episode_number": 6, "target": "Hank"} in jack_votes

    def test_cleans_footnotes(self):
        result = parse_voting_history(VOTING_FOOTNOTES_HTML)
        assert result is not None
        votes = result["votes"]
        assert len(votes) == 2
        for v in votes:
            assert v["target"] == "Zara"

    def test_skips_special_values(self):
        result = parse_voting_history(VOTING_SPECIAL_VALUES_HTML)
        assert result is not None
        votes = result["votes"]
        p1_votes = [v for v in votes if v["voter"] == "Player1"]
        p2_votes = [v for v in votes if v["voter"] == "Player2"]
        assert len(p1_votes) == 2
        assert {"voter": "Player1", "episode_number": 1, "target": "A"} in p1_votes
        assert {"voter": "Player1", "episode_number": 4, "target": "D"} in p1_votes
        assert len(p2_votes) == 1
        assert {"voter": "Player2", "episode_number": 2, "target": "B"} in p2_votes

    def test_returns_none(self):
        assert parse_voting_history(NO_VOTING_HTML) is None


class TestParseJuryVote:
    def test_basic(self):
        result = parse_jury_vote(JURY_BASIC_HTML)
        assert result is not None
        jv = result["jury_votes"]
        assert len(jv) == 4
        assert {"juror": "Dan", "voted_for": "Anna"} in jv
        assert {"juror": "Eva", "voted_for": "Beth"} in jv
        assert {"juror": "Fred", "voted_for": "Carl"} in jv
        assert {"juror": "Gina", "voted_for": "Anna"} in jv

    def test_image_checkmarks(self):
        result = parse_jury_vote(JURY_IMAGE_HTML)
        assert result is not None
        jv = result["jury_votes"]
        assert len(jv) == 3
        assert {"juror": "Oscar", "voted_for": "Maya"} in jv
        assert {"juror": "Patty", "voted_for": "Nate"} in jv
        assert {"juror": "Quinn", "voted_for": "Maya"} in jv

    def test_returns_none(self):
        assert parse_jury_vote(NO_JURY_HTML) is None


class TestRealSeason41:
    @pytest.fixture()
    def season41_html(self):
        path = Path(__file__).resolve().parent.parent / "data" / "raw_html" / "Survivor_41.html"
        if not path.exists():
            pytest.skip("Survivor_41.html not available")
        return path.read_text(encoding="utf-8")

    def test_voting_history(self, season41_html):
        result = parse_voting_history(season41_html)
        assert result is not None
        votes = result["votes"]
        assert len(votes) > 50
        voters = {v["voter"] for v in votes}
        assert "Erika" in voters
        assert "Xander" in voters
        assert "Deshawn" in voters
        assert "Ricard" in voters

    def test_jury_votes(self, season41_html):
        result = parse_jury_vote(season41_html)
        assert result is not None
        jv = result["jury_votes"]
        assert len(jv) == 8
        vote_counts = {}
        for v in jv:
            vote_counts[v["voted_for"]] = vote_counts.get(v["voted_for"], 0) + 1
        assert vote_counts.get("Erika") == 7
        assert vote_counts.get("Deshawn") == 1
