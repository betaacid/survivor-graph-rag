from pathlib import Path

from lib.table_parser import extract_tables_for_season, extract_tables_from_html


FIXTURE = Path(__file__).parent / "fixtures" / "sample_season.html"


def test_extract_tables_from_html_reads_wikitable_caption_and_rows():
    html = FIXTURE.read_text(encoding="utf-8")
    tables = extract_tables_from_html(html)
    assert len(tables) == 1
    assert tables[0]["caption"] == "Challenge winners and eliminations by episode"
    assert tables[0]["num_rows"] == 2
    assert "No." in tables[0]["columns"][0]


def test_extract_tables_for_season_writes_json(tmp_path):
    tables = extract_tables_for_season(FIXTURE, tmp_path)
    out_file = tmp_path / "sample_season.tables.json"
    assert out_file.exists()
    assert len(tables) == 1
