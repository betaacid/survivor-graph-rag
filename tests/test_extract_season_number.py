import importlib.util
from pathlib import Path


module_path = Path(__file__).resolve().parents[1] / "scripts" / "04_setup_graph_rag.py"
spec = importlib.util.spec_from_file_location("setup_graph_rag", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_extract_season_number_known_name():
    assert module.extract_season_number("Survivor: The Australian Outback") == 2


def test_extract_season_number_explicit_number():
    assert module.extract_season_number("Survivor 47") == 47


def test_extract_season_number_returns_none_for_unknown():
    assert module.extract_season_number("Not Survivor") is None
