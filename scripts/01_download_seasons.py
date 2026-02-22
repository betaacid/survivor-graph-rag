import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.wiki_fetcher import download_all_seasons

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def main():
    print("Downloading all Survivor season pages from Wikipedia...")
    results = download_all_seasons(DATA_DIR)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    manifest_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nDone. {len(results)} seasons saved. Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
