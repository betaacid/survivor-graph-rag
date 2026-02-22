import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.wiki_fetcher import download_all_seasons

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Limit to N seasons (smoke test)")
    parser.add_argument("--fresh", action="store_true", help="Clear data/ before download")
    args = parser.parse_args()

    if args.fresh:
        import shutil
        if DATA_DIR.exists():
            log.info("Removing existing data/ for fresh run")
            shutil.rmtree(DATA_DIR)
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Downloading Survivor season pages from Wikipedia...")
    results = download_all_seasons(DATA_DIR, limit=args.limit, fresh=args.fresh)

    manifest_path = DATA_DIR / "seasons_manifest.json"
    manifest_path.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("Done. %d seasons saved. Manifest: %s", len(results), manifest_path)


if __name__ == "__main__":
    main()
