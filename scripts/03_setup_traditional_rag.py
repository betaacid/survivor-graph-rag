import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from tqdm import tqdm

from lib.chunker import chunk_text
from lib.embeddings import embed_texts
from lib.pg_client import get_chunk_count, insert_chunks, setup_schema

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TEXT_DIR = DATA_DIR / "raw_text"


def main():
    manifest_path = DATA_DIR / "seasons_manifest.json"
    if not manifest_path.exists():
        print("Run 01_download_seasons.py first.")
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    print("Setting up pgvector schema...")
    setup_schema()

    print(f"Chunking and embedding {len(manifest)} seasons...")
    total_chunks = 0

    for season in tqdm(manifest, desc="Embedding seasons"):
        title = season["title"]
        text_path = season["text_path"]
        text = Path(text_path).read_text(encoding="utf-8")

        chunks = chunk_text(text)
        if not chunks:
            continue

        embeddings = embed_texts(chunks)
        insert_chunks(title, chunks, embeddings)
        total_chunks += len(chunks)

    count = get_chunk_count()
    print(f"\nDone. {total_chunks} chunks embedded and stored. Total in DB: {count}")


if __name__ == "__main__":
    main()
