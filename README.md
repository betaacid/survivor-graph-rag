# Survivor: Graph RAG vs Traditional RAG

A demo that compares Traditional RAG (pgvector semantic search) with Graph RAG (Neo4j) using Wikipedia data from all 50 seasons of Survivor.

Traditional RAG works great for single-document, extractive questions. Graph RAG shines on multi-hop, cross-season, aggregation, and relational queries.

## Quick Start

```bash
# 1. Clone and install (requires uv: https://docs.astral.sh/uv/)
git clone <repo-url> && cd survivorgraph
uv sync

# 2. Configure
cp .env.example .env
# Edit .env — add your OPENAI_API_KEY (required)

# 3. Start databases
docker compose up -d

# 4. Run the full pipeline (download, parse, embed, ingest, demo)
uv run python run_all.py

# Or reset databases first for a clean slate (clears Neo4j graph and Postgres chunks)
uv run python run_all.py --reset
```

## What It Does

1. **Downloads** all 50 Survivor season pages from Wikipedia via the MediaWiki API
2. **Extracts** structured tables (contestants, episodes, voting history) using pandas + GPT for column normalization
3. **Sets up Traditional RAG** — chunks text, embeds with OpenAI, stores in pgvector
4. **Sets up Graph RAG** — ingests structured data into Neo4j with a rich schema (Season, Player, Episode, Tribe, TribalCouncil, Vote edges)
5. **Runs demo queries** side-by-side to compare both approaches

## Requirements

- [uv](https://docs.astral.sh/uv/) (Python 3.11+)
- Docker & Docker Compose
- OpenAI API key

## Run Tests

```bash
uv sync --group dev
uv run --group dev pytest
```

These tests validate parsing and normalization helpers without running the full download/embedding/graph pipeline.

## Smoke Test (Lightweight Run)

Run each script one at a time with minimal data to verify the pipeline:

```bash
# 1. Start databases first
docker compose up -d

# 2. Reset databases (optional; use when re-running to avoid constraint-already-exists noise)
uv run python scripts/00_reset_databases.py

# 3. Download 1 season (--limit 1 skips cache; --fresh clears data/ first)
uv run python scripts/01_download_seasons.py --limit 1 --fresh

# 4. Extract tables (--fresh skips cache; needs GROQ_API_KEY)
uv run python scripts/02_extract_tables.py --fresh

# 5. Traditional RAG (needs Postgres, OPENAI_API_KEY)
uv run python scripts/03_setup_traditional_rag.py

# 6. Graph RAG (needs Neo4j, GROQ_API_KEY)
uv run python scripts/04_setup_graph_rag.py --smoke

# 7. Demo (runs 2 smoke-test questions that work with 1 season)
uv run python scripts/05_demo_queries.py --limit 2
```

All scripts log to stdout. Check logs if any step fails.
