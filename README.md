# Survivor: Graph RAG vs Traditional RAG

A demo that compares Traditional RAG (pgvector semantic search) with Graph RAG (Neo4j) using Wikipedia data from all 50 seasons of Survivor.

Traditional RAG works great for single-document, extractive questions. Graph RAG shines on multi-hop, cross-season, aggregation, and relational queries.

## Quick Start

```bash
# 1. Clone and install
git clone <repo-url> && cd survivorgraph
pip install -r requirements.txt

# 2. Configure
cp .env.example .env
# Edit .env — add your OPENAI_API_KEY (required)

# 3. Start databases
docker compose up -d

# 4. Run the full pipeline (download, parse, embed, ingest, demo)
python run_all.py
```

## What It Does

1. **Downloads** all 50 Survivor season pages from Wikipedia via the MediaWiki API
2. **Extracts** structured tables (contestants, episodes, voting history) using pandas + GPT for column normalization
3. **Sets up Traditional RAG** — chunks text, embeds with OpenAI, stores in pgvector
4. **Sets up Graph RAG** — ingests structured data into Neo4j with a rich schema (Season, Player, Episode, Tribe, TribalCouncil, Vote edges)
5. **Runs demo queries** side-by-side to compare both approaches

## Requirements

- Python 3.11+
- Docker & Docker Compose
- OpenAI API key
