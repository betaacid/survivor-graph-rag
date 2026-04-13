.DEFAULT_GOAL := setup
.PHONY: env check-openai up down reset ingest app test demo setup

-include .env
export

NEO4J_USER ?= neo4j
NEO4J_PASSWORD ?= survivor
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= survivor
POSTGRES_DB ?= survivor_rag
NEO4J_URI ?= bolt://localhost:7687
DATABASE_URL ?= postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@localhost:5433/$(POSTGRES_DB)

env:
	@if [ ! -f .env ]; then printf "OPENAI_API_KEY=\nNEO4J_URI=%s\nNEO4J_USER=%s\nNEO4J_PASSWORD=%s\nPOSTGRES_USER=%s\nPOSTGRES_PASSWORD=%s\nDATABASE_URL=%s\n" "$(NEO4J_URI)" "$(NEO4J_USER)" "$(NEO4J_PASSWORD)" "$(POSTGRES_USER)" "$(POSTGRES_PASSWORD)" "$(DATABASE_URL)" > .env; echo "Created .env. Add OPENAI_API_KEY before running setup, demo, or asking questions in the app."; else echo ".env already exists."; fi

check-openai:
	@python -c "import os, sys; key = os.getenv('OPENAI_API_KEY', '').strip(); sys.exit(0 if key and key != 'sk-your-key-here' else 1)" || (echo "Set OPENAI_API_KEY in .env before running this target."; exit 1)

up: env
	docker compose up -d
	@echo "Waiting for services..."
	@until docker compose exec -T neo4j cypher-shell -u "$(NEO4J_USER)" -p "$(NEO4J_PASSWORD)" "RETURN 1" >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T postgres pg_isready -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" >/dev/null 2>&1; do sleep 1; done
	@echo "Services ready."

down:
	docker compose down

reset: env up
	uv run python scripts/reset.py

ingest: check-openai up
	uv run python scripts/01_ingest_graph.py
	uv run python scripts/02_download_wiki.py
	uv run python scripts/03_setup_traditional_rag.py
	uv run python scripts/04_ingest_wiki_chunks.py

app: up
	uv run streamlit run app.py

test:
	uv run pytest

demo: check-openai up
	uv run python scripts/05_demo_queries.py

setup: check-openai up ingest
	@echo "Ready. Run 'make app' to start."
