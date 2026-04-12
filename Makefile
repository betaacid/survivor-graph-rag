.DEFAULT_GOAL := setup
.PHONY: up down reset ingest app test demo setup

-include .env
export

NEO4J_USER ?= neo4j
NEO4J_PASSWORD ?= survivor
POSTGRES_USER ?= postgres
POSTGRES_DB ?= survivor_rag

up:
	docker compose up -d
	@echo "Waiting for services..."
	@until docker compose exec -T neo4j cypher-shell -u "$(NEO4J_USER)" -p "$(NEO4J_PASSWORD)" "RETURN 1" >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec -T postgres pg_isready -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" >/dev/null 2>&1; do sleep 1; done
	@echo "Services ready."

down:
	docker compose down

reset:
	uv run python scripts/reset.py

ingest:
	uv run python scripts/01_ingest_graph.py
	uv run python scripts/02_download_wiki.py
	uv run python scripts/03_setup_traditional_rag.py
	uv run python scripts/04_ingest_wiki_chunks.py

app:
	uv run streamlit run app.py

test:
	uv run pytest

demo:
	uv run python scripts/05_demo_queries.py

setup: up ingest
	@echo "Ready. Run 'make app' to start."
