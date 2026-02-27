# Survivor graph RAG

Compares three ways to answer questions about the TV show Survivor using data from all 49 aired seasons:

1. **Traditional RAG** -- semantic search over chunked Wikipedia text stored in pgvector.
2. **Graph RAG** -- an LLM generates Cypher queries against a Neo4j knowledge graph.
3. **Agentic RAG** -- a multi-step pipeline that rewrites the question, picks the right tool, checks the answer, and retries if something is missing.

The point of the project is to show where each approach works well and where it falls apart.

## Setup

You need [uv](https://docs.astral.sh/uv/) (Python 3.11+), Docker, and an OpenAI API key.

```bash
git clone <repo-url> && cd survivorgraph
uv sync
```

Copy the example environment file and fill in your keys:

```bash
cp .env.example .env
```

The `.env` file needs:

| Variable | What it is |
|---|---|
| `OPENAI_API_KEY` | Used for embeddings, chat completions, and structured outputs (table classification, normalization) |
| `NEO4J_URI` | Defaults to `bolt://localhost:7687` |
| `NEO4J_PASSWORD` | Defaults to `survivor` |
| `DATABASE_URL` | Postgres connection string, defaults to `postgresql://postgres:survivor@localhost:5433/survivor_rag` |

Start the databases:

```bash
docker compose up -d
```

This runs Neo4j 5 Community and pgvector (Postgres 17) in containers. Neo4j takes about 10 seconds to become healthy.

## Loading data

There are two ways to get data into the graph. Pick one.

### Option A: survivoR dataset (fast, no LLM calls)

This pulls pre-structured JSON from the [survivoR R package](https://github.com/doehm/survivoR) on GitHub and writes it straight to Neo4j. No API keys needed for this step, no LLM calls, fully deterministic. Takes about 45 seconds.

```bash
uv run python scripts/05_ingest_survivoR.py
```

It clears the graph first (unless you pass `--seasons 41,45` to target specific seasons). When it finishes, it prints a coverage report showing node counts and which seasons have data for each relationship type.

### Option B: Wikipedia pipeline (slower, uses LLM calls)

This is the original pipeline. It downloads Wikipedia pages, extracts tables, classifies and normalizes them with OpenAI, then ingests into Neo4j. It also chunks the text and embeds it into pgvector for Traditional RAG.

```bash
uv run python run_all.py        # full run
uv run python run_all.py --reset # wipe both databases first
```

Or run scripts individually:

```bash
uv run python scripts/00_reset_databases.py       # clear Neo4j + Postgres
uv run python scripts/01_download_seasons.py       # fetch Wikipedia HTML
uv run python scripts/02_extract_tables.py         # classify tables (OpenAI)
uv run python scripts/03_setup_traditional_rag.py  # chunk, embed, store in pgvector
uv run python scripts/04_setup_graph_rag.py        # normalize + ingest into Neo4j (OpenAI)
```

Scripts 02 and 04 are where the LLM calls happen. Script 03 makes OpenAI embedding API calls (cheap, but still API calls). Scripts 00 and 01 don't call any APIs beyond Wikipedia.

If you just want the graph and don't need Traditional RAG, use Option A and skip the Wikipedia pipeline entirely. The pgvector chunks from a previous run will still work for Traditional RAG queries.

### Smoke test (one season)

To verify the Wikipedia pipeline without processing all 49 seasons:

```bash
docker compose up -d
uv run python scripts/00_reset_databases.py
uv run python scripts/01_download_seasons.py --limit 1 --fresh
uv run python scripts/02_extract_tables.py --fresh
uv run python scripts/03_setup_traditional_rag.py
uv run python scripts/04_setup_graph_rag.py --smoke
uv run python scripts/05_demo_queries.py --limit 2
```

## Running the app

```bash
uv run streamlit run app.py
```

Opens at `http://localhost:8501`. Pick a mode from the radio buttons:

- **All** -- runs all three approaches side-by-side on the same question. Good for comparing answers and timing.
- **Traditional RAG** -- just the vector search path.
- **Graph RAG** -- just the Cypher generation path.
- **Agentic RAG** -- the full pipeline with an expandable "Agent Trace" showing each step.

The sidebar has example questions grouped by which approach handles them best, plus live node counts from the graph.

## How it works

### Traditional RAG

The simplest approach. Wikipedia article text for each season gets split into overlapping chunks (800 words, 200-word overlap), embedded with OpenAI's text-embedding-3-small, and stored in Postgres with the pgvector extension.

At query time, the question is embedded with the same model, pgvector finds the 6 most similar chunks by cosine distance, and those chunks are passed to GPT as context to generate an answer.

This works well for questions that can be answered from a single passage: "Why was Michael Skupin medevaced?", "What was the fan reception of season 48?", narrative and opinion questions. It struggles with anything that requires counting, comparing across seasons, or following relationships between entities. If you ask "Who has played the most seasons?", the retriever might find chunks that mention a few returning players, but it can't aggregate across all 49 seasons.

The code for this lives in `lib/traditional_rag.py`. It's about 30 lines.

### Graph RAG

Instead of searching text chunks, this approach stores structured data in a Neo4j graph and generates Cypher queries to answer questions.

The graph schema has six node types:

- **Season** -- title, number
- **Player** -- name (unique across the show)
- **PlayerSeason** -- one per player per season, with placement, exit type, jury membership, age, hometown
- **Episode** -- season number, episode number, title, air date, viewers
- **Tribe** -- name, season, phase (premerge or merged)
- **TribalCouncil** -- season, episode number

These are connected by relationships: `PLAYED_IN`, `IN_SEASON`, `MEMBER_OF`, `HAS_EPISODE`, `HAS_TRIBE`, `ELIMINATED`, `IMMUNITY_WON_BY`, `REWARD_WON_BY`, `CAST_VOTE`, `JURY_VOTE_FOR`, `ATTENDED_TRIBAL`, and a few others.

At query time, the question goes to GPT along with the full graph schema, a terminology map (so the LLM knows that "won immunity" means the `IMMUNITY_WON_BY` relationship, not a property), and 11 few-shot Cypher examples. GPT generates a Cypher query, which runs against Neo4j. If the query returns no results or errors out, the system retries up to twice, feeding the error or empty-result context back to the LLM to fix the query. The results then go to GPT to generate a natural language answer.

This handles aggregation and relational queries that Traditional RAG can't touch: "How many tribal councils across all seasons?", "Which player has played the most seasons?", "List all winners and their jury vote counts." It fails when the question doesn't map cleanly to the schema, or when the LLM generates bad Cypher.

The code lives in `lib/graph_rag.py`.

### Agentic RAG

Graph RAG with text2cypher works, but it's brittle. The LLM sometimes generates Cypher that's syntactically valid but semantically wrong -- querying properties that don't exist, using the wrong relationship direction, or missing a `WHERE` clause. Agentic RAG addresses this with a pipeline of four steps.

**Step 1: Rewrite.** The original question gets rewritten to be more atomic and specific. "Who won 45 and who was on the jury?" becomes two clear sub-questions. This helps the router pick the right tool.

**Step 2: Route.** An LLM with OpenAI function calling chooses between 8 prebuilt query tools and a text2cypher fallback. The prebuilt tools are parameterized Cypher queries that are known to work: `season_winner`, `player_seasons`, `season_tribes`, `top_immunity_winners`, `top_reward_winners`, `jury_members`, `elimination_by_episode`, `players_multiple_seasons`. Each one takes typed arguments (a season number, a player name, a limit) and returns rows. If none of the prebuilt tools fit, the router falls back to text2cypher, which generates Cypher from scratch the same way Graph RAG does.

**Step 3: Critique.** After the tool returns data, a critic LLM checks whether the retrieved data actually answers the original question. If information is missing, it generates follow-up questions.

**Step 4: Retry.** If the critic flagged gaps, the follow-up questions go back through the router. The results get merged with the first round, and the combined data goes to GPT for the final answer.

The prebuilt tools are the key improvement. Text2cypher guesses at query structure every time. The prebuilt tools are hand-written Cypher that always works. The router just has to pick the right one and pass the right arguments, which is a much easier job for an LLM than writing Cypher from scratch.

The code lives in `lib/agentic_rag.py`.

### Where each approach works

| Question type | Traditional RAG | Graph RAG | Agentic RAG |
|---|---|---|---|
| Narrative / opinion ("Why did X happen?") | Good | Poor | Poor |
| Single-fact lookup ("Who won season 45?") | Depends on chunk | Good | Good |
| Aggregation ("Total tribal councils across all seasons?") | Can't do it | Good | Good |
| Multi-hop ("Who has the most immunity wins, and how many seasons did they play?") | Can't do it | Possible but fragile | Good |
| Cross-entity ("Players in 4+ seasons who won the most rewards?") | Can't do it | Fragile | Good |

## Project structure

```
lib/
  traditional_rag.py   # vector search query path
  graph_rag.py         # text2cypher query path, schema, few-shot examples
  agentic_rag.py       # rewriter, router, tool registry, critic
  llm.py               # OpenAI client wrappers
  neo4j_client.py      # all Neo4j reads and writes
  pg_client.py         # pgvector schema, inserts, similarity search
  embeddings.py        # OpenAI embedding wrapper
  chunker.py           # text chunking
  demo_questions.py    # example questions grouped by best approach
  table_parser.py      # HTML table extraction from Wikipedia
  vote_parser.py       # custom parser for voting/jury tables
  wiki_fetcher.py      # Wikipedia API client

scripts/
  00_reset_databases.py        # clear both databases
  01_download_seasons.py       # fetch Wikipedia HTML
  02_extract_tables.py         # extract and classify tables
  03_setup_traditional_rag.py  # chunk, embed, store
  04_setup_graph_rag.py        # normalize tables, ingest into Neo4j
  05_ingest_survivoR.py        # alternative: deterministic ingest from survivoR dataset
  05_demo_queries.py           # CLI demo comparing approaches

app.py          # Streamlit UI
run_all.py      # runs the Wikipedia pipeline end to end
```

## Tests

```bash
uv sync --group dev
uv run --group dev pytest
```

These test the parsing and normalization helpers. They don't need databases or API keys.
