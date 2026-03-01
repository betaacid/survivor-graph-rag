# Survivor graph RAG 🏝️

A question-answering app about the TV show Survivor, built to compare three retrieval approaches side by side.

This project compares traditional RAG, graph RAG, and agentic graph RAG. We built all three against the same datasets (49 seasons of Survivor Wikipedia articles and the [survivoR dataset](https://github.com/doehm/survivoR)) so we could ask the same questions and see where each one works and where it falls apart.

**Traditional RAG** chunks the Wikipedia articles for each Survivor season, embeds them, and stores them in pgvector. At query time it finds the most similar chunks and passes them to GPT as context. Works for narrative questions where the answer lives in a few paragraphs.

**Graph RAG** stores structured facts in Neo4j -- players, seasons, tribes, votes, immunity wins -- as nodes and relationships. Those same Wikipedia articles are also chunked and stored as nodes in the graph, linked to the players and seasons they mention. When you ask a question, an LLM writes a Cypher query against all of it. Structured traversals for factual lookups, text chunks for narrative ones. It breaks when the LLM writes bad Cypher, which happens more than you'd want.

**Agentic graph RAG** adds a layer on top. Instead of trusting a single LLM-generated Cypher query, it rewrites your question to be more precise, routes it to a prebuilt tool when one fits (so it can avoid freeform Cypher entirely), checks whether the result actually answers the question, and retries if something's missing. It has a dedicated chunk search tool for narrative questions and prebuilt Cypher for common structured lookups. More expensive per query, but noticeably more reliable on complex questions.

## The app

We picked Survivor because it's a good stress test. The show has a dense web of relationships -- who played with whom, who voted for whom, tribal swaps, immunity wins, jury votes -- spread across 49 seasons and 750+ players. Plenty of structure for graph queries, and plenty of raw text for vector search.

The app is a Streamlit interface. Type a question, pick a mode (or run all three side-by-side to compare). The sidebar has example questions grouped by which approach handles them best.

## Running it

You need [uv](https://docs.astral.sh/uv/) (Python 3.11+), Docker, and an OpenAI API key.

```bash
git clone <repo-url> && cd survivorgraph
uv sync
cp .env.example .env
# add your OPENAI_API_KEY to .env
```

Start the databases (Neo4j and pgvector):

```bash
docker compose up -d
```

Load the knowledge graph. This pulls pre-cleaned data from the [survivoR R package](https://github.com/doehm/survivoR) -- no LLM calls, takes about 45 seconds:

```bash
uv run python scripts/05_ingest_survivoR.py
```

If you also want traditional RAG (vector search over Wikipedia text), run the Wikipedia pipeline to download, chunk, and embed the season articles:

```bash
uv run python scripts/01_download_seasons.py
uv run python scripts/02_extract_tables.py
uv run python scripts/03_setup_traditional_rag.py
```

Scripts 02 and 03 make OpenAI API calls (table classification and embeddings).

Start the app:

```bash
uv run streamlit run app.py
```

Opens at `http://localhost:8501`.

## What works where

| Question type                                                       | Traditional RAG | Graph RAG | Agentic RAG |
| ------------------------------------------------------------------- | --------------- | --------- | ----------- |
| Narrative ("Why was Skupin medevaced?")                             | Good            | Good      | Good        |
| Single fact ("Who won season 45?")                                  | Hit or miss     | Good      | Good        |
| Aggregation ("Total tribal councils across all seasons?")           | No              | Good      | Good        |
| Multi-hop ("Most immunity wins -- how many seasons did they play?") | No              | Sometimes | Good        |
| Cross-entity ("Players in 4+ seasons with the most reward wins?")   | No              | Sometimes | Good        |

All three modes can handle narrative questions since each has access to text chunks -- traditional RAG through pgvector, graph RAG and agentic RAG through chunk nodes in Neo4j. The difference shows up on structured questions.

Graph RAG handles counting, aggregation, and cross-season comparisons well. "How many tribal councils total?" or "List all winners and their jury vote counts" -- these need Cypher, not text retrieval. It can also handle multi-hop and cross-entity queries when the LLM generates correct Cypher, but compound queries that chain multiple aggregations together don't always come out right.

Agentic RAG earns its keep on those harder multi-part questions. "Who won Survivor 45, and who were the jury members that season?" needs two separate lookups and the agent handles the routing and merging. Because it uses prebuilt, tested queries whenever one fits, it avoids the freeform Cypher mistakes that trip up graph RAG on complex queries.

## The Wikipedia extraction detour

We originally planned to build the entire knowledge graph from Wikipedia. Download all 49 season pages, extract the HTML tables, use an LLM to classify and normalize them, load everything into Neo4j.

It kind of worked. The LLM handled simple tables fine -- contestant lists, episode summaries. But voting history and jury vote grids have strange layouts with colspan and rowspan, and the 49 season articles were written by different editors over 20 years. Column names vary ("Finish" vs "Placement" vs "Result"), cells are full of footnote markers, and the normalization step would quietly drop players or misattribute votes. We wrote custom BeautifulSoup parsers for the worst tables (`lib/vote_parser.py`), but even those miss edge cases. Name matching across tables was its own headache -- "Ozzy" in one table, "Oscar 'Ozzy' Lusth" in another.

All solvable with enough time, but we went with the [survivoR R package](https://github.com/doehm/survivoR) dataset instead. It's pre-cleaned with consistent identifiers across all 49 seasons, and loading it doesn't touch the OpenAI API at all. The Wikipedia pipeline code is still in the repo since it's the only source of long-form text for traditional RAG.
