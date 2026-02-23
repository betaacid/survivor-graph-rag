import argparse
import datetime
import json
import logging
import sys
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.embeddings import embed_query

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)
from lib.llm import chat
from lib.neo4j_client import get_graph_schema, run_query
from lib.pg_client import search_similar

DEMO_QUESTIONS = [
    {
        "category": "Smoke Test (work with 1 season)",
        "questions": [
            "Who won Survivor 41?",
            "Who were the tribes in Survivor 41?",
        ],
    },
    {
        "category": "Traditional RAG Strengths",
        "questions": [
            "Who won Survivor: The Australian Outback?",
            "Why was Michael Skupin medically evacuated?",
            "Describe the dynamics between Colby and Jerri in The Australian Outback.",
        ],
    },
    {
        "category": "Traditional RAG Starts to Struggle",
        "questions": [
            "How many individual immunity challenges did Colby win in The Australian Outback?",
            "Who were all the jury members in Survivor: The Australian Outback?",
        ],
    },
    {
        "category": "Graph RAG Wins",
        "questions": [
            "Across all seasons, who has the highest number of individual immunity wins?",
            "Which players were voted out in one season but returned in a later season?",
            "Find every instance where a player voted for someone who later voted them out in the same season.",
            "What is the most common episode number for a player's elimination across all seasons?",
            "Which seasons had a medical evacuation?",
            "For each winner, how many jury votes did they receive? Show the top 10 by margin of victory.",
            "Show all players who competed in 3 or more seasons and their placement in each.",
        ],
    },
]

TRAD_RAG_SYSTEM = """You are a Survivor TV show expert answering questions using only the provided context passages.
If the context doesn't contain enough information to answer confidently, say so.
Be specific and cite which season/episode when relevant."""

GRAPH_RAG_SYSTEM = """You are a Survivor TV show expert. You have been given structured query results from a graph database.
Answer the question using ONLY the data provided in the query results below. Do not supplement with your own knowledge or make assumptions beyond what the data shows.
If the results are empty or do not contain enough information to answer, say that the data wasn't found in the graph.
Format numbers, lists, and tables clearly."""

CYPHER_EXAMPLES = [
    (
        "Who won Survivor 41?",
        "MATCH (ps:PlayerSeason {season_number: 41}) WHERE ps.exit_type = 'winner' RETURN ps.player_name",
    ),
    (
        "What tribes were in Survivor 41?",
        "MATCH (t:Tribe {season_number: 41}) RETURN t.name, t.phase ORDER BY t.phase",
    ),
    (
        "How many individual immunity challenges did Colby win in The Australian Outback?",
        "MATCH (e:Episode)-[:IMMUNITY_WON_BY]->(ps:PlayerSeason {player_name: 'Colby Donaldson', season_number: 2}) RETURN count(e) AS immunity_wins",
    ),
    (
        "Which players competed in 3 or more seasons?",
        "MATCH (p:Player)-[:PLAYED_IN]->(ps:PlayerSeason) WITH p, count(ps) AS seasons WHERE seasons >= 3 RETURN p.name, seasons ORDER BY seasons DESC",
    ),
    (
        "Which seasons had a medical evacuation?",
        "MATCH (ps:PlayerSeason) WHERE ps.exit_type = 'medevac' RETURN DISTINCT ps.season_number ORDER BY ps.season_number",
    ),
    (
        "Who were all the jury members in Survivor: The Australian Outback?",
        "MATCH (ps:PlayerSeason {season_number: 2}) WHERE ps.jury_member = true RETURN ps.player_name, ps.placement ORDER BY ps.placement",
    ),
    (
        "Find every instance where a player voted for someone who later voted them out in the same season.",
        "MATCH (a:PlayerSeason)-[v1:CAST_VOTE]->(b:PlayerSeason), (b)-[v2:CAST_VOTE]->(a), (e:Episode)-[:ELIMINATED]->(a) WHERE a.season_number = b.season_number AND v1.episode_number < v2.episode_number AND v2.episode_number = e.episode_number RETURN a.player_name AS voted_first, b.player_name AS voted_back, a.season_number AS season, v1.episode_number AS first_vote_ep, v2.episode_number AS elimination_ep",
    ),
    (
        "Across all seasons, who has the highest number of individual immunity wins?",
        "MATCH (e:Episode)-[:IMMUNITY_WON_BY]->(ps:PlayerSeason) RETURN ps.player_name, count(e) AS wins ORDER BY wins DESC LIMIT 10",
    ),
]

TERMINOLOGY_MAP = """Terminology mappings:
- "winner" / "won the season" -> PlayerSeason node where exit_type = 'winner'
- "runner-up" -> PlayerSeason node where exit_type = 'runner_up'
- "voted out" / "eliminated" -> exit_type = 'voted_out', or the [:ELIMINATED] relationship from Episode
- "immunity win" / "won immunity" -> [:IMMUNITY_WON_BY] relationship from Episode to PlayerSeason (NOT a property)
- "reward win" -> [:REWARD_WON_BY] relationship from Episode to PlayerSeason (NOT a property)
- "tribe" / "team" -> (:Tribe) node accessed via [:MEMBER_OF] from PlayerSeason
- "season N" / "Survivor N" -> Season.number = N or PlayerSeason.season_number = N
- "jury member" -> PlayerSeason.jury_member property (boolean)
- "medevac" / "medical evacuation" -> exit_type = 'medevac'
- "quit" -> exit_type = 'quit'
- "returning player" / "played multiple times" -> Player with multiple [:PLAYED_IN] relationships
- "voted for" -> [:CAST_VOTE] relationship between PlayerSeason nodes (has episode_number property)
- "original tribe" / "starting tribe" -> Tribe node where phase = 'premerge', accessed via [:MEMBER_OF]"""

FORMAT_INSTRUCTIONS = """Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
Do not wrap the output in code blocks or backticks."""

CYPHER_MAX_RETRIES = 2


def _build_cypher_system_prompt(schema=None):
    if schema is None:
        try:
            schema = get_graph_schema()
        except Exception:
            log.warning("Could not infer schema from Neo4j; using static fallback")
            schema = _STATIC_SCHEMA

    examples_block = "Few-shot examples:\n"
    for q, c in CYPHER_EXAMPLES:
        examples_block += f"Question: {q}\nCypher: {c}\n\n"

    return "\n\n".join([
        "You are a Neo4j Cypher query expert for a Survivor TV show graph database.",
        f"Graph database schema:\nUse only the provided relationship types and properties in the schema. "
        f"Do not use any other relationship types or properties that are not provided.\n\n{schema}",
        TERMINOLOGY_MAP,
        examples_block.rstrip(),
        "Query guidelines:\n"
        "- Keep results to a reasonable size (use LIMIT if the result set could be large).\n"
        "- Use OPTIONAL MATCH when a path might not exist for all nodes.\n"
        "- Prefer traversing relationships over filtering on properties when the schema models something as a relationship.",
        FORMAT_INSTRUCTIONS,
    ])


_STATIC_SCHEMA = """Node labels and properties:
  Season {title: STRING, number: INTEGER}
  Player {name: STRING}
  PlayerSeason {player_name: STRING, season_number: INTEGER, age: INTEGER, hometown: STRING, placement: INTEGER, day_out: INTEGER, exit_type: STRING, jury_member: BOOLEAN}
  Episode {season_number: INTEGER, episode_number: INTEGER, title: STRING, air_date: STRING, viewers_millions: FLOAT}
  Tribe {name: STRING, season_number: INTEGER, phase: STRING}
Relationship types and properties:
  HAS_EPISODE
  HAS_TRIBE
  PLAYED_IN
  IN_SEASON
  MEMBER_OF
  ELIMINATED
  IMMUNITY_WON_BY
  REWARD_WON_BY
  CAST_VOTE {episode_number: INTEGER}
The relationships:
  (:Season)-[:HAS_EPISODE]->(:Episode)
  (:Season)-[:HAS_TRIBE]->(:Tribe)
  (:Player)-[:PLAYED_IN]->(:PlayerSeason)
  (:PlayerSeason)-[:IN_SEASON]->(:Season)
  (:PlayerSeason)-[:MEMBER_OF]->(:Tribe)
  (:Episode)-[:ELIMINATED]->(:PlayerSeason)
  (:Episode)-[:IMMUNITY_WON_BY]->(:PlayerSeason)
  (:Episode)-[:REWARD_WON_BY]->(:PlayerSeason)
  (:PlayerSeason)-[:CAST_VOTE]->(:PlayerSeason)"""


def _clean_cypher(raw):
    cypher = raw.strip().strip("`").strip()
    if cypher.lower().startswith("cypher"):
        cypher = cypher[6:].strip()
    if cypher.startswith("```"):
        cypher = cypher.split("\n", 1)[-1]
    if cypher.endswith("```"):
        cypher = cypher[: -3].rstrip()
    return cypher.strip()


def query_traditional_rag(question):
    query_emb = embed_query(question)
    results = search_similar(query_emb, top_k=6)

    context_parts = []
    for r in results:
        context_parts.append(f"[{r['season_title']}] (similarity: {r['similarity']:.3f})\n{r['content']}")
    context = "\n\n---\n\n".join(context_parts)

    answer = chat(
        TRAD_RAG_SYSTEM,
        f"Context:\n{context}\n\nQuestion: {question}",
    )
    return answer, results


def query_graph_rag(question):
    system_prompt = _build_cypher_system_prompt()

    cypher = _clean_cypher(chat(system_prompt, question))

    last_error = None
    graph_results = None
    for attempt in range(1 + CYPHER_MAX_RETRIES):
        try:
            graph_results = run_query(cypher)
            if graph_results:
                break
            log.warning("Cypher attempt %d returned 0 rows: %s", attempt + 1, cypher)
            if attempt < CYPHER_MAX_RETRIES:
                empty_prompt = (
                    f"The following Cypher query returned no results:\n{cypher}\n\n"
                    f"Original question: {question}\n\n"
                    f"The query may be using property values that don't exist in the data "
                    f"or filtering too aggressively. Relax filters or try a different approach. "
                    f"Return ONLY the corrected Cypher, nothing else."
                )
                cypher = _clean_cypher(chat(system_prompt, empty_prompt))
        except Exception as e:
            last_error = e
            log.warning("Cypher attempt %d failed: %s\nQuery: %s", attempt + 1, e, cypher)
            if attempt < CYPHER_MAX_RETRIES:
                repair_prompt = (
                    f"The following Cypher query failed:\n{cypher}\n\n"
                    f"Error: {e}\n\n"
                    f"Original question: {question}\n\n"
                    f"Fix the query. Return ONLY the corrected Cypher, nothing else."
                )
                cypher = _clean_cypher(chat(system_prompt, repair_prompt))

    if graph_results is None and last_error is not None:
        return f"Cypher query failed after {1 + CYPHER_MAX_RETRIES} attempts: {last_error}\n\nLast query:\n{cypher}", cypher, []

    if graph_results is None:
        graph_results = []

    results_str = ""
    if graph_results:
        for i, row in enumerate(graph_results[:50]):
            results_str += f"Row {i+1}: {row}\n"
    else:
        results_str = "(no results)"

    answer = chat(
        GRAPH_RAG_SYSTEM,
        f"Query results:\n{results_str}\n\nQuestion: {question}",
    )
    return answer, cypher, graph_results


def print_divider(char="=", width=80):
    print(char * width)


def print_wrapped(text, indent=2, width=76):
    for line in text.split("\n"):
        wrapped = textwrap.fill(line, width=width, initial_indent=" " * indent, subsequent_indent=" " * indent)
        print(wrapped)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Run only first N questions (smoke test)")
    args = parser.parse_args()

    runs_dir = Path(__file__).resolve().parent.parent / "data" / "demo_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_path = runs_dir / f"{timestamp}.jsonl"
    log.info("Results will be persisted to %s", run_path)

    print_divider()
    print("  SURVIVOR: Traditional RAG vs Graph RAG — Side-by-Side Demo")
    print_divider()

    questions_run = 0
    for category in DEMO_QUESTIONS:
        print(f"\n{'':>2}--- {category['category']} ---\n")

        for question in category["questions"]:
            if args.limit is not None and questions_run >= args.limit:
                log.info("Stopping after %d questions (--limit)", args.limit)
                break
            questions_run += 1
            print_divider("-")
            print(f"  Q: {question}")
            print_divider("-")

            trad_record = {"question": question, "mode": "traditional_rag", "answer": None, "chunks": [], "time_s": None}
            graph_record = {"question": question, "mode": "graph_rag", "answer": None, "cypher": None, "graph_rows": 0, "time_s": None}

            print("\n  [Traditional RAG]")
            t0 = time.time()
            try:
                trad_answer, trad_context = query_traditional_rag(question)
                trad_time = time.time() - t0
                print(f"  Time: {trad_time:.1f}s | Chunks retrieved: {len(trad_context)}")
                print_wrapped(trad_answer)
                log.info("Traditional RAG answer for '%s': %s", question, trad_answer[:200])
                trad_record["answer"] = trad_answer
                trad_record["time_s"] = round(trad_time, 2)
                trad_record["chunks"] = [
                    {"season_title": c["season_title"], "similarity": round(c["similarity"], 4)}
                    for c in trad_context
                ]
            except Exception as e:
                log.exception("Traditional RAG failed")
                print(f"  Error: {e}")
                trad_record["answer"] = f"ERROR: {e}"

            print(f"\n  [Graph RAG]")
            t0 = time.time()
            try:
                graph_answer, cypher, graph_results = query_graph_rag(question)
                graph_time = time.time() - t0
                result_count = len(graph_results) if isinstance(graph_results, list) else 0
                print(f"  Time: {graph_time:.1f}s | Graph rows: {result_count}")
                print(f"  Cypher: {cypher[:120]}{'...' if len(cypher) > 120 else ''}")
                print_wrapped(graph_answer)
                log.info("Graph RAG cypher for '%s': %s", question, cypher)
                log.info("Graph RAG answer for '%s': %s", question, graph_answer[:200])
                graph_record["answer"] = graph_answer
                graph_record["cypher"] = cypher
                graph_record["graph_rows"] = result_count
                graph_record["time_s"] = round(graph_time, 2)
            except Exception as e:
                log.exception("Graph RAG failed")
                print(f"  Error: {e}")
                graph_record["answer"] = f"ERROR: {e}"

            with open(run_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(trad_record, ensure_ascii=False) + "\n")
                f.write(json.dumps(graph_record, ensure_ascii=False) + "\n")

            print()

        if args.limit is not None and questions_run >= args.limit:
            break

    print_divider()
    print("  Demo complete.")
    print_divider()
    log.info("All results saved to %s", run_path)


if __name__ == "__main__":
    main()
