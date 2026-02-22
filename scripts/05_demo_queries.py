import sys
import textwrap
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from lib.embeddings import embed_query
from lib.llm import chat
from lib.neo4j_client import run_query
from lib.pg_client import search_similar

DEMO_QUESTIONS = [
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
Use these results to answer the question precisely. If the results are empty, say that the data wasn't found in the graph.
Format numbers, lists, and tables clearly."""

CYPHER_SYSTEM = """You are a Neo4j Cypher query expert for a Survivor TV show graph database.

The schema is:
Nodes:
- Season {title, number, location, days, num_castaways, num_episodes, winner, runner_up, air_start, air_end}
- Player {name}
- PlayerSeason {player_name, season_number, age, hometown, placement, day_out, exit_type, jury_member, starting_tribe}
- Episode {season_number, episode_number, title, air_date, viewers_millions}
- Tribe {name, season_number, phase}

Relationships:
- (Season)-[:HAS_EPISODE]->(Episode)
- (Season)-[:HAS_TRIBE]->(Tribe)
- (Player)-[:PLAYED_IN]->(PlayerSeason)-[:IN_SEASON]->(Season)
- (PlayerSeason)-[:MEMBER_OF]->(Tribe)
- (Episode)-[:ELIMINATED]->(PlayerSeason)
- (Episode)-[:IMMUNITY_WON_BY]->(PlayerSeason)
- (Episode)-[:REWARD_WON_BY]->(PlayerSeason)
- (PlayerSeason)-[:CAST_VOTE {episode_number}]->(PlayerSeason)

Write a Cypher query to answer the user's question. Return ONLY the Cypher query, no explanation.
Keep results to a reasonable size (use LIMIT if the result set could be large).
Use OPTIONAL MATCH when a path might not exist for all nodes."""


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
    cypher = chat(CYPHER_SYSTEM, question)
    cypher = cypher.strip().strip("`").strip()
    if cypher.lower().startswith("cypher"):
        cypher = cypher[6:].strip()

    try:
        graph_results = run_query(cypher)
    except Exception as e:
        return f"Cypher query failed: {e}\n\nGenerated query:\n{cypher}", cypher, []

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
    print_divider()
    print("  SURVIVOR: Traditional RAG vs Graph RAG — Side-by-Side Demo")
    print_divider()

    for category in DEMO_QUESTIONS:
        print(f"\n{'':>2}--- {category['category']} ---\n")

        for question in category["questions"]:
            print_divider("-")
            print(f"  Q: {question}")
            print_divider("-")

            print("\n  [Traditional RAG]")
            t0 = time.time()
            try:
                trad_answer, trad_context = query_traditional_rag(question)
                trad_time = time.time() - t0
                print(f"  Time: {trad_time:.1f}s | Chunks retrieved: {len(trad_context)}")
                print_wrapped(trad_answer)
            except Exception as e:
                print(f"  Error: {e}")

            print(f"\n  [Graph RAG]")
            t0 = time.time()
            try:
                graph_answer, cypher, graph_results = query_graph_rag(question)
                graph_time = time.time() - t0
                result_count = len(graph_results) if isinstance(graph_results, list) else 0
                print(f"  Time: {graph_time:.1f}s | Graph rows: {result_count}")
                print(f"  Cypher: {cypher[:120]}{'...' if len(cypher) > 120 else ''}")
                print_wrapped(graph_answer)
            except Exception as e:
                print(f"  Error: {e}")

            print()

    print_divider()
    print("  Demo complete.")
    print_divider()


if __name__ == "__main__":
    main()
