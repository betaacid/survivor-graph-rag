import logging

from lib.embeddings import embed_query
from lib.llm import chat
from lib.neo4j_client import get_graph_schema, run_query
from lib.pg_client import search_similar

log = logging.getLogger(__name__)

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
    (
        "Who was eliminated in each episode of Survivor 41?",
        "MATCH (e:Episode {season_number: 41})-[:ELIMINATED]->(ps:PlayerSeason) RETURN e.episode_number, ps.player_name ORDER BY e.episode_number",
    ),
    (
        "Who won reward challenges in Survivor 41?",
        "MATCH (e:Episode {season_number: 41})-[:REWARD_WON_BY]->(ps:PlayerSeason) RETURN e.episode_number, ps.player_name ORDER BY e.episode_number",
    ),
    (
        "Who attended the most tribal councils in Survivor 41?",
        "MATCH (ps:PlayerSeason {season_number: 41})-[:ATTENDED_TRIBAL]->(tc:TribalCouncil) RETURN ps.player_name, count(tc) AS tribals_attended ORDER BY tribals_attended DESC LIMIT 10",
    ),
    (
        "For each winner, how many jury votes did they receive?",
        "MATCH (juror:PlayerSeason)-[:JURY_VOTE_FOR]->(winner:PlayerSeason) WHERE winner.exit_type = 'winner' RETURN winner.player_name, winner.season_number, count(juror) AS jury_votes ORDER BY winner.season_number",
    ),
    (
        "Which tribe went to tribal council in episode 3 of Survivor 41?",
        "MATCH (e:Episode {season_number: 41, episode_number: 3})-[:TRIBAL_COUNCIL_FOR]->(t:Tribe) RETURN t.name",
    ),
]

TERMINOLOGY_MAP = """Terminology mappings:
- "winner" / "won the season" -> PlayerSeason node where exit_type = 'winner'
- "runner-up" -> PlayerSeason node where exit_type = 'runner_up'
- "voted out" / "eliminated" -> exit_type = 'voted_out' on PlayerSeason, or (Episode)-[:ELIMINATED]->(PlayerSeason) to find WHO was eliminated in a specific episode
- "immunity win" / "won immunity" -> (Episode)-[:IMMUNITY_WON_BY]->(PlayerSeason) relationship (NOT a property on any node)
- "reward win" / "won reward" / "reward challenge" -> (Episode)-[:REWARD_WON_BY]->(PlayerSeason) relationship (NOT a property on any node)
- "tribe" / "team" -> (:Tribe) node accessed via [:MEMBER_OF] from PlayerSeason
- "original tribe" / "starting tribe" -> Tribe node where phase = 'premerge', accessed via [:MEMBER_OF]
- "merged tribe" / "post-merge tribe" -> Tribe node where phase = 'merged', accessed via [:MEMBER_OF]
- "season N" / "Survivor N" -> Season.number = N or PlayerSeason.season_number = N
- "jury member" -> PlayerSeason.jury_member property (boolean)
- "jury vote" / "voted for the winner" / "final vote" -> [:JURY_VOTE_FOR] relationship from juror PlayerSeason to finalist PlayerSeason
- "medevac" / "medical evacuation" -> exit_type = 'medevac'
- "quit" -> exit_type = 'quit'
- "returning player" / "played multiple times" -> Player with multiple [:PLAYED_IN] relationships
- "voted for" (during tribal council) -> [:CAST_VOTE] relationship between PlayerSeason nodes (has episode_number property)
- "tribal council attendance" / "attended tribal" / "went to tribal" -> [:ATTENDED_TRIBAL] from PlayerSeason to TribalCouncil node
- "which tribe went to tribal" -> (Episode)-[:TRIBAL_COUNCIL_FOR]->(Tribe)"""

FORMAT_INSTRUCTIONS = """Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
Do not wrap the output in code blocks or backticks."""

CYPHER_MAX_RETRIES = 2

_STATIC_SCHEMA = """Node labels and properties:
  Season {title: STRING, number: INTEGER}
  Player {name: STRING}
  PlayerSeason {player_name: STRING, season_number: INTEGER, age: INTEGER, hometown: STRING, placement: INTEGER, day_out: INTEGER, exit_type: STRING, jury_member: BOOLEAN}
  Episode {season_number: INTEGER, episode_number: INTEGER, title: STRING, air_date: STRING, viewers_millions: FLOAT}
  Tribe {name: STRING, season_number: INTEGER, phase: STRING}
  TribalCouncil {season_number: INTEGER, episode_number: INTEGER}
Relationship types and properties:
  HAS_EPISODE
  HAS_TRIBE
  HAS_TRIBAL
  PLAYED_IN
  IN_SEASON
  MEMBER_OF
  ATTENDED_TRIBAL
  ELIMINATED
  IMMUNITY_WON_BY
  REWARD_WON_BY
  TRIBAL_COUNCIL_FOR
  CAST_VOTE {episode_number: INTEGER}
  JURY_VOTE_FOR
The relationships:
  (:Season)-[:HAS_EPISODE]->(:Episode)
  (:Season)-[:HAS_TRIBE]->(:Tribe)
  (:Player)-[:PLAYED_IN]->(:PlayerSeason)
  (:PlayerSeason)-[:IN_SEASON]->(:Season)
  (:PlayerSeason)-[:MEMBER_OF]->(:Tribe)
  (:PlayerSeason)-[:ATTENDED_TRIBAL]->(:TribalCouncil)
  (:PlayerSeason)-[:CAST_VOTE]->(:PlayerSeason)
  (:PlayerSeason)-[:JURY_VOTE_FOR]->(:PlayerSeason)
  (:Episode)-[:HAS_TRIBAL]->(:TribalCouncil)
  (:Episode)-[:ELIMINATED]->(:PlayerSeason)
  (:Episode)-[:IMMUNITY_WON_BY]->(:PlayerSeason)
  (:Episode)-[:REWARD_WON_BY]->(:PlayerSeason)
  (:Episode)-[:TRIBAL_COUNCIL_FOR]->(:Tribe)"""


def build_cypher_system_prompt(schema=None):
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


def clean_cypher(raw):
    cypher = raw.strip().strip("`").strip()
    if cypher.lower().startswith("cypher"):
        cypher = cypher[6:].strip()
    if cypher.startswith("```"):
        cypher = cypher.split("\n", 1)[-1]
    if cypher.endswith("```"):
        cypher = cypher[:-3].rstrip()
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
    system_prompt = build_cypher_system_prompt()

    cypher = clean_cypher(chat(system_prompt, question))

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
                cypher = clean_cypher(chat(system_prompt, empty_prompt))
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
                cypher = clean_cypher(chat(system_prompt, repair_prompt))

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
