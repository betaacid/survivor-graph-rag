import time

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit_agraph import Config, Edge, Node, agraph

load_dotenv()

from lib.neo4j_client import fetch_subgraph_for_results, get_node_counts
from lib.query import DEMO_QUESTIONS, query_graph_rag, query_traditional_rag

LABEL_COLORS = {
    "Season": "#e74c3c",
    "Player": "#3498db",
    "PlayerSeason": "#2ecc71",
    "Episode": "#f39c12",
    "Tribe": "#9b59b6",
}

NODE_DISPLAY_KEY = {
    "Season": "title",
    "Player": "name",
    "PlayerSeason": "player_name",
    "Episode": "episode_number",
    "Tribe": "name",
}


def build_agraph(nodes_data, edges_data):
    ag_nodes = []
    ag_edges = []
    seen_edges = set()

    for n in nodes_data:
        label = n["labels"][0] if n["labels"] else "Unknown"
        props = n.get("props", {})
        display_key = NODE_DISPLAY_KEY.get(label)
        title = str(props.get(display_key, "")) if display_key else ""
        if not title:
            title = str(props.get("name", props.get("title", props.get("player_name", n["id"]))))

        tooltip_parts = [f"{label}"]
        for k, v in props.items():
            tooltip_parts.append(f"  {k}: {v}")
        tooltip = "\n".join(tooltip_parts)

        ag_nodes.append(Node(
            id=n["id"],
            label=title,
            size=25,
            color=LABEL_COLORS.get(label, "#95a5a6"),
            title=tooltip,
        ))

    for e in edges_data:
        edge_key = (e["source"], e["target"], e["type"])
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)
        ag_edges.append(Edge(
            source=e["source"],
            target=e["target"],
            label=e["type"],
        ))

    return ag_nodes, ag_edges

st.set_page_config(page_title="Survivor RAG", page_icon="🏝️", layout="wide")

st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; }
    div[data-testid="stExpander"] details summary p { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

if "question" not in st.session_state:
    st.session_state.question = ""


def set_question(q):
    st.session_state.question = q


with st.sidebar:
    st.title("Survivor RAG")
    st.caption("Traditional RAG vs Graph RAG")

    st.divider()
    st.subheader("Example Questions")

    for group in DEMO_QUESTIONS:
        with st.expander(group["category"]):
            for q in group["questions"]:
                st.button(q, key=f"ex_{q}", on_click=set_question, args=(q,), use_container_width=True)

    st.divider()
    st.subheader("Graph Stats")
    try:
        counts = get_node_counts()
        for label, cnt in sorted(counts.items()):
            st.metric(label, cnt)
    except Exception as e:
        st.error(f"Could not reach Neo4j: {e}")

st.title("Ask a Survivor Question")

question = st.text_input(
    "Enter your question",
    value=st.session_state.question,
    placeholder="e.g. Who won Survivor 41?",
    label_visibility="collapsed",
)

mode = st.radio(
    "Mode",
    ["Both (side-by-side)", "Traditional RAG", "Graph RAG"],
    horizontal=True,
)

run = st.button("Run", type="primary", disabled=not question.strip())

if run and question.strip():
    run_trad = mode in ("Both (side-by-side)", "Traditional RAG")
    run_graph = mode in ("Both (side-by-side)", "Graph RAG")

    if mode == "Both (side-by-side)":
        col_trad, col_graph = st.columns(2)
    else:
        col_trad = col_graph = st.container()

    if run_trad:
        with col_trad:
            st.subheader("Traditional RAG")
            with st.spinner("Searching chunks & generating answer..."):
                t0 = time.time()
                try:
                    trad_answer, trad_chunks = query_traditional_rag(question)
                    elapsed = time.time() - t0
                    st.markdown(trad_answer)
                    st.caption(f"{elapsed:.1f}s  |  {len(trad_chunks)} chunks retrieved")
                    with st.expander("Retrieved context chunks"):
                        for i, chunk in enumerate(trad_chunks):
                            st.markdown(
                                f"**Chunk {i+1}** — {chunk['season_title']} "
                                f"(similarity: {chunk['similarity']:.3f})"
                            )
                            st.text(chunk["content"][:500])
                            if i < len(trad_chunks) - 1:
                                st.divider()
                except Exception as e:
                    st.error(f"Traditional RAG failed: {e}")

    if run_graph:
        with col_graph:
            st.subheader("Graph RAG")
            with st.spinner("Generating Cypher & querying graph..."):
                t0 = time.time()
                try:
                    graph_answer, cypher, graph_rows = query_graph_rag(question)
                    elapsed = time.time() - t0
                    st.markdown(graph_answer)
                    row_count = len(graph_rows) if isinstance(graph_rows, list) else 0
                    st.caption(f"{elapsed:.1f}s  |  {row_count} rows returned")
                    st.code(cypher, language="cypher")
                    if graph_rows:
                        with st.expander(f"Raw graph results ({row_count} rows)"):
                            try:
                                df = pd.DataFrame(graph_rows)
                                st.dataframe(df, use_container_width=True)
                            except Exception:
                                for i, row in enumerate(graph_rows[:50]):
                                    st.text(f"Row {i+1}: {row}")

                        with st.expander("Graph Visualization", expanded=True):
                            with st.spinner("Loading subgraph..."):
                                sg_nodes, sg_edges = fetch_subgraph_for_results(graph_rows)
                            if sg_nodes:
                                ag_nodes, ag_edges = build_agraph(sg_nodes, sg_edges)
                                config = Config(
                                    width=700,
                                    height=500,
                                    directed=True,
                                    physics=True,
                                    hierarchical=False,
                                )
                                agraph(nodes=ag_nodes, edges=ag_edges, config=config)
                            else:
                                st.info("No graph structure to visualize for this query.")
                except Exception as e:
                    st.error(f"Graph RAG failed: {e}")
