import time

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dotenv import load_dotenv
from pyvis.network import Network

load_dotenv()

from lib.agentic_rag import query_agentic_rag
from lib.demo_questions import DEMO_QUESTIONS
from lib.graph_rag import query_graph_rag
from lib.neo4j_client import fetch_subgraph_for_results, get_node_counts
from lib.traditional_rag import query_traditional_rag

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


def build_pyvis(nodes_data, edges_data, height="500px", width="100%"):
    net = Network(height=height, width=width, directed=True, bgcolor="#ffffff", font_color="#333333")
    net.barnes_hut(gravity=-3000, central_gravity=0.3, spring_length=150)
    seen_edges = set()

    for n in nodes_data:
        label = n["labels"][0] if n["labels"] else "Unknown"
        props = n.get("props", {})
        display_key = NODE_DISPLAY_KEY.get(label)
        display = str(props.get(display_key, "")) if display_key else ""
        if not display:
            display = str(props.get("name", props.get("title", props.get("player_name", n["id"]))))

        tooltip_parts = [f"<b>{label}</b>"]
        for k, v in props.items():
            tooltip_parts.append(f"{k}: {v}")
        tooltip = "<br>".join(tooltip_parts)

        net.add_node(
            n["id"],
            label=display,
            title=tooltip,
            color=LABEL_COLORS.get(label, "#95a5a6"),
            size=20,
        )

    for e in edges_data:
        edge_key = (e["source"], e["target"], e["type"])
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)
        net.add_edge(e["source"], e["target"], title=e["type"], label=e["type"])

    html = net.generate_html()
    html = html.replace("<body>", "<body style='margin:0; padding:0;'>")
    return html


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
if "results" not in st.session_state:
    st.session_state.results = None


def set_question(q):
    st.session_state.question = q


with st.sidebar:
    st.title("Survivor RAG")
    st.caption("Traditional, Graph, and Agentic RAG")

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
    ["All", "Traditional RAG", "Graph RAG", "Agentic RAG"],
    horizontal=True,
)

run = st.button("Run", type="primary", disabled=not question.strip())

if run and question.strip():
    run_trad = mode in ("All", "Traditional RAG")
    run_graph = mode in ("All", "Graph RAG")
    run_agentic = mode in ("All", "Agentic RAG")
    st.session_state.results = {
        "question": question,
        "mode": mode,
        "trad_answer": None,
        "trad_chunks": None,
        "trad_elapsed": None,
        "graph_answer": None,
        "cypher": None,
        "graph_rows": None,
        "graph_elapsed": None,
        "agentic_answer": None,
        "agentic_steps": None,
        "agentic_elapsed": None,
    }
    if run_trad:
        with st.spinner("Searching chunks & generating answer..."):
            t0 = time.time()
            try:
                trad_answer, trad_chunks = query_traditional_rag(question)
                st.session_state.results["trad_answer"] = trad_answer
                st.session_state.results["trad_chunks"] = trad_chunks
                st.session_state.results["trad_elapsed"] = time.time() - t0
            except Exception as e:
                st.session_state.results["trad_error"] = str(e)
    if run_graph:
        with st.spinner("Generating Cypher & querying graph..."):
            t0 = time.time()
            try:
                graph_answer, cypher, graph_rows = query_graph_rag(question)
                st.session_state.results["graph_answer"] = graph_answer
                st.session_state.results["cypher"] = cypher
                st.session_state.results["graph_rows"] = graph_rows
                st.session_state.results["graph_elapsed"] = time.time() - t0
            except Exception as e:
                st.session_state.results["graph_error"] = str(e)
    if run_agentic:
        with st.spinner("Agentic RAG: rewriting, routing, querying..."):
            t0 = time.time()
            try:
                agentic_answer, agentic_steps = query_agentic_rag(question)
                st.session_state.results["agentic_answer"] = agentic_answer
                st.session_state.results["agentic_steps"] = agentic_steps
                st.session_state.results["agentic_elapsed"] = time.time() - t0
            except Exception as e:
                st.session_state.results["agentic_error"] = str(e)

if st.session_state.results and st.session_state.results.get("question") == question:
    r = st.session_state.results
    run_trad = r["mode"] in ("All", "Traditional RAG")
    run_graph = r["mode"] in ("All", "Graph RAG")
    run_agentic = r["mode"] in ("All", "Agentic RAG")

    if r["mode"] == "All":
        col_trad, col_graph, col_agentic = st.columns(3)
    else:
        col_trad = col_graph = col_agentic = st.container()

    if run_trad:
        with col_trad:
            st.subheader("Traditional RAG")
            if r.get("trad_error"):
                st.error(f"Traditional RAG failed: {r['trad_error']}")
            else:
                st.markdown(r["trad_answer"])
                st.caption(f"{r['trad_elapsed']:.1f}s  |  {len(r['trad_chunks'])} chunks retrieved")
                with st.expander("Retrieved context chunks"):
                    for i, chunk in enumerate(r["trad_chunks"]):
                        st.markdown(
                            f"**Chunk {i+1}** — {chunk['season_title']} "
                            f"(similarity: {chunk['similarity']:.3f})"
                        )
                        st.text(chunk["content"][:500])
                        if i < len(r["trad_chunks"]) - 1:
                            st.divider()

    if run_graph:
        with col_graph:
            st.subheader("Graph RAG")
            if r.get("graph_error"):
                st.error(f"Graph RAG failed: {r['graph_error']}")
            else:
                st.markdown(r["graph_answer"])
                row_count = len(r["graph_rows"]) if isinstance(r["graph_rows"], list) else 0
                st.caption(f"{r['graph_elapsed']:.1f}s  |  {row_count} rows returned")
                st.code(r["cypher"], language="cypher")
                if r["graph_rows"]:
                    with st.expander(f"Raw graph results ({row_count} rows)"):
                        try:
                            df = pd.DataFrame(r["graph_rows"])
                            st.dataframe(df, width="stretch")
                        except Exception:
                            for i, row in enumerate(r["graph_rows"][:50]):
                                st.text(f"Row {i+1}: {row}")

                    st.checkbox("Show graph full-width", key="graph_viz_fullscreen")

    fullscreen = st.session_state.get("graph_viz_fullscreen", False)
    if run_graph and not r.get("graph_error") and r.get("graph_rows"):
        with st.spinner("Loading subgraph..."):
            sg_nodes, sg_edges = fetch_subgraph_for_results(r["graph_rows"])
        if sg_nodes:
            if fullscreen:
                st.divider()
                st.subheader("Graph Visualization")
                graph_html = build_pyvis(sg_nodes, sg_edges, height="90vh", width="100%")
                components.html(graph_html, height=900, scrolling=False)
            else:
                with col_graph:
                    st.markdown("**Graph Visualization**")
                    graph_html = build_pyvis(sg_nodes, sg_edges, height="500px", width="100%")
                    components.html(graph_html, height=520, scrolling=False)
        else:
            target = st if fullscreen else col_graph
            with target if not fullscreen else st.container():
                st.info("No graph structure to visualize for this query.")

    if run_agentic:
        with col_agentic:
            st.subheader("Agentic RAG")
            if r.get("agentic_error"):
                st.error(f"Agentic RAG failed: {r['agentic_error']}")
            elif r.get("agentic_answer"):
                st.markdown(r["agentic_answer"])
                st.caption(f"{r['agentic_elapsed']:.1f}s")

                steps = r.get("agentic_steps", [])
                with st.expander("Agent Trace"):
                    for step in steps:
                        stage = step.get("stage", "")
                        if stage == "rewriter":
                            st.markdown("**Rewriter**")
                            st.text(f"Original:  {step.get('original', '')}")
                            st.text(f"Rewritten: {step.get('rewritten', '')}")
                            st.divider()
                        elif stage == "router":
                            if step.get("error"):
                                st.error(f"Router error: {step['error']}")
                            else:
                                st.markdown(f"**Router** selected tool: `{step.get('tool', '')}`")
                                if step.get("args"):
                                    st.json(step["args"])
                                if step.get("cypher"):
                                    st.code(step["cypher"], language="cypher")
                                st.caption(f"{step.get('rows_returned', 0)} rows returned")
                            st.divider()
                        elif stage == "critic":
                            follow_ups = step.get("follow_ups", [])
                            if follow_ups:
                                st.markdown(f"**Critic** requested follow-ups: {len(follow_ups)}")
                                for fu in follow_ups:
                                    st.text(f"  - {fu}")
                            else:
                                st.markdown("**Critic** -- answer deemed complete")
                            st.divider()
                        elif stage == "critic_retry":
                            if step.get("error"):
                                st.error(f"Retry error: {step['error']}")
                            else:
                                st.markdown(f"**Retry** tool: `{step.get('tool', '')}`")
                                if step.get("cypher"):
                                    st.code(step["cypher"], language="cypher")
                                st.caption(f"{step.get('rows_returned', 0)} rows returned")
