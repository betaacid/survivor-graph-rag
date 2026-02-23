import time

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from lib.neo4j_client import get_node_counts
from lib.query import DEMO_QUESTIONS, query_graph_rag, query_traditional_rag

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
                except Exception as e:
                    st.error(f"Graph RAG failed: {e}")
