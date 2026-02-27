import logging

from lib.embeddings import embed_query
from lib.llm import chat
from lib.pg_client import search_similar

log = logging.getLogger(__name__)

TRAD_RAG_SYSTEM = """You are a Survivor TV show expert answering questions using only the provided context passages.
If the context doesn't contain enough information to answer confidently, say so.
Be specific and cite which season/episode when relevant."""


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
