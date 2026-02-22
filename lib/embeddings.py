import os

from openai import OpenAI

_client = None
MODEL = "text-embedding-3-small"
DIMENSIONS = 1536


def get_client():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client


def embed_texts(texts, batch_size=100):
    client = get_client()
    all_embeddings = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        resp = client.embeddings.create(model=MODEL, input=batch)
        all_embeddings.extend([d.embedding for d in resp.data])
    return all_embeddings


def embed_query(text):
    return embed_texts([text])[0]
