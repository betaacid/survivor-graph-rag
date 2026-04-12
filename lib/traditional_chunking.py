def chunk_word_windows(text, chunk_size=800, overlap=200):
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


def chunk_text(text, chunk_size=800, overlap=200):
    return chunk_word_windows(text, chunk_size=chunk_size, overlap=overlap)
