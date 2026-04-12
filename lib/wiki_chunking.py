def chunk_sections(sections, doc_id, chunk_size_chars=2000, overlap_chars=200):
    chunks = []
    global_idx = 0

    for section in sections:
        heading = section["heading"]
        text = section["text"]
        if not text:
            continue

        start = 0
        while start < len(text):
            end = min(start + chunk_size_chars, len(text))

            if end < len(text):
                boundary = text.rfind(" ", start, end)
                if boundary > start:
                    end = boundary

            chunk_text = text[start:end].strip()
            if chunk_text:
                chunks.append({
                    "chunk_id": f"{doc_id}#{global_idx:04d}",
                    "doc_id": doc_id,
                    "text": chunk_text,
                    "section": heading,
                    "idx": global_idx,
                    "char_start": start,
                    "char_end": end,
                })
                global_idx += 1

            advance = end - start
            if advance <= overlap_chars:
                start = end
            else:
                start += advance - overlap_chars

    return chunks
