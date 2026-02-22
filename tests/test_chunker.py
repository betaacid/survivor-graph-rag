from lib.chunker import chunk_text


def test_chunk_text_splits_with_overlap():
    text = " ".join([f"w{i}" for i in range(50)])
    chunks = chunk_text(text, chunk_size=10, overlap=2)
    assert len(chunks) >= 6
    assert chunks[0].split()[-2:] == chunks[1].split()[:2]


def test_chunk_text_short_input_single_chunk():
    text = "alpha beta gamma"
    chunks = chunk_text(text, chunk_size=10, overlap=2)
    assert chunks == ["alpha beta gamma"]
