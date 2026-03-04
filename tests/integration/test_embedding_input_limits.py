import asyncio

from app.rag.embeddings import EmbeddingsManager
from app.rag import embeddings as embeddings_module


def test_local_embedding_inputs_are_segmented_without_lossy_truncation(monkeypatch):
    seen_lengths = []

    async def fake_generate_embedding(*, text, model_source=None, model_name=None):  # noqa: ARG001
        seen_lengths.append(len(text))
        return [float(len(text)), 1.0, 2.0]

    monkeypatch.setattr(embeddings_module.settings, "OLLAMA_EMBED_MAX_INPUT_CHARS", 120)
    monkeypatch.setattr(embeddings_module.settings, "OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS", 20)
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDING_CONCURRENCY", 2)
    monkeypatch.setattr(embeddings_module.llm_manager, "generate_embedding", fake_generate_embedding)

    mgr = EmbeddingsManager(mode="local", model="nomic-embed-text:latest")
    result = asyncio.run(mgr.embedd_documents_async(["x" * 400, "ok"]))

    assert len(result) == 2
    # long text should be segmented: 120,120,120,100
    assert seen_lengths[:4] == [120, 120, 120, 100]
    assert seen_lengths[4] == 2
    # first embedding is mean pooled over segments
    assert abs(result[0][0] - 115.0) < 1e-6
