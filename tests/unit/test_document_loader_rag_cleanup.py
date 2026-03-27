import asyncio
from pathlib import Path

from langchain_core.documents import Document

from app.rag.document_loader import DocumentLoader


def test_pdf_loader_uses_fallback_when_primary_extraction_is_near_empty(monkeypatch, tmp_path: Path):
    loader = DocumentLoader()
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%fake\n")

    class FakePyPDFLoader:
        def __init__(self, filepath):  # noqa: ANN001
            self.filepath = filepath

        def load(self):
            return [Document(page_content=" 1 ", metadata={"source": self.filepath, "page": 0})]

    import langchain_community.document_loaders as lc_loaders

    monkeypatch.setattr(lc_loaders, "PyPDFLoader", FakePyPDFLoader)
    monkeypatch.setattr(
        loader,
        "_load_pdf_fallback",
        lambda *, filepath, metadata: [
            Document(
                page_content="Meaningful extracted fallback text for retrieval.",
                metadata={"source": filepath, **(metadata or {})},
            )
        ],
    )

    docs = asyncio.run(loader.load_pdf(str(pdf_path), metadata={"file_id": "f-1"}))
    assert docs
    assert "Meaningful extracted fallback text" in docs[0].page_content
    assert docs[0].metadata["file_id"] == "f-1"


def test_load_file_accepts_short_txt_content(tmp_path: Path):
    loader = DocumentLoader()
    txt_path = tmp_path / "short.txt"
    txt_path.write_text("ok", encoding="utf-8")

    docs = asyncio.run(loader.load_file(str(txt_path), metadata={"file_id": "txt-1"}))
    assert docs
    assert docs[0].metadata["file_type"] == "txt"
    assert docs[0].metadata["file_id"] == "txt-1"


def test_load_file_accepts_markdown_content(tmp_path: Path):
    loader = DocumentLoader()
    md_path = tmp_path / "note.md"
    md_path.write_text("# Title\n\nsmall body", encoding="utf-8")

    docs = asyncio.run(loader.load_file(str(md_path), metadata={"file_id": "md-1"}))
    assert docs
    assert docs[0].metadata["file_type"] == "md"
    assert docs[0].metadata["file_id"] == "md-1"


def test_load_file_routes_docx_and_preserves_metadata(tmp_path: Path):
    loader = DocumentLoader()
    docx_path = tmp_path / "memo.docx"
    docx_path.write_text("placeholder", encoding="utf-8")

    async def fake_docx_loader(filepath: str, metadata):  # noqa: ANN001
        return [Document(page_content="docx payload", metadata=dict(metadata or {}))]

    loader.supported_loaders[".docx"] = fake_docx_loader

    docs = asyncio.run(loader.load_file(str(docx_path), metadata={"file_id": "docx-1"}))
    assert docs
    assert docs[0].metadata["file_id"] == "docx-1"
    assert docs[0].metadata["file_type"] == "docx"
    assert docs[0].metadata["chunk_type"] == "extracted_text"
