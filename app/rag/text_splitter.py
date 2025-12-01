# app/rag/text_splitter.py
import logging
from typing import List, Dict, Any
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from app.core.config import settings

logger = logging.getLogger(__name__)

MIN_CHUNK_SIZE = 50
DEFAULT_CHUNK_SIZE = getattr(settings, "CHUNK_SIZE", 2000) or 2000  # ИСПРАВЛЕНИЕ: увеличено с 1000 до 2000
DEFAULT_CHUNK_OVERLAP = getattr(settings, "CHUNK_OVERLAP", 400) or 400  # ИСПРАВЛЕНИЕ: увеличено с 200 до 400


class SmartTextSplitter:
    def __init__(
            self,
            chunk_size: int = None,
            chunk_overlap: int = None,
            separators: List[str] = None
    ):
        raw_chunk_size = chunk_size if chunk_size is not None else DEFAULT_CHUNK_SIZE
        raw_chunk_overlap = chunk_overlap if chunk_overlap is not None else DEFAULT_CHUNK_OVERLAP

        try:
            raw_chunk_size = int(raw_chunk_size)
        except Exception:
            logger.warning(f"chunk_size={raw_chunk_size} is not int, fallback to {DEFAULT_CHUNK_SIZE}")
            raw_chunk_size = DEFAULT_CHUNK_SIZE

        if raw_chunk_size < MIN_CHUNK_SIZE:
            logger.warning(f"chunk_size={raw_chunk_size} < {MIN_CHUNK_SIZE}, bumping to {MIN_CHUNK_SIZE}")
            raw_chunk_size = MIN_CHUNK_SIZE

        try:
            raw_chunk_overlap = int(raw_chunk_overlap)
        except Exception:
            logger.warning(f"chunk_overlap={raw_chunk_overlap} is not int, fallback to {DEFAULT_CHUNK_OVERLAP}")
            raw_chunk_overlap = DEFAULT_CHUNK_OVERLAP

        if raw_chunk_overlap >= raw_chunk_size:
            adjusted = max(0, raw_chunk_size // 4)
            logger.warning(
                f"chunk_overlap={raw_chunk_overlap} >= chunk_size={raw_chunk_size}, "
                f"reducing overlap to {adjusted}"
            )
            raw_chunk_overlap = adjusted

        self.chunk_size = raw_chunk_size
        self.chunk_overlap = raw_chunk_overlap
        self.separators = separators or ["\n\n", "\n", ". ", " ", ""]

        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.chunk_size,
            chunk_overlap=self.chunk_overlap,
            separators=self.separators,
            length_function=len,
            is_separator_regex=False
        )

        logger.info(
            f"✅ SmartTextSplitter initialized: "
            f"chunk_size={self.chunk_size}, overlap={self.chunk_overlap}"
        )

    def split_text(self, text: str) -> List[str]:
        """Разбить текст на chunks"""
        try:
            if not text or not text.strip():
                logger.warning("⚠️ Empty text provided for splitting")
                return []

            logger.info(f"🔪 Splitting text ({len(text)} chars)...")
            chunks = self.text_splitter.split_text(text)
            logger.info(f"✅ Text split into {len(chunks)} chunks")
            return chunks

        except Exception as e:
            logger.error(f"❌ Error splitting text: {e}")
            raise

    def split_documents(self, documents: List[Document]) -> List[Document]:
        """
        ИСПРАВЛЕНО: Разбить список Document объектов с учетом типа файла
        Для Excel файлов используется специальная логика
        """
        try:
            if not documents:
                logger.warning("⚠️ Empty documents list provided")
                return []

            logger.info(f"🔪 Splitting {len(documents)} documents...")
            all_chunks = []

            for doc_idx, doc in enumerate(documents):
                file_type = doc.metadata.get('file_type', '')

                # ИСПРАВЛЕНИЕ 4: Специальная обработка для Excel
                if file_type in ['xlsx', 'xls', 'csv']:
                    logger.info(f"📊 Using optimized splitting for {file_type} file")
                    # Для табличных данных используем увеличенный chunk_size
                    excel_splitter = RecursiveCharacterTextSplitter(
                        chunk_size=self.chunk_size * 3,  # Увеличиваем в 3 раза
                        chunk_overlap=self.chunk_overlap,
                        separators=["\n===", "\n---", "\n\n", "\n"],  # Разбиваем по логическим блокам
                        length_function=len
                    )
                    text_chunks = excel_splitter.split_text(doc.page_content)
                else:
                    # Стандартная разбивка для других типов
                    text_chunks = self.text_splitter.split_text(doc.page_content)

                # Создать Document объекты для каждого chunk
                for chunk_idx, chunk_text in enumerate(text_chunks):
                    metadata = doc.metadata.copy()
                    metadata.update({
                        'chunk_index': chunk_idx,
                        'total_chunks': len(text_chunks),
                        'doc_index': doc_idx,
                        'chunk_size': len(chunk_text)
                    })

                    chunk_doc = Document(
                        page_content=chunk_text,
                        metadata=metadata
                    )
                    all_chunks.append(chunk_doc)

            logger.info(f"✅ Created {len(all_chunks)} document chunks")
            return all_chunks

        except Exception as e:
            logger.error(f"❌ Error splitting documents: {e}")
            raise

    def create_documents_from_text(
            self,
            text: str,
            metadata: Dict[str, Any] = None
    ) -> List[Document]:
        """Создать список Document объектов из текста с метаданными"""
        try:
            if not text or not text.strip():
                logger.warning("⚠️ Empty text provided")
                return []

            metadata = metadata or {}
            logger.info(f"📄 Creating documents from text ({len(text)} chars)...")

            chunks = self.split_text(text)

            documents = []
            for idx, chunk in enumerate(chunks):
                doc_metadata = metadata.copy()
                doc_metadata.update({
                    'chunk_index': idx,
                    'total_chunks': len(chunks),
                    'chunk_size': len(chunk)
                })

                doc = Document(
                    page_content=chunk,
                    metadata=doc_metadata
                )
                documents.append(doc)

            logger.info(f"✅ Created {len(documents)} documents")
            return documents

        except Exception as e:
            logger.error(f"❌ Error creating documents: {e}")
            raise

    def split_by_file_type(
            self,
            text: str,
            file_type: str,
            metadata: Dict[str, Any] = None
    ) -> List[Document]:
        """
        ИСПРАВЛЕНО: Разбить текст с учетом типа файла
        """
        try:
            metadata = metadata or {}
            metadata['file_type'] = file_type

            # ИСПРАВЛЕНИЕ 5: Улучшенная обработка для разных типов
            if file_type in ['csv', 'xlsx', 'xls']:
                # Для табличных данных - значительно больший chunk size
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size * 3,  # Увеличено с *2 до *3
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n===", "\n---", "\n\n", "\n"],
                    length_function=len
                )
                chunks = splitter.split_text(text)

            elif file_type == 'json':
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n\n", "\n", ",", " "],
                    length_function=len
                )
                chunks = splitter.split_text(text)

            elif file_type == 'md':
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n## ", "\n### ", "\n\n", "\n", ". ", " "],
                    length_function=len
                )
                chunks = splitter.split_text(text)

            else:
                chunks = self.split_text(text)

            documents = []
            for idx, chunk in enumerate(chunks):
                doc_metadata = metadata.copy()
                doc_metadata.update({
                    'chunk_index': idx,
                    'total_chunks': len(chunks),
                    'chunk_size': len(chunk)
                })

                doc = Document(
                    page_content=chunk,
                    metadata=doc_metadata
                )
                documents.append(doc)

            logger.info(
                f"✅ Split {file_type} file into {len(documents)} chunks "
                f"(avg size: {sum(len(c) for c in chunks) // len(chunks) if chunks else 0})"
            )
            return documents

        except Exception as e:
            logger.error(f"❌ Error splitting by file type: {e}")
            raise

    def get_chunk_stats(self, documents: List[Document]) -> Dict[str, Any]:
        """Получить статистику по chunks"""
        if not documents:
            return {
                'total_chunks': 0,
                'avg_chunk_size': 0,
                'min_chunk_size': 0,
                'max_chunk_size': 0,
                'total_size': 0
            }

        chunk_sizes = [len(doc.page_content) for doc in documents]

        stats = {
            'total_chunks': len(documents),
            'avg_chunk_size': sum(chunk_sizes) // len(chunk_sizes),
            'min_chunk_size': min(chunk_sizes),
            'max_chunk_size': max(chunk_sizes),
            'total_size': sum(chunk_sizes),
            'overlap_ratio': self.chunk_overlap / self.chunk_size
        }

        logger.info(f"📊 Chunk statistics: {stats}")
        return stats
