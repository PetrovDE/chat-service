# app/rag/text_splitter.py
import logging
from typing import List, Dict, Any
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from app.rag.config import rag_config

logger = logging.getLogger(__name__)


class SmartTextSplitter:
    """
    Умная разбивка текста на chunks с сохранением контекста
    Использует RecursiveCharacterTextSplitter для структурированной разбивки
    """

    def __init__(
            self,
            chunk_size: int = None,
            chunk_overlap: int = None,
            separators: List[str] = None
    ):
        """
        Инициализация text splitter

        Args:
            chunk_size: Размер chunk в символах (из config по умолчанию)
            chunk_overlap: Перекрытие между chunks (из config по умолчанию)
            separators: Список разделителей в порядке приоритета
        """
        self.chunk_size = chunk_size or rag_config.chunk_size
        self.chunk_overlap = chunk_overlap or rag_config.chunk_overlap
        self.separators = separators or ["\n\n", "\n", ". ", " ", ""]

        # Создать основной splitter
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
        """
        Разбить текст на chunks

        Args:
            text: Исходный текст

        Returns:
            Список текстовых chunks
        """
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
        Разбить список Document объектов на chunks
        Сохраняет метаданные из оригинальных документов

        Args:
            documents: Список LangChain Document объектов

        Returns:
            Список Document chunks с сохраненными метаданными
        """
        try:
            if not documents:
                logger.warning("⚠️ Empty documents list provided")
                return []

            logger.info(f"🔪 Splitting {len(documents)} documents...")

            all_chunks = []

            for doc_idx, doc in enumerate(documents):
                # Разбить текст документа
                text_chunks = self.text_splitter.split_text(doc.page_content)

                # Создать Document объекты для каждого chunk
                for chunk_idx, chunk_text in enumerate(text_chunks):
                    # Копировать метаданные из оригинала
                    metadata = doc.metadata.copy()

                    # Добавить информацию о chunk
                    metadata.update({
                        'chunk_index': chunk_idx,
                        'total_chunks': len(text_chunks),
                        'doc_index': doc_idx,
                        'chunk_size': len(chunk_text)
                    })

                    # Создать новый Document
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
        """
        Создать список Document объектов из текста с метаданными

        Args:
            text: Исходный текст
            metadata: Метаданные для документов

        Returns:
            Список Document chunks
        """
        try:
            if not text or not text.strip():
                logger.warning("⚠️ Empty text provided")
                return []

            metadata = metadata or {}

            logger.info(f"📄 Creating documents from text ({len(text)} chars)...")

            # Разбить текст
            chunks = self.split_text(text)

            # Создать Document объекты
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
        Разбить текст с учетом типа файла
        Использует разные стратегии для разных типов

        Args:
            text: Исходный текст
            file_type: Тип файла (pdf, docx, txt, csv, etc.)
            metadata: Метаданные файла

        Returns:
            Список Document chunks
        """
        try:
            metadata = metadata or {}
            metadata['file_type'] = file_type

            # Специальная обработка для разных типов
            if file_type in ['csv', 'xlsx', 'xls']:
                # Для табличных данных - больший chunk size
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size * 2,  # Увеличенный размер
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n\n", "\n"],  # Только строки
                    length_function=len
                )
                chunks = splitter.split_text(text)

            elif file_type == 'json':
                # JSON - разбиваем по структуре
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n\n", "\n", ",", " "],
                    length_function=len
                )
                chunks = splitter.split_text(text)

            elif file_type == 'md':
                # Markdown - сохраняем структуру заголовков
                splitter = RecursiveCharacterTextSplitter(
                    chunk_size=self.chunk_size,
                    chunk_overlap=self.chunk_overlap,
                    separators=["\n## ", "\n### ", "\n\n", "\n", ". ", " "],
                    length_function=len
                )
                chunks = splitter.split_text(text)

            else:
                # Обычная разбивка для остальных типов
                chunks = self.split_text(text)

            # Создать документы с метаданными
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
                f"(avg size: {sum(len(c) for c in chunks) // len(chunks)})"
            )
            return documents

        except Exception as e:
            logger.error(f"❌ Error splitting by file type: {e}")
            raise

    def get_chunk_stats(self, documents: List[Document]) -> Dict[str, Any]:
        """
        Получить статистику по chunks

        Args:
            documents: Список Document chunks

        Returns:
            Словарь со статистикой
        """
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