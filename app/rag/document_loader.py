# app/rag/document_loader.py

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from langchain_core.documents import Document
from app.core.config import settings

logger = logging.getLogger(__name__)


class DocumentLoader:
    def __init__(self):
        # Получаем поддерживаемые типы из конфигурации
        self.supported_loaders = {
            ".pdf": self.load_pdf,
            ".docx": self.load_docx,
            ".txt": self.load_text,
            ".csv": self.load_csv,
            ".xlsx": self.load_excel,
            ".json": self.load_json,
            ".md": self.load_markdown,
        }
        logger.info("DocumentLoader initialized")

    async def load_file(self, filepath: str, metadata: Optional[Dict[str, Any]] = None) -> List[Document]:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        # ИСПРАВЛЕНИЕ 1: Проверка размера файла ДО обработки
        file_size_mb = path.stat().st_size / (1024 * 1024)
        if file_size_mb > settings.MAX_FILESIZE_MB:
            raise ValueError(
                f"File {path.name} exceeds max allowed size: "
                f"{file_size_mb:.2f} MB > {settings.MAX_FILESIZE_MB} MB"
            )

        ext = path.suffix.lower()
        if not settings.is_file_supported(filepath):
            raise ValueError(f"Filetype {ext} not supported")

        logger.info(f"📂 Loading file: {path.name} ({file_size_mb:.2f} MB)")
        return await self.supported_loaders[ext](filepath, metadata)

    async def load_pdf(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import PyPDFLoader
        loader = PyPDFLoader(filepath)
        return loader.load()

    async def load_docx(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        try:
            from langchain_community.document_loaders import Docx2txtLoader
            loader = Docx2txtLoader(filepath)
            return loader.load()
        except ImportError:
            raise ImportError("docx2txt required. Install: pip install docx2txt")

    async def load_text(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import TextLoader
        loader = TextLoader(filepath, encoding='utf-8')
        return loader.load()

    async def load_csv(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import CSVLoader
        loader = CSVLoader(filepath, encoding='utf-8')
        return loader.load()

    async def load_excel(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        ИСПРАВЛЕНО: Оптимизированная загрузка Excel с минимальным количеством чанков
        - Объединяет все листы в один документ
        - Сохраняет структуру таблицы
        - Избегает излишней фрагментации
        """
        try:
            import pandas as pd

            excel_file = pd.ExcelFile(filepath)
            all_sheets_content = []

            logger.info(f"📊 Processing Excel file with {len(excel_file.sheet_names)} sheet(s)")

            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(filepath, sheet_name=sheet_name)

                    # Пропускаем пустые листы
                    if df.empty:
                        logger.warning(f"⚠️ Sheet '{sheet_name}' is empty, skipping")
                        continue

                    # Формируем структурированный текст для листа
                    sheet_content = []
                    sheet_content.append(f"\n{'=' * 60}")
                    sheet_content.append(f"ЛИСТ: {sheet_name}")
                    sheet_content.append(f"{'=' * 60}")

                    # Добавляем заголовки
                    headers = df.columns.tolist()
                    sheet_content.append(f"\nКолонки: {', '.join(str(h) for h in headers)}")
                    sheet_content.append(f"Количество строк: {len(df)}\n")

                    # ИСПРАВЛЕНИЕ 2: Оптимизированное форматирование строк
                    # Группируем данные в блоки по 10 строк для уменьшения фрагментации
                    rows_per_block = 10
                    for block_start in range(0, len(df), rows_per_block):
                        block_end = min(block_start + rows_per_block, len(df))
                        sheet_content.append(f"\n--- Строки {block_start + 1}-{block_end} ---")

                        for idx in range(block_start, block_end):
                            row = df.iloc[idx]
                            row_parts = []
                            for col in df.columns:
                                value = row[col]
                                if pd.notna(value):
                                    # Обрезаем слишком длинные значения
                                    str_value = str(value)
                                    if len(str_value) > 200:
                                        str_value = str_value[:200] + "..."
                                    row_parts.append(f"{col}: {str_value}")

                            if row_parts:
                                sheet_content.append(f"Строка {idx + 1}: {' | '.join(row_parts)}")

                    all_sheets_content.append("\n".join(sheet_content))
                    logger.info(f"✅ Processed sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")

                except Exception as e:
                    logger.warning(f"⚠️ Error reading sheet '{sheet_name}': {str(e)}")
                    continue

            if not all_sheets_content:
                raise ValueError(f"No readable data found in Excel file: {filepath}")

            # ИСПРАВЛЕНИЕ 3: Создаем ОДИН документ для всего файла
            # Это предотвратит излишнее дробление на чанки
            combined_content = "\n\n".join(all_sheets_content)

            doc_metadata = {
                "source": filepath,
                "file_type": "xlsx",
                "sheet_count": len(excel_file.sheet_names),
                "total_content_length": len(combined_content),
            }

            if metadata:
                doc_metadata.update(metadata)

            document = Document(
                page_content=combined_content,
                metadata=doc_metadata
            )

            logger.info(
                f"✅ Excel loaded as single document: "
                f"{len(excel_file.sheet_names)} sheets, "
                f"{len(combined_content)} chars"
            )

            return [document]

        except ImportError:
            logger.error("pandas and openpyxl are required for Excel file loading")
            raise ImportError("Please install: pip install pandas openpyxl")
        except Exception as e:
            logger.error(f"❌ Error loading Excel file {filepath}: {str(e)}")
            raise

    async def load_json(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import JSONLoader
        loader = JSONLoader(filepath, jq_schema='.', text_content=False)
        return loader.load()

    async def load_markdown(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import UnstructuredMarkdownLoader
        loader = UnstructuredMarkdownLoader(filepath)
        return loader.load()
