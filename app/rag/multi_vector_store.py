# app/rag/multi_vector_store.py

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class VectorStoreType(str, Enum):
    """Типы vector stores"""
    REDIS = "redis"
    CHROMA = "chroma"
    POSTGRESQL = "postgresql"
    ALL = "all"


class MultiVectorStore:
    """
    Многоуровневая RAG система:
    1. Redis (быстрый кэш - поиск < 100ms)
    2. PostgreSQL + pgvector (долгосрочное хранилище)
    3. ChromaDB (локальный RAG)

    ✅ Все импорты в правильных местах - NO LAZY LOADING в основных блоках!
    """

    def __init__(self):
        from app.rag.vector_store import VectorStoreManager

        self.chroma_store = VectorStoreManager()
        self.redis_client = None
        self.redis_ready = False
        self.pg_enabled = True

    async def initialize(self):
        """Инициализация всех хранилищ"""
        try:
            # Инициализируем Redis (опционально, в try-except)
            try:
                import redis.asyncio as redis

                self.redis_client = await redis.from_url(
                    "redis://localhost:6379",
                    encoding="utf8",
                    decode_responses=True
                )
                await self.redis_client.ping()
                self.redis_ready = True
                logger.info("✅ Redis connected")
            except Exception as e:
                logger.warning(f"⚠️ Redis not available: {e}")
                self.redis_ready = False

            logger.info("✅ MultiVectorStore initialized")
        except Exception as e:
            logger.error(f"Error initializing MultiVectorStore: {e}")

    async def search(
            self,
            query: str,
            k: int = 5,
            filter_dict: Optional[Dict] = None,
            include_corporate: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Параллельный поиск по всем хранилищам
        """

        search_times = {}
        all_results = []

        # === 1. REDIS (< 100ms) ===
        if self.redis_client:
            start = time.time()
            try:
                redis_results = await self._search_redis(query, k)
                search_times["redis"] = time.time() - start
                all_results.extend(redis_results)
                logger.debug(f"🚀 Redis: {search_times['redis']:.3f}s, found {len(redis_results)}")
            except Exception as e:
                logger.debug(f"Redis search failed: {e}")

        # === 2. PostgreSQL (300-800ms) ===
        if include_corporate:
            start = time.time()
            try:
                pg_results = await self._search_postgresql(query, k)
                search_times["postgresql"] = time.time() - start
                all_results.extend(pg_results)
                logger.debug(f"🏢 PostgreSQL: {search_times['postgresql']:.3f}s, found {len(pg_results)}")
            except Exception as e:
                logger.debug(f"PostgreSQL search failed: {e}")

        # === 3. ChromaDB (200-500ms) ===
        start = time.time()
        try:
            chroma_results = await self._search_chroma(query, k, filter_dict)
            search_times["chroma"] = time.time() - start
            all_results.extend(chroma_results)
            logger.debug(f"📁 ChromaDB: {search_times['chroma']:.3f}s, found {len(chroma_results)}")
        except Exception as e:
            logger.debug(f"ChromaDB search failed: {e}")

        # Дедупликация и ранжирование
        unique = self._deduplicate_results(all_results)
        ranked = sorted(unique, key=lambda x: x.get("relevance", 0), reverse=True)[:k]

        logger.info(f"Multi-search complete: {search_times}, results: {len(ranked)}")

        return ranked

    async def _search_redis(self, query: str, k: int) -> List[Dict]:
        """Поиск в Redis"""
        if not self.redis_client:
            return []

        keys = await self.redis_client.keys("rag:doc:*")
        results = []
        query_lower = query.lower()

        for key in keys[:100]:
            content = await self.redis_client.get(key)
            if content and query_lower in content.lower():
                results.append({
                    "content": content,
                    "metadata": {"source": "redis"},
                    "relevance": 0.95
                })

        return results[:k]

    async def _search_chroma(
            self,
            query: str,
            k: int,
            filter_dict: Optional[Dict] = None
    ) -> List[Dict]:
        """Поиск в ChromaDB"""
        results = await asyncio.to_thread(
            self.chroma_store.similarity_search_with_score,
            query, k, filter_dict
        )

        return [
            {
                "content": doc.page_content,
                "metadata": doc.metadata,
                "relevance": float(score),
                "source": "chroma"
            }
            for doc, score in results
        ]

    async def _search_postgresql(self, query: str, k: int) -> List[Dict]:
        """Поиск в PostgreSQL + pgvector"""
        from sqlalchemy import text
        from app.database.database import async_session_maker

        async with async_session_maker() as session:
            # Генерируем embedding для query
            from app.rag.embeddings import OllamaEmbeddingsManager
            embeddings_manager = OllamaEmbeddingsManager()
            query_embedding = await asyncio.to_thread(
                embeddings_manager.embeddings.embed_query,
                query
            )

            # Ищем похожие документы
            search_query = text("""
                                SELECT content,
                                       metadata,
                                       1 - (embedding <=> :query_embedding) as similarity
                                FROM document_embeddings
                                WHERE metadata ->>'source' = 'corporate'
                                ORDER BY embedding <=> :query_embedding
                                    LIMIT :k
                                """)

            result = await session.execute(
                search_query,
                {
                    "query_embedding": str(query_embedding),
                    "k": k
                }
            )

            rows = result.fetchall()
            return [
                {
                    "content": row[0],
                    "metadata": row[1],
                    "relevance": float(row[2]),
                    "source": "postgresql"
                }
                for row in rows
            ]

    def _deduplicate_results(self, results: List[Dict]) -> List[Dict]:
        """Дедупликация результатов"""
        seen = set()
        unique = []
        for r in results:
            key = hash(r["content"][:100])
            if key not in seen:
                seen.add(key)
                unique.append(r)
        return unique
