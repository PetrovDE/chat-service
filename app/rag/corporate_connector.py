# app/rag/corporate_connector.py
import logging
from typing import List, Dict, Any, Optional
import httpx
import asyncio

logger = logging.getLogger(__name__)


class CorporateDataConnector:
    """
    Подключение к корпоративным источникам:
    - Внутренние wiki/knowledge base
    - Документ-менеджмент системы
    - Корпоративные API
    """

    def __init__(self):
        self.sources = {}
        self.http_client = httpx.AsyncClient()

    async def register_source(
            self,
            name: str,
            endpoint: str,
            auth_token: Optional[str] = None,
            search_method: str = "full_text"
    ):
        """
        Регистрация корпоративного источника

        Примеры:
        - name: "company_wiki"
        - endpoint: "http://internal-wiki.corp/api/search"
        - auth_token: "Bearer token123"
        """
        self.sources[name] = {
            "endpoint": endpoint,
            "auth_token": auth_token,
            "search_method": search_method
        }
        logger.info(f"✅ Registered corporate source: {name}")

    async def search_all_sources(
            self,
            query: str,
            limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Параллельный поиск по всем корпоративным источникам
        """
        tasks = [
            self._search_source(source_name, query, limit)
            for source_name in self.sources.keys()
        ]

        results_per_source = await asyncio.gather(*tasks, return_exceptions=True)

        all_results = []
        for source_name, results in zip(self.sources.keys(), results_per_source):
            if isinstance(results, Exception):
                logger.error(f"Error searching {source_name}: {results}")
            else:
                all_results.extend(results)
                logger.info(f"Found {len(results)} results in {source_name}")

        return all_results

    async def _search_source(
            self,
            source_name: str,
            query: str,
            limit: int
    ) -> List[Dict]:
        """Поиск в одном источнике"""
        source = self.sources[source_name]

        headers = {}
        if source["auth_token"]:
            headers["Authorization"] = source["auth_token"]

        try:
            response = await self.http_client.post(
                source["endpoint"],
                json={"query": query, "limit": limit},
                headers=headers,
                timeout=10.0
            )
            response.raise_for_status()

            results = response.json().get("results", [])

            # Нормализуем результаты
            return [
                {
                    "content": item.get("content", item.get("text", "")),
                    "metadata": {
                        "source": source_name,
                        "url": item.get("url", ""),
                        "title": item.get("title", "")
                    },
                    "relevance": item.get("score", 0.5)
                }
                for item in results
            ]

        except Exception as e:
            logger.error(f"Error searching {source_name}: {e}")
            return []
