# app/rag/vector_store.py
from __future__ import annotations

import json
import logging
from typing import List, Dict, Any, Optional

from app.core.config import settings

try:
    from chromadb import PersistentClient
except ImportError:
    PersistentClient = None

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """
    Менеджер ChromaDB с поддержкой динамических коллекций по размерности эмбеддингов.
    """

    def __init__(
        self,
        base_collection_name: str = None,
        persist_directory: str = None
    ):
        self.base_collection_name = base_collection_name or settings.COLLECTION_NAME
        self.persist_directory = persist_directory or str(settings.get_vectordb_path())

        if not PersistentClient:
            raise ImportError("chromadb library not installed")

        self.client = PersistentClient(path=self.persist_directory)

        self._collections_cache: Dict[int, Any] = {}
        self._current_dimension: Optional[int] = None
        self._current_collection: Optional[Any] = None

        logger.info(
            "VectorStoreManager initialized. base=%s dir=%s",
            self.base_collection_name,
            self.persist_directory,
        )

    def _get_collection_name(self, dimension: int) -> str:
        return f"{self.base_collection_name}_{dimension}d"

    def _ensure_collection(self, embedding: List[float]) -> Any:
        dimension = len(embedding)

        if self._current_dimension == dimension and self._current_collection is not None:
            return self._current_collection

        if dimension in self._collections_cache:
            self._current_dimension = dimension
            self._current_collection = self._collections_cache[dimension]
            logger.info("Active collection: %s (dim=%d)", self._get_collection_name(dimension), dimension)
            return self._current_collection

        collection_name = self._get_collection_name(dimension)
        try:
            collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"dimension": dimension},
            )
            self._collections_cache[dimension] = collection
            self._current_dimension = dimension
            self._current_collection = collection

            try:
                count = collection.count()
            except Exception:
                count = -1

            logger.info("Collection initialized: %s dim=%d count=%s", collection_name, dimension, str(count))
            return collection
        except Exception as e:
            logger.error("Failed to initialize collection: %s", e, exc_info=True)
            raise

    def _sanitize(self, data: Dict[str, Any], *, mode: str) -> Dict[str, Any]:
        """
        mode:
          - "storage": sanitize metadata for collection.add (Chroma требует scalar)
          - "where": sanitize where-filter for collection.query/delete (нужно сохранить operator dicts и $in lists)
        """
        if not data:
            return {}

        operator_keys = {
            "$and", "$or",
            "$in", "$nin",
            "$gt", "$gte", "$lt", "$lte",
            "$ne", "$eq",
            "$contains",
        }

        def sanitize_value(v: Any, *, in_operator: bool) -> Any:
            if v is None:
                return None
            if isinstance(v, (str, int, float, bool)):
                return v
            if isinstance(v, (bytes, bytearray)):
                try:
                    return v.decode("utf-8", errors="ignore")
                except Exception:
                    return str(v)

            if isinstance(v, dict):
                is_operator = any(k in operator_keys for k in v.keys())
                # For where-mode: keep operator dicts and recurse
                if mode == "where" and is_operator:
                    return {k: sanitize_value(val, in_operator=True) for k, val in v.items()}

                # For storage-mode OR non-operator dict: must be scalar -> JSON string
                try:
                    payload = {k: sanitize_value(val, in_operator=False) for k, val in v.items()}
                    return json.dumps(payload, ensure_ascii=False)
                except Exception:
                    return str(v)

            if isinstance(v, (list, tuple, set)):
                items = [sanitize_value(x, in_operator=in_operator) for x in list(v)]
                # In where-mode inside operator ($in): keep list
                if mode == "where" and in_operator:
                    return items
                # In storage-mode (or not-operator): must be scalar -> JSON string
                try:
                    return json.dumps(items, ensure_ascii=False)
                except Exception:
                    return str(items)

            return str(v)

        out: Dict[str, Any] = {}
        for k, v in data.items():
            # when k itself is an operator key, children are in_operator context
            in_operator = (mode == "where" and k in operator_keys)
            out[k] = sanitize_value(v, in_operator=in_operator)

        return out

    def _normalize_where(self, where: Dict[str, Any]) -> Dict[str, Any]:
        if not where:
            return {}

        # Если уже оператор верхнего уровня — просто нормализуем рекурсивно
        if "$and" in where and isinstance(where["$and"], list):
            return {"$and": [self._normalize_where(x) if isinstance(x, dict) else x for x in where["$and"]]}
        if "$or" in where and isinstance(where["$or"], list):
            return {"$or": [self._normalize_where(x) if isinstance(x, dict) else x for x in where["$or"]]}

        # КЛЮЧЕВОЕ: Chroma требует ровно один оператор на верхнем уровне,
        # поэтому если у нас несколько полей — оборачиваем в $and
        if isinstance(where, dict) and len(where.keys()) > 1:
            return {"$and": [{k: v} for k, v in where.items()]}

        return where

    def add_document(
        self,
        *,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
        embedding: Optional[List[float]] = None,
        doc_id: Optional[str] = None
    ) -> bool:
        if embedding is None:
            raise ValueError("Embedding is required")

        self._ensure_collection(embedding)
        safe_metadata = self._sanitize(metadata or {}, mode="storage")

        try:
            self._current_collection.add(
                documents=[content],
                metadatas=[safe_metadata],
                embeddings=[embedding],
                ids=[doc_id] if doc_id else None,
            )
            logger.info(
                "Document added: id=%s size=%d dim=%d collection=%s",
                doc_id or "-",
                len(content or ""),
                len(embedding),
                self._get_collection_name(len(embedding)),
            )
            return True
        except Exception as e:
            logger.error("Failed to add document: %s", e, exc_info=True)
            return False

    def delete_by_metadata(self, metadata_filter: Dict[str, Any]) -> int:
        if not metadata_filter:
            return 0

        safe_filter = self._sanitize(metadata_filter, mode="where")
        deleted_total = 0
        try:
            collections = list(self._collections_cache.values()) + self._iter_base_collections()
            seen = set()
            for collection in collections:
                try:
                    name = getattr(collection, "name", None)
                    if name and name in seen:
                        continue
                    if name:
                        seen.add(name)
                    before = collection.count()
                    collection.delete(where=self._normalize_where(safe_filter))
                    after = collection.count()
                    deleted_total += max(0, int(before - after))
                except Exception:
                    continue
            logger.info("Deleted by metadata filter: %s deleted=%d", safe_filter, deleted_total)
            return deleted_total
        except Exception as e:
            logger.error("Failed to delete by metadata: %s", e, exc_info=True)
            return 0

    def query(
        self,
        embedding_query: List[float],
        top_k: int = 5,
        filter_dict: Optional[Dict[str, Any]] = None,
        search_all_dimensions: bool = False
    ) -> List[Dict[str, Any]]:
        dimension = len(embedding_query)

        if search_all_dimensions:
            return self._query_all_dimensions(embedding_query, top_k, filter_dict)

        self._ensure_collection(embedding_query)

        safe_filter = self._sanitize(filter_dict or {}, mode="where") if filter_dict else None

        logger.info(
            "Query: collection=%s top_k=%d dim=%d filter=%s",
            self._get_collection_name(dimension),
            top_k,
            dimension,
            safe_filter if safe_filter else None,
        )

        try:
            query_params = {"query_embeddings": [embedding_query], "n_results": top_k}
            if safe_filter:
                query_params["where"] = self._normalize_where(safe_filter)

            results = self._current_collection.query(**query_params)
            return self._parse_results(results)
        except Exception as e:
            logger.error("Query failed: %s", e, exc_info=True)
            return []

    def _query_all_dimensions(
        self,
        embedding_query: List[float],
        top_k: int,
        filter_dict: Optional[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        all_results: List[Dict[str, Any]] = []

        _ = self._ensure_collection(embedding_query)

        for dim in list(self._collections_cache.keys()):
            try:
                collection = self._collections_cache[dim]
                safe_filter = self._sanitize(filter_dict or {}, mode="where") if filter_dict else None

                query_params = {"query_embeddings": [embedding_query], "n_results": top_k}
                if safe_filter:
                    query_params["where"] = self._normalize_where(safe_filter)

                results = collection.query(**query_params)
                all_results.extend(self._parse_results(results))
            except Exception:
                continue

        try:
            all_results = sorted(all_results, key=lambda x: float(x.get("distance", 1e9)))[:top_k]
        except Exception:
            all_results = all_results[:top_k]

        return all_results

    def _parse_results(self, results: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            ids = (results.get("ids") or [[]])[0]
            docs = (results.get("documents") or [[]])[0]
            metas = (results.get("metadatas") or [[]])[0]
            dists = (results.get("distances") or [[]])[0]
        except Exception:
            return []

        out: List[Dict[str, Any]] = []
        for i in range(min(len(ids), len(docs), len(metas), len(dists))):
            out.append(
                {"id": ids[i], "content": docs[i], "metadata": metas[i] or {}, "distance": dists[i]}
            )
        return out

    def _iter_base_collections(self) -> List[Any]:
        """
        Return all Chroma collections that belong to current base_collection_name.
        Works with different Chroma list_collections return types.
        """
        out: List[Any] = []
        seen = set()

        try:
            listed = self.client.list_collections()
        except Exception:
            logger.warning("Could not list collections from Chroma", exc_info=True)
            return out

        for item in listed or []:
            try:
                name = item.name if hasattr(item, "name") else str(item)
            except Exception:
                continue

            if not name.startswith(f"{self.base_collection_name}_"):
                continue
            if name in seen:
                continue

            seen.add(name)
            try:
                out.append(self.client.get_collection(name=name))
            except Exception:
                logger.warning("Could not open collection %s", name, exc_info=True)

        return out

    def get_by_filter(
        self,
        *,
        filter_dict: Optional[Dict[str, Any]] = None,
        limit_per_collection: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Fetch documents by metadata filter across all known base collections.
        Used for hybrid lexical retrieval and full-file analysis.
        """
        safe_filter = self._sanitize(filter_dict or {}, mode="where") if filter_dict else None
        where = self._normalize_where(safe_filter) if safe_filter else None

        results: List[Dict[str, Any]] = []
        collections = self._iter_base_collections()
        logger.info("get_by_filter: collections=%d where=%s", len(collections), where if where else None)

        for collection in collections:
            try:
                raw = collection.get(
                    where=where,
                    include=["documents", "metadatas"],
                    limit=limit_per_collection,
                )
            except Exception:
                logger.warning("Collection get failed", exc_info=True)
                continue

            ids = raw.get("ids") or []
            docs = raw.get("documents") or []
            metas = raw.get("metadatas") or []

            for i in range(min(len(ids), len(docs), len(metas))):
                results.append(
                    {
                        "id": ids[i],
                        "content": docs[i],
                        "metadata": metas[i] or {},
                    }
                )

        return results
