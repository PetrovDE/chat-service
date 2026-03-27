# app/rag/vector_store.py
from __future__ import annotations

import json
import re
import logging
from hashlib import sha1
from threading import Lock
from typing import List, Dict, Any, Optional, Tuple

from app.core.config import settings

try:
    from chromadb import EphemeralClient, PersistentClient
except ImportError:
    EphemeralClient = None
    PersistentClient = None

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """
    Менеджер ChromaDB с поддержкой динамических коллекций по размерности эмбеддингов.
    """

    _shared_clients: Dict[Tuple[str, str], Any] = {}
    _shared_clients_lock: Lock = Lock()

    def __init__(
        self,
        base_collection_name: str = None,
        persist_directory: str = None
    ):
        self.base_collection_name = base_collection_name or settings.COLLECTION_NAME
        self.persist_directory = persist_directory or str(settings.get_vectordb_path())
        self.ephemeral_mode = bool(getattr(settings, "VECTORDB_EPHEMERAL_MODE", False))

        self._client: Optional[Any] = None
        self._collections_cache: Dict[Tuple[int, Optional[str], Optional[str]], Any] = {}
        self._current_collection_key: Optional[Tuple[int, Optional[str], Optional[str]]] = None
        self._current_collection: Optional[Any] = None

        logger.info(
            (
                "VectorStoreManager configured: base=%s persist_directory=%s mode=%s "
                "lazy_initialized=%s"
            ),
            self.base_collection_name,
            self.persist_directory,
            self.client_mode,
            False,
        )

    _COLLECTION_TOKEN_RE = re.compile(r"[^a-z0-9_-]+")

    @property
    def client_mode(self) -> str:
        return "ephemeral" if self.ephemeral_mode else "persistent"

    @property
    def client(self) -> Any:
        return self._get_or_init_client()

    def _cache_key(self) -> Tuple[str, str]:
        if self.ephemeral_mode:
            return ("ephemeral", "__shared__")
        return ("persistent", self.persist_directory)

    def _build_client(self) -> Any:
        if self.ephemeral_mode:
            if not EphemeralClient:
                raise ImportError("chromadb EphemeralClient is not available")
            return EphemeralClient()

        if not PersistentClient:
            raise ImportError("chromadb PersistentClient is not available")
        return PersistentClient(path=self.persist_directory)

    def _get_or_init_client(self) -> Any:
        if self._client is not None:
            return self._client

        cache_key = self._cache_key()
        with self._shared_clients_lock:
            cached = self._shared_clients.get(cache_key)
            if cached is None:
                logger.info(
                    (
                        "Initializing Chroma client: mode=%s persist_directory=%s "
                        "lazy_initialized=%s shared_client_created=%s"
                    ),
                    self.client_mode,
                    self.persist_directory,
                    True,
                    True,
                )
                try:
                    cached = self._build_client()
                except Exception:
                    logger.exception(
                        (
                            "Chroma client initialization failed: mode=%s persist_directory=%s "
                            "hint=Persisted Chroma store may be corrupted or incompatible. "
                            "recovery_hint=For local dev, stop the service, backup/remove chroma.sqlite3, "
                            "or set VECTORDB_EPHEMERAL_MODE=true to start without persisted state."
                        ),
                        self.client_mode,
                        self.persist_directory,
                    )
                    raise
                self._shared_clients[cache_key] = cached
            else:
                logger.info(
                    (
                        "Initializing Chroma client: mode=%s persist_directory=%s "
                        "lazy_initialized=%s shared_client_created=%s"
                    ),
                    self.client_mode,
                    self.persist_directory,
                    True,
                    False,
                )

            self._client = cached
            return cached

    def _normalize_collection_token(self, value: Optional[str], *, fallback: str) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return fallback
        normalized = self._COLLECTION_TOKEN_RE.sub("-", raw).strip("-_")
        if not normalized:
            normalized = fallback
        return normalized[:40]

    def _normalize_embedding_identity(
        self,
        *,
        embedding_mode: Optional[str],
        embedding_model: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        mode = self._normalize_collection_token(embedding_mode, fallback="default") if embedding_mode else None
        model = self._normalize_collection_token(embedding_model, fallback="default") if embedding_model else None
        if not mode and not model:
            return None, None
        return mode or "default", model or "default"

    def _collection_cache_key(
        self,
        *,
        dimension: int,
        embedding_mode: Optional[str],
        embedding_model: Optional[str],
    ) -> Tuple[int, Optional[str], Optional[str]]:
        mode, model = self._normalize_embedding_identity(
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
        return (int(dimension), mode, model)

    def _get_collection_name(
        self,
        dimension: int,
        *,
        embedding_mode: Optional[str] = None,
        embedding_model: Optional[str] = None,
    ) -> str:
        mode, model = self._normalize_embedding_identity(
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
        if not mode and not model:
            # Keep legacy naming for old collections without model identity.
            return f"{self.base_collection_name}_{dimension}d"

        identity_hash = sha1(f"{mode}:{model}".encode("utf-8")).hexdigest()[:10]
        return f"{self.base_collection_name}_{dimension}d_{mode}_{model}_{identity_hash}"

    def _ensure_collection(
        self,
        embedding: List[float],
        *,
        embedding_mode: Optional[str] = None,
        embedding_model: Optional[str] = None,
    ) -> Any:
        dimension = len(embedding)
        cache_key = self._collection_cache_key(
            dimension=dimension,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
        collection_name = self._get_collection_name(
            dimension,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )

        if self._current_collection_key == cache_key and self._current_collection is not None:
            return self._current_collection

        if cache_key in self._collections_cache:
            self._current_collection_key = cache_key
            self._current_collection = self._collections_cache[cache_key]
            logger.info("Active collection: %s key=%s", collection_name, cache_key)
            return self._current_collection

        try:
            collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={
                    "dimension": dimension,
                    "embedding_mode": embedding_mode or "",
                    "embedding_model": embedding_model or "",
                },
            )
            self._collections_cache[cache_key] = collection
            self._current_collection_key = cache_key
            self._current_collection = collection

            try:
                count = collection.count()
            except Exception:
                count = -1

            logger.info(
                "Collection initialized: %s dim=%d mode=%s model=%s count=%s",
                collection_name,
                dimension,
                embedding_mode or "",
                embedding_model or "",
                str(count),
            )
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

    def _identity_from_metadata(self, metadata: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        raw = metadata or {}
        mode_raw = str(raw.get("embedding_mode") or "").strip().lower() or None
        model_raw = str(raw.get("embedding_model") or "").strip() or None

        if model_raw and ":" in model_raw:
            mode_part, model_part = model_raw.split(":", 1)
            if not mode_raw and mode_part:
                mode_raw = mode_part.strip().lower()
            model_raw = model_part.strip() or None

        if mode_raw == "ollama":
            mode_raw = "local"
        if mode_raw == "corporate":
            mode_raw = "aihub"

        return mode_raw, model_raw

    def _identity_from_filter(self, where: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
        if not isinstance(where, dict):
            return None, None
        mode = where.get("embedding_mode")
        model = where.get("embedding_model")
        if isinstance(mode, dict):
            mode = mode.get("$eq")
        if isinstance(model, dict):
            model = model.get("$eq")
        return self._identity_from_metadata({"embedding_mode": mode, "embedding_model": model})

    def _extract_dimension_from_name(self, collection_name: str) -> Optional[int]:
        m = re.search(r"_(\d+)d(?:_|$)", str(collection_name or ""))
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _query_collection(
        self,
        *,
        collection: Any,
        embedding_query: List[float],
        top_k: int,
        safe_filter: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        query_params = {"query_embeddings": [embedding_query], "n_results": top_k}
        if safe_filter:
            query_params["where"] = self._normalize_where(safe_filter)
        results = collection.query(**query_params)
        return self._parse_results(results)

    def resolve_collection_name(self, *, embedding: List[float], metadata: Optional[Dict[str, Any]]) -> str:
        mode, model = self._identity_from_metadata(metadata)
        return self._get_collection_name(
            len(embedding),
            embedding_mode=mode,
            embedding_model=model,
        )

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

        mode, model = self._identity_from_metadata(metadata)
        collection_name = self._get_collection_name(
            len(embedding),
            embedding_mode=mode,
            embedding_model=model,
        )
        self._ensure_collection(
            embedding,
            embedding_mode=mode,
            embedding_model=model,
        )
        enriched_metadata = dict(metadata or {})
        enriched_metadata["collection"] = collection_name
        enriched_metadata["embedding_dimension"] = len(embedding)
        safe_metadata = self._sanitize(enriched_metadata, mode="storage")

        try:
            add_payload = {
                "documents": [content],
                "metadatas": [safe_metadata],
                "embeddings": [embedding],
            }
            if doc_id:
                add_payload["ids"] = [doc_id]
            upsert_fn = getattr(self._current_collection, "upsert", None)
            if callable(upsert_fn):
                upsert_fn(**add_payload)
            else:
                self._current_collection.add(**add_payload)
            logger.info(
                "Document added: id=%s size=%d dim=%d mode=%s model=%s collection=%s",
                doc_id or "-",
                len(content or ""),
                len(embedding),
                mode or "-",
                model or "-",
                collection_name,
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

        safe_filter = self._sanitize(filter_dict or {}, mode="where") if filter_dict else None
        mode, model = self._identity_from_filter(safe_filter or {})

        logger.info(
            "Query: dim=%d top_k=%d mode=%s model=%s filter=%s",
            dimension,
            top_k,
            mode or "-",
            model or "-",
            safe_filter if safe_filter else None,
        )

        try:
            if mode or model:
                collection = self._ensure_collection(
                    embedding_query,
                    embedding_mode=mode,
                    embedding_model=model,
                )
                return self._query_collection(
                    collection=collection,
                    embedding_query=embedding_query,
                    top_k=top_k,
                    safe_filter=safe_filter,
                )

            # No explicit embedding identity in filters: query all base collections for this dimension.
            rows: List[Dict[str, Any]] = []
            for collection in self._iter_base_collections():
                name = str(getattr(collection, "name", ""))
                coll_dim = self._extract_dimension_from_name(name)
                if coll_dim is not None and coll_dim != dimension:
                    continue
                try:
                    rows.extend(
                        self._query_collection(
                            collection=collection,
                            embedding_query=embedding_query,
                            top_k=top_k,
                            safe_filter=safe_filter,
                        )
                    )
                except Exception:
                    continue

            if not rows:
                return []
            rows = sorted(rows, key=lambda x: float(x.get("distance", 1e9)))[:top_k]
            return rows
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
        safe_filter = self._sanitize(filter_dict or {}, mode="where") if filter_dict else None

        for collection in self._iter_base_collections():
            try:
                all_results.extend(
                    self._query_collection(
                        collection=collection,
                        embedding_query=embedding_query,
                        top_k=top_k,
                        safe_filter=safe_filter,
                    )
                )
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
