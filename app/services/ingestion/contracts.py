from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class IngestionJobPayload:
    file_id: str
    file_path: str
    embedding_mode: str
    embedding_model: str
    request_id: Optional[str] = None
    processing_id: Optional[str] = None
    pipeline_version: Optional[str] = None
    parser_version: Optional[str] = None
    artifact_version: Optional[str] = None
    chunking_strategy: Optional[str] = None
    retrieval_profile: Optional[str] = None


@dataclass(frozen=True)
class IngestionEnqueueResult:
    job_id: str
    deduplicated: bool
    status: str
    attempt: int


@dataclass(frozen=True)
class IngestionLeasedJob:
    job_id: str
    idempotency_key: str
    payload: IngestionJobPayload
    attempt: int
    max_retries: int
    next_run_at: float
    created_at: float


@dataclass(frozen=True)
class IngestionQueueStats:
    queued: int
    processing: int
    completed: int
    dead_letter: int
    lag_seconds: float
    last_heartbeat_age_seconds: Optional[float]


class IngestionQueueAdapter(ABC):
    @abstractmethod
    async def enqueue(
        self,
        *,
        payload: IngestionJobPayload,
        idempotency_key: str,
        max_retries: int,
        allow_requeue_terminal: bool,
    ) -> IngestionEnqueueResult:
        raise NotImplementedError

    @abstractmethod
    async def acquire(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> Optional[IngestionLeasedJob]:
        raise NotImplementedError

    @abstractmethod
    async def mark_completed(self, job_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def mark_retry(self, *, job_id: str, error_message: str, delay_seconds: float) -> None:
        raise NotImplementedError

    @abstractmethod
    async def mark_dead_letter(self, *, job_id: str, error_message: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def release_lease(self, *, job_id: str, delay_seconds: float, error_message: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def requeue_expired_leases(self) -> int:
        raise NotImplementedError

    @abstractmethod
    async def heartbeat(self, *, worker_id: str) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_stats(self, *, worker_id: Optional[str]) -> IngestionQueueStats:
        raise NotImplementedError
