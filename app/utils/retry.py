from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional, Tuple, TypeVar

import httpx

T = TypeVar("T")


RETRYABLE_EXCEPTIONS: Tuple[type[BaseException], ...] = (
    httpx.TimeoutException,
    httpx.ConnectError,
    httpx.RemoteProtocolError,
    httpx.ReadError,
)


async def async_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    retries: int = 2,
    base_delay: float = 0.4,
    max_delay: float = 2.0,
    retry_exceptions: Optional[Tuple[type[BaseException], ...]] = None,
) -> T:
    errors = retry_exceptions or RETRYABLE_EXCEPTIONS
    attempt = 0
    while True:
        try:
            return await fn()
        except errors:
            if attempt >= retries:
                raise
            delay = min(max_delay, base_delay * (2 ** attempt))
            await asyncio.sleep(delay)
            attempt += 1

