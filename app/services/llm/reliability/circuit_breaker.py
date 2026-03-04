from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Dict, Optional, Tuple


@dataclass(frozen=True)
class CircuitBreakerConfig:
    window_seconds: int = 60
    min_requests: int = 4
    failure_ratio_threshold: float = 0.5
    open_duration_seconds: int = 30
    half_open_max_requests: int = 1


class CircuitBreaker:
    def __init__(self, config: CircuitBreakerConfig):
        self._config = config
        self._state = "closed"
        self._events: Deque[Tuple[float, bool]] = deque()
        self._opened_at: Optional[float] = None
        self._half_open_attempts = 0
        self._lock = Lock()

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    def allow_request(self, now: Optional[float] = None) -> Tuple[bool, Optional[str]]:
        ts = float(now if now is not None else time.time())
        with self._lock:
            if self._state == "closed":
                self._prune(ts)
                return True, None

            if self._state == "open":
                if self._opened_at is None:
                    self._opened_at = ts
                    return False, "circuit_open"
                if (ts - self._opened_at) < float(self._config.open_duration_seconds):
                    return False, "circuit_open"
                self._state = "half_open"
                self._half_open_attempts = 0

            if self._state == "half_open":
                if self._half_open_attempts >= int(self._config.half_open_max_requests):
                    return False, "circuit_open"
                self._half_open_attempts += 1
                return True, None

            return True, None

    def record_success(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        with self._lock:
            if self._state == "half_open":
                self._state = "closed"
                self._opened_at = None
                self._half_open_attempts = 0
                self._events.clear()
                return

            self._events.append((ts, True))
            self._prune(ts)

    def record_failure(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        with self._lock:
            if self._state == "half_open":
                self._trip_open(ts)
                return

            self._events.append((ts, False))
            self._prune(ts)
            if self._should_open():
                self._trip_open(ts)

    def snapshot(self, now: Optional[float] = None) -> Dict[str, float | int | str]:
        ts = float(now if now is not None else time.time())
        with self._lock:
            self._prune(ts)
            total = len(self._events)
            failures = sum(1 for _, ok in self._events if not ok)
            ratio = (float(failures) / float(total)) if total else 0.0
            return {
                "state": self._state,
                "window_total_requests": total,
                "window_failures": failures,
                "window_failure_ratio": ratio,
                "half_open_attempts": self._half_open_attempts,
            }

    def _prune(self, now: float) -> None:
        window = float(self._config.window_seconds)
        while self._events and (now - self._events[0][0]) > window:
            self._events.popleft()

    def _should_open(self) -> bool:
        total = len(self._events)
        if total < int(self._config.min_requests):
            return False
        failures = sum(1 for _, ok in self._events if not ok)
        ratio = float(failures) / float(total)
        return ratio >= float(self._config.failure_ratio_threshold)

    def _trip_open(self, now: float) -> None:
        self._state = "open"
        self._opened_at = now
        self._half_open_attempts = 0

