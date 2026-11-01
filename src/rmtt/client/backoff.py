"""Exponential backoff with jitter, ported from ``rmtt-go``."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class _BackoffStatus:
    last_sleep: float
    last_error_at: float


class Backoff:
    """Per-situation exponential backoff, identical in spirit to the Go client."""

    def __init__(self, jitter: float = 0.25) -> None:
        self._jitter = jitter
        self._status: Dict[str, _BackoffStatus] = {}

    def next_sleep(
        self,
        situation: str,
        init_sleep: float,
        max_sleep: float,
        process_time: float,
        skip_first: bool,
        now: float,
    ) -> float:
        init_sleep = min(init_sleep, max_sleep)
        status = self._status.get(situation)
        if status is None:
            status = _BackoffStatus(init_sleep, now)
            self._status[situation] = status
            return 0.0 if skip_first else init_sleep

        old_time = status.last_error_at
        status.last_error_at = now
        if now - old_time > (process_time * 2 + status.last_sleep):
            status.last_sleep = init_sleep
            return 0.0 if skip_first else init_sleep

        if status.last_sleep == 0:
            status.last_sleep = init_sleep
            return init_sleep

        status.last_sleep = min(status.last_sleep * 2, max_sleep)
        return status.last_sleep

    def jitter(self, duration: float) -> float:
        """Apply the configured jitter factor (no randomness when jitter <= 0)."""
        if self._jitter <= 0 or duration <= 0:
            return duration
        factor = 1 - self._jitter + random.random() * (2 * self._jitter)
        return duration * factor

    def reset(self, situation: Optional[str] = None) -> None:
        if situation is None:
            self._status.clear()
        else:
            self._status.pop(situation, None)
