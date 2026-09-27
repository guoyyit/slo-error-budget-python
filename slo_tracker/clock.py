"""Clock abstraction so tests can advance time deterministically."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Return the current time in seconds since the epoch."""
        ...


class SystemClock:
    """Wall-clock implementation used in production."""

    def now(self) -> float:
        return time.time()


class FakeClock:
    """Deterministic, manually advanced clock for tests and demos."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot advance time backwards")
        self._now += seconds

    def set(self, value: float) -> None:
        self._now = float(value)
