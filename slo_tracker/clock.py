"""Clock abstraction so tests can advance time deterministically."""

from __future__ import annotations

from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Return the current time in seconds since the epoch."""
        raise NotImplementedError


class SystemClock:
    def now(self) -> float:
        raise NotImplementedError


class FakeClock:
    """Deterministic, manually advanced clock for tests."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def now(self) -> float:
        raise NotImplementedError

    def advance(self, seconds: float) -> None:
        raise NotImplementedError

    def set(self, value: float) -> None:
        raise NotImplementedError
