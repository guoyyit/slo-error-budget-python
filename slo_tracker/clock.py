"""Clock abstraction so tests and the local demo can control time."""
from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Return current epoch seconds."""
        ...


class SystemClock:
    """Wall-clock source used by the real loopback service."""

    def now(self) -> float:
        return time.time()


class ManualClock:
    """Deterministic clock driven explicitly by tests / simulation."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def set(self, value: float) -> None:
        if value < self._now:
            raise ValueError("manual clock cannot move backwards")
        self._now = float(value)

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("cannot advance by a negative duration")
        self._now += float(seconds)
