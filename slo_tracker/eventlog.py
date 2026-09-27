"""Append-only, idempotent, in-memory event log shared by every window.

The log is the *single source of truth*: long windows, short windows and
budget periods are all computed from the same contained events, so their
results can never diverge due to different data feeds.

Concurrency: callers (the service layer) hold the coordinating lock. The
log itself is a plain data structure kept insertion-sorted by timestamp.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right

from .models import Observation


class EventLog:
    def __init__(self) -> None:
        self._by_id: dict[str, Observation] = {}
        # Parallel, kept sorted by (timestamp, event_id) via key lists.
        self._timestamps: list[float] = []
        self._events: list[Observation] = []

    def add(self, event: Observation) -> bool:
        """Add an event; return False (without changing the log) on duplicate."""
        if event.event_id in self._by_id:
            return False
        self._by_id[event.event_id] = event
        key = (event.timestamp, event.event_id)
        # Insertion position keeps the array timestamp-ordered even when
        # events arrive late or out of order.
        lo = bisect_left(self._timestamps, event.timestamp)
        pos = lo
        while pos < len(self._events) and (
            self._events[pos].timestamp, self._events[pos].event_id
        ) < key:
            pos += 1
        self._timestamps.insert(pos, event.timestamp)
        self._events.insert(pos, event)
        return True

    def __contains__(self, event_id: str) -> bool:
        return event_id in self._by_id

    def get(self, event_id: str) -> Observation | None:
        return self._by_id.get(event_id)

    def window_counts(self, start: float, end: float) -> tuple[int, int]:
        """Return ``(total, failures)`` for events in ``[start, end)``."""
        lo = bisect_left(self._timestamps, start)
        hi = bisect_left(self._timestamps, end)
        total = hi - lo
        failures = 0
        for i in range(lo, hi):
            if not self._events[i].success:
                failures += 1
        return total, failures

    def events_in(self, start: float, end: float) -> list[Observation]:
        lo = bisect_left(self._timestamps, start)
        hi = bisect_left(self._timestamps, end)
        return list(self._events[lo:hi])

    def all_events(self) -> list[Observation]:
        return list(self._events)

    def __len__(self) -> int:
        return len(self._by_id)
