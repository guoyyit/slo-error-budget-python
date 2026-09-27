"""Local in-memory observation pipeline (loopback).

No external broker is required: observations flow through a thread-safe
in-process queue fed by :meth:`InMemoryPipeline.submit` and drained into the
:class:`~slo_tracker.service.SLOService` by :meth:`drain`. This makes the
whole system reproducible locally with no network or credentials.
"""

from __future__ import annotations

import queue
from collections.abc import Callable

from .models import Observation, RejectReason

# A submitter returns (accepted, reason, detail), just like SLOService.ingest.
Submitter = Callable[
    [Observation], tuple[bool, RejectReason | None, str]
]


class InMemoryPipeline:
    """Thread-safe loopback pipe for observations."""

    def __init__(self, submitter: Submitter) -> None:
        self._submit = submitter
        self._queue: queue.Queue[Observation] = queue.Queue()
        self._results: list[
            tuple[Observation, bool, RejectReason | None, str]
        ] = []

    def submit(self, observation: Observation) -> None:
        """Enqueue one observation for later delivery (never blocks on the SLO)."""
        self._queue.put(observation)

    def drain(self, timeout: float | None = None, max_items: int | None = None) -> int:
        """Deliver queued observations to the service.

        Drains until the queue is empty, or until ``max_items`` have been
        delivered, or until an empty-poll exceeds ``timeout`` seconds.
        Returns the number of observations delivered (accepted or rejected).
        """
        delivered = 0
        while max_items is None or delivered < max_items:
            try:
                if delivered == 0:
                    observation = self._queue.get(timeout=timeout)
                else:
                    observation = self._queue.get_nowait()
            except queue.Empty:
                break
            accepted, reason, detail = self._submit(observation)
            self._results.append((observation, accepted, reason, detail))
            delivered += 1
        return delivered

    @property
    def results(
        self,
    ) -> list[tuple[Observation, bool, RejectReason | None, str]]:
        """All delivery results recorded so far, in delivery order."""
        return list(self._results)

    def accepted(self) -> list[Observation]:
        return [obs for obs, ok, _r, _d in self._results if ok]

    def rejected(
        self,
    ) -> list[tuple[Observation, RejectReason, str]]:
        return [
            (obs, reason, detail)
            for obs, ok, reason, detail in self._results
            if not ok and reason is not None
        ]

    def run_demo(
        self,
        observations: list[Observation],
        timeout: float | None = None,
    ) -> list[tuple[Observation, bool, RejectReason | None, str]]:
        """Submit a batch and drain it; returns the per-observation outcomes."""
        for observation in observations:
            self.submit(observation)
        self.drain(timeout=timeout)
        return self.results[-len(observations):]
