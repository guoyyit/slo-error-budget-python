"""Local in-memory observation pipeline (loopback) and demo data generator.

No external broker is required: observations flow through a thread-safe
in-process queue fed by ``submit`` and drained by the service.
"""

from __future__ import annotations

import queue
from typing import Callable

from .models import Observation, RejectReason


class InMemoryPipeline:
    """Small thread-safe loopback pipe for observations."""

    def __init__(self, submitter: Callable[[Observation], tuple[bool, RejectReason | None, str]]) -> None:
        self._submit = submitter
        self._queue: queue.Queue[Observation] = queue.Queue()
        self._results: list[tuple[Observation, bool, RejectReason | None, str]] = []

    def submit(self, observation: Observation) -> None:
        """Enqueue one observation for asynchronous delivery."""
        raise NotImplementedError

    def drain(self, timeout: float | None = None) -> int:
        """Deliver queued observations to the service; returns delivered count."""
        raise NotImplementedError

    @property
    def results(self) -> list[tuple[Observation, bool, RejectReason | None, str]]:
        raise NotImplementedError

    def run_demo(self, observations: list[Observation], timeout: float | None = None) -> None:
        """Submit a batch and drain it; used by the local demo."""
        raise NotImplementedError
