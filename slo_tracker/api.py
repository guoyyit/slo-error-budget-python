"""FastAPI application exposing the SLO service over loopback."""

from __future__ import annotations

from .clock import Clock
from .service import SLOService
from .storage import EventStore


def create_app(
    service: SLOService | None = None,
    store: EventStore | None = None,
    clock: Clock | None = None,
    data_dir: str = "./data",
):
    """Build the FastAPI app.

    A real :class:`SLOService` and on-disk store are wired by default; tests
    may inject their own service / clock.
    """
    raise NotImplementedError
