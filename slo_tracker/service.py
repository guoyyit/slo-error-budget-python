"""Concurrency-safe SLO service: ingest, query, reset, alert evaluation."""

from __future__ import annotations

from .clock import Clock
from .models import (
    AlertStatus,
    AvailabilityResult,
    BudgetState,
    Observation,
    RejectReason,
    SLOConfig,
    ServiceSnapshot,
)
from .storage import EventStore


class SLOService:
    """Coordinates storage, window computation and alert state."""

    def __init__(self, store: EventStore, clock: Clock) -> None:
        raise NotImplementedError

    def configure(self, config: SLOConfig) -> None:
        """Register or update a service configuration."""
        raise NotImplementedError

    def ingest(
        self, observation: Observation
    ) -> tuple[bool, RejectReason | None, str]:
        """Validate and commit one observation.

        Returns ``(accepted, reason, detail)``.
        """
        raise NotImplementedError

    def reset_budget(self, service: str, at: float | None = None) -> BudgetState:
        """Explicitly anchor a fresh error-budget period."""
        raise NotImplementedError

    def query(self, service: str) -> ServiceSnapshot:
        """Return a self-consistent snapshot from one committed point in time."""
        raise NotImplementedError

    def availability(self, service: str) -> AvailabilityResult:
        raise NotImplementedError

    def alert_status(self, service: str) -> AlertStatus:
        raise NotImplementedError

    def shutdown(self) -> None:
        """Durably flush everything before process exit."""
        raise NotImplementedError
