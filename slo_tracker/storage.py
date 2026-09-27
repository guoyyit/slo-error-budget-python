"""Local persistence: append-only event log + state log with fsync.

The store is the durable source of truth. A record is considered committed
only after it has been appended, flushed and fsynced; a torn trailing record
left by a crash is discarded on recovery.
"""

from __future__ import annotations

from pathlib import Path

from .models import AlertStatus, BudgetState, Observation, SLOConfig


class EventStore:
    """Durable storage for one data directory."""

    def __init__(self, directory: str | Path) -> None:
        raise NotImplementedError

    def append_observation(self, observation: Observation) -> None:
        """Atomically persist one observation; raises on failure."""
        raise NotImplementedError

    def load_observations(self) -> list[Observation]:
        """Return all committed observations, dropping torn trailing bytes."""
        raise NotImplementedError

    def append_state(
        self,
        service: str,
        budget: BudgetState,
        alert: AlertStatus,
        watermark: float,
        period_start: float,
    ) -> None:
        """Persist one committed budget/alert state transition."""
        raise NotImplementedError

    def load_states(self) -> dict:
        """Return the last committed state per service."""
        raise NotImplementedError

    def save_config(self, config: SLOConfig) -> None:
        """Persist a service configuration (overwrite, durable)."""
        raise NotImplementedError

    def load_configs(self) -> dict[str, SLOConfig]:
        """Return all persisted service configurations."""
        raise NotImplementedError

    def close(self) -> None:
        """Flush and release file handles."""
        raise NotImplementedError
