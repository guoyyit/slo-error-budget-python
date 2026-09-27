"""Core domain types for SLO / error-budget tracking.

All value objects are immutable. Timestamps are epoch seconds (float) so the
same code works with simulated clock sources (local loopback / in-memory
pipe) and with real wall-clock time.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass


class IngestStatus(enum.Enum):
    """Outcome of attempting to accept an observation."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE_OUTSIDE_RETENTION = "late_outside_retention"
    FUTURE_EVENT = "future_event"
    UNKNOWN_SERVICE = "unknown_service"


class AvailabilityState(enum.Enum):
    """How to interpret an availability reading.

    A window with no observations must never be reported as 100% (or 0%)
    available; it is ``UNKNOWN`` instead.
    """

    OK = "ok"
    UNKNOWN = "unknown"


class BudgetState(enum.Enum):
    """Lifecycle state of one error-budget period."""

    WITHIN = "within"
    EXHAUSTED = "exhausted"
    RESET = "reset"


class AlertState(enum.Enum):
    """Per-rule alert lifecycle state."""

    INACTIVE = "inactive"
    FIRING = "firing"
    SUPPRESSED = "suppressed"
    RESOLVED = "resolved"


@dataclass(frozen=True, slots=True)
class Observation:
    """A single request outcome, identified by a client-supplied id.

    ``event_id`` makes ingestion idempotent: replaying the same observation
    is rejected with ``IngestStatus.DUPLICATE`` instead of counted twice.
    """

    event_id: str
    service: str
    timestamp: float
    success: bool


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: IngestStatus
    event_id: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class WindowMetric:
    """Availability measured over one rolling window.

    ``state == UNKNOWN`` means the window contained no observations; in that
    case ``availability`` and ``burn_rate`` are ``None`` rather than 1.0/0.0.
    """

    window_seconds: float
    total: int
    failures: int
    availability: float | None
    burn_rate: float | None
    state: AvailabilityState


@dataclass(frozen=True, slots=True)
class BurnRateResult:
    """Long/short window burn rates derived from one shared event source."""

    long_window: WindowMetric
    short_window: WindowMetric


@dataclass(frozen=True, slots=True)
class BudgetSnapshot:
    """Deterministic snapshot of an error-budget period.

    Period identity is the integer ``period_index``; boundaries are
    ``[period_start, period_end)``.
    """

    period_index: int
    period_start: float
    period_end: float
    state: BudgetState
    total: int
    failures: int
    allowed_failures: float
    budget_spent: float
    budget_remaining: float


@dataclass(frozen=True, slots=True)
class AlertSnapshot:
    rule_name: str
    state: AlertState
    firing: bool
    long_window_seconds: float
    short_window_seconds: float
    long_burn_rate: float | None
    short_burn_rate: float | None
    last_fired_at: float | None
    cooldown_until: float | None
    reason: str
