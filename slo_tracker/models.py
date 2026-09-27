"""Core type definitions for SLO / error-budget tracking.

Only types live here; all behaviour is implemented in the other modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Outcome(str, Enum):
    """Result of a single request observation."""

    SUCCESS = "success"
    FAILURE = "failure"


class AvailabilityStatus(str, Enum):
    """Whether availability could be computed for a window."""

    KNOWN = "known"
    UNKNOWN = "unknown"


class RejectReason(str, Enum):
    """Distinguishable reasons an observation is rejected at ingest."""

    DUPLICATE = "duplicate"
    TOO_LATE = "too_late"
    UNKNOWN_SERVICE = "unknown_service"
    MALFORMED = "malformed"


class AlertConditionState(str, Enum):
    """Lifecycle state of one multi-window alert condition."""

    # No breach currently observed.
    IDLE = "idle"
    # Thresholds are breached and the alert is actively signalling.
    FIRING = "firing"
    # Thresholds are breached again but cooldown suppresses the re-fire.
    SUPPRESSED_COOLDOWN = "suppressed_cooldown"
    # Was firing before; condition cleared.
    RESOLVED = "resolved"


@dataclass(frozen=True)
class Observation:
    """One request outcome observed for a service at a point in time.

    ``timestamp`` is event time (seconds since epoch); ``ingest_time`` is
    wall-clock receipt time, filled in by the service when stored.
    """

    observation_id: str
    service: str
    timestamp: float
    outcome: Outcome
    ingest_time: float | None = None


@dataclass(frozen=True)
class SLOConfig:
    """Configuration for one service.

    Windows are rolling and expressed in seconds. Burn thresholds compare the
    observed error rate against the budgeted error rate
    (``1 - availability_target``).
    """

    service: str
    availability_target: float
    availability_window_seconds: float
    short_window_seconds: float
    long_window_seconds: float
    alert_short_burn: float
    alert_long_burn: float
    cooldown_seconds: float
    budget_window_seconds: float
    # When None the error budget is derived from the observed request count;
    # when set it is ``expected_requests * (1 - availability_target)``.
    budget_expected_requests: float | None = None


@dataclass(frozen=True)
class AvailabilityResult:
    """Availability for one evaluation of a rolling window."""

    service: str
    status: AvailabilityStatus
    window_start: float
    window_end: float
    total: int
    successes: int
    failures: int
    ratio: float | None
    reason: str | None


@dataclass(frozen=True)
class WindowBurn:
    """Burn-rate computation over a single window, from shared observations."""

    window_seconds: float
    window_start: float
    window_end: float
    total: int
    failures: int
    error_rate: float | None
    burn_rate: float | None


@dataclass(frozen=True)
class BudgetState:
    """Error-budget state for one anchored budget period."""

    service: str
    period_start: float
    period_end: float | None
    availability_target: float
    total_requests: int
    failures: int
    total_budget: float
    consumed: float
    remaining: float
    burn_fraction: float | None
    exhausted: bool


@dataclass(frozen=True)
class AlertStatus:
    """Explainable state of one alert condition at query time."""

    service: str
    state: AlertConditionState
    short_window: WindowBurn | None
    long_window: WindowBurn | None
    last_fired_at: float | None
    cooldown_until: float | None
    breached: bool
    suppressed: bool
    detail: str


@dataclass(frozen=True)
class Rejection:
    """An observation that was refused at ingest, with a distinguishable reason."""

    observation: Observation
    reason: RejectReason
    detail: str


@dataclass(frozen=True)
class ServiceSnapshot:
    """Self-consistent read of everything known about one service at a moment."""

    config: SLOConfig
    evaluated_at: float
    observations: list[Observation] = field(default_factory=list)
    availability: AvailabilityResult | None = None
    short_burn: WindowBurn | None = None
    long_burn: WindowBurn | None = None
    budget: BudgetState | None = None
    alert: AlertStatus | None = None
    watermark: float = float("-inf")
