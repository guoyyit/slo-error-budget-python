"""Configuration objects for services, burn-rate rules and persistence."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class BurnRateRule:
    """A multi-window, multi-burnrate alerting rule.

    An alert fires only when *both* windows exceed their burn-rate
    thresholds at the same evaluation (long-window guard + short-window
    fast trigger).
    """

    name: str
    long_window_seconds: float
    short_window_seconds: float
    long_burn_rate_threshold: float
    short_burn_rate_threshold: float
    cooldown_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("rule name must not be empty")
        if self.short_window_seconds <= 0 or self.long_window_seconds <= 0:
            raise ValueError("window lengths must be positive")
        if self.short_window_seconds > self.long_window_seconds:
            raise ValueError("short window must not be longer than long window")
        if self.long_burn_rate_threshold <= 0 or self.short_burn_rate_threshold <= 0:
            raise ValueError("burn-rate thresholds must be positive")
        if self.cooldown_seconds < 0:
            raise ValueError("cooldown must not be negative")


@dataclass(frozen=True, slots=True)
class ServiceSLO:
    """Per-service availability target and budget/alerting windows."""

    name: str
    target_availability: float
    long_window_seconds: float
    short_window_seconds: float
    budget_period_seconds: float
    max_future_skew_seconds: float = 60.0
    rules: tuple[BurnRateRule, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("service name must not be empty")
        if not 0.0 < self.target_availability < 1.0:
            raise ValueError("target_availability must be within (0, 1)")
        if self.short_window_seconds <= 0 or self.long_window_seconds <= 0:
            raise ValueError("window lengths must be positive")
        if self.short_window_seconds > self.long_window_seconds:
            raise ValueError("short window must not be longer than long window")
        if self.budget_period_seconds <= 0:
            raise ValueError("budget period must be positive")
        if self.max_future_skew_seconds < 0:
            raise ValueError("future skew must not be negative")

    @property
    def retention_seconds(self) -> float:
        """How long after wall-clock ingestion an old event is still accepted.

        Equal to the longest analysis window; events older than this cannot
        affect any current window or budget period, so they are rejected.
        """
        return max(self.long_window_seconds, self.budget_period_seconds)


@dataclass(frozen=True, slots=True)
class StoreConfig:
    """Directory and durability settings for the write-ahead log."""

    data_dir: str
    fsync: bool = True
