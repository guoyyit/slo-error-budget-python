"""Pure windowing / availability / burn-rate computations.

Everything here is stateless and derived only from the observation list it
is given, so short and long windows are always computed from the same source
of truth.
"""

from __future__ import annotations

from .models import (
    AvailabilityResult,
    BudgetState,
    Observation,
    SLOConfig,
    WindowBurn,
)


def in_window(obs: Observation, start: float, end: float) -> bool:
    """Return True if the observation's event time is in [start, end)."""
    raise NotImplementedError


def window_slice(
    observations: list[Observation], start: float, end: float
) -> list[Observation]:
    """Return the observations whose event time lies in [start, end)."""
    raise NotImplementedError


def compute_availability(
    config: SLOConfig, observations: list[Observation], now: float
) -> AvailabilityResult:
    """Compute availability over the configured rolling window."""
    raise NotImplementedError


def compute_window_burn(
    config: SLOConfig,
    observations: list[Observation],
    now: float,
    window_seconds: float,
) -> WindowBurn:
    """Compute burn rate for one window length from shared observations."""
    raise NotImplementedError


def compute_budget(
    config: SLOConfig,
    observations: list[Observation],
    period_start: float,
    period_end: float | None,
    now: float,
) -> BudgetState:
    """Compute error-budget consumption for one anchored budget period."""
    raise NotImplementedError
