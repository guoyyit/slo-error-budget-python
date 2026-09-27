"""Pure windowing / availability / burn-rate computations.

Everything here is stateless and derived only from the observation list it
is given, so short and long windows are always computed from the same source
of truth.

Window convention: half-open intervals ``[start, end)`` evaluated at an
explicit ``now``. A window with no observations yields *unknown*, never 0% or
100%.
"""

from __future__ import annotations

import math

from .models import (
    AvailabilityResult,
    AvailabilityStatus,
    BudgetState,
    Observation,
    Outcome,
    SLOConfig,
    WindowBurn,
)


def in_window(obs: Observation, start: float, end: float) -> bool:
    """Return True if the observation's event time is in ``[start, end)``."""
    return start <= obs.timestamp < end


def window_slice(
    observations: list[Observation], start: float, end: float
) -> list[Observation]:
    """Return observations whose event time lies in ``[start, end)``."""
    return [obs for obs in observations if in_window(obs, start, end)]


def _counts(items: list[Observation]) -> tuple[int, int, int]:
    """Return ``(total, successes, failures)``."""
    total = len(items)
    successes = sum(1 for obs in items if obs.outcome is Outcome.SUCCESS)
    return total, successes, total - successes


def compute_availability(
    config: SLOConfig, observations: list[Observation], now: float
) -> AvailabilityResult:
    """Availability over ``[now - availability_window, now)``.

    Empty windows return :pyattr:`AvailabilityStatus.UNKNOWN` so an idle
    service is never misreported as 100% or 0% available.
    """
    start = now - config.availability_window_seconds
    items = window_slice(observations, start, now)
    total, successes, failures = _counts(items)
    if total == 0:
        return AvailabilityResult(
            service=config.service,
            status=AvailabilityStatus.UNKNOWN,
            window_start=start,
            window_end=now,
            total=0,
            successes=0,
            failures=0,
            ratio=None,
            reason="no_observations_in_window",
        )
    return AvailabilityResult(
        service=config.service,
        status=AvailabilityStatus.KNOWN,
        window_start=start,
        window_end=now,
        total=total,
        successes=successes,
        failures=failures,
        ratio=successes / total,
        reason=None,
    )


def _burn(items: list[Observation], allowed_error_rate: float) -> tuple[
    int, int, float | None, float | None
]:
    """Return ``(total, failures, error_rate, burn_rate)`` for one window."""
    total = len(items)
    failures = sum(1 for obs in items if obs.outcome is Outcome.FAILURE)
    if total == 0:
        return 0, 0, None, None
    error_rate = failures / total
    if allowed_error_rate <= 0:
        # A perfect target tolerates zero errors: any failure is infinite burn.
        burn = float("inf") if failures > 0 else 0.0
    else:
        burn = error_rate / allowed_error_rate
    return total, failures, error_rate, burn


def compute_window_burn(
    config: SLOConfig,
    observations: list[Observation],
    now: float,
    window_seconds: float,
) -> WindowBurn:
    """Burn rate for one window length, derived from ``observations``."""
    start = now - window_seconds
    items = window_slice(observations, start, now)
    total, failures, error_rate, burn = _burn(
        items, 1.0 - config.availability_target
    )
    return WindowBurn(
        window_seconds=window_seconds,
        window_start=start,
        window_end=now,
        total=total,
        failures=failures,
        error_rate=error_rate,
        burn_rate=burn,
    )


def align_period_start(now: float, window_seconds: float, anchor: float = 0.0) -> float:
    """Deterministic budget-period boundary at or before ``now``.

    Boundaries are ``anchor + k * window_seconds`` for integer ``k``, so the
    old/new budget boundary is never ambiguous.
    """
    if window_seconds <= 0:
        return anchor
    k = math.floor((now - anchor) / window_seconds)
    return anchor + k * window_seconds


def compute_budget(
    config: SLOConfig,
    observations: list[Observation],
    period_start: float,
    period_end: float | None,
    now: float,
) -> BudgetState:
    """Error-budget consumption for the anchored period ``[start, end)``."""
    end = period_end if period_end is not None else now
    items = window_slice(observations, period_start, end)
    total, _successes, failures = _counts(items)

    expected = (
        config.budget_expected_requests
        if config.budget_expected_requests is not None
        else float(total)
    )
    total_budget = max(expected, 0.0) * (1.0 - config.availability_target)
    consumed = float(failures)
    remaining = total_budget - consumed

    if total_budget <= 0:
        burn_fraction = float("inf") if failures > 0 else 0.0
        exhausted = failures > 0
    else:
        burn_fraction = consumed / total_budget
        exhausted = consumed >= total_budget and total > 0

    return BudgetState(
        service=config.service,
        period_start=period_start,
        period_end=period_end,
        availability_target=config.availability_target,
        total_requests=total,
        failures=failures,
        total_budget=total_budget,
        consumed=consumed,
        remaining=remaining,
        burn_fraction=burn_fraction,
        exhausted=exhausted,
    )
