"""Rolling-window availability and burn-rate computation.

Window semantics: a query evaluated at time ``t`` with window length ``W``
covers event timestamps in the half-open range ``(t - W, t]`` (equivalently
``[start, end)`` with ``end == t``). Both long and short windows read the
same :class:`~slo_tracker.eventlog.EventLog`, so they are always derived
from one committed observation set.
"""
from __future__ import annotations

from .config import ServiceSLO
from .eventlog import EventLog
from .models import AvailabilityState, BurnRateResult, WindowMetric


def window_metric(
    log: EventLog, window_seconds: float, now: float, error_fraction: float
) -> WindowMetric:
    """Compute availability / burn rate for ``(now-window, now]``.

    ``error_fraction`` is the SLO's allowed failure budget, i.e.
    ``1 - target_availability``. Burn rate is the observed failure
    fraction divided by the allowed failure fraction.

    An empty window returns :attr:`AvailabilityState.UNKNOWN` with
    ``availability`` and ``burn_rate`` both ``None``: no traffic means the
    availability is genuinely unknown, never implicitly 100% or 0%.
    """
    start = now - window_seconds
    total, failures = log.window_counts(start, now)
    if total == 0:
        return WindowMetric(
            window_seconds=window_seconds,
            total=0,
            failures=0,
            availability=None,
            burn_rate=None,
            state=AvailabilityState.UNKNOWN,
        )
    availability = (total - failures) / total
    observed_error_fraction = failures / total
    burn_rate = observed_error_fraction / error_fraction
    return WindowMetric(
        window_seconds=window_seconds,
        total=total,
        failures=failures,
       availability=availability,
        burn_rate=burn_rate,
        state=AvailabilityState.OK,
    )


def burn_rate(log: EventLog, slo: ServiceSLO, now: float) -> BurnRateResult:
    """Compute long + short window metrics from one shared event log."""
    error_fraction = 1.0 - slo.target_availability
    long_metric = window_metric(log, slo.long_window_seconds, now, error_fraction)
    short_metric = window_metric(log, slo.short_window_seconds, now, error_fraction)
    return BurnRateResult(long_window=long_metric, short_window=short_metric)
