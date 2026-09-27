"""Error-budget accounting over deterministic tumbling periods.

Budget model
------------
For a period the allowed failure count is::

    budget = (1 - target_availability) * total_requests_in_period

and ``budget_spent`` is the actual failure count. The budget is exhausted
when failures exceed the allowance (observed availability is below target).

Period boundaries are deterministic: period ``k`` is the half-open
``[k*period_seconds, (k+1)*period_seconds)`` anchored at the Unix epoch.
A fresh period is a configured recomputation boundary — budget state never
auto-resets *inside* a period; rolling into a new period recomputes from
that period's observations only.
"""
from __future__ import annotations

import math

from .config import ServiceSLO
from .eventlog import EventLog
from .models import BudgetSnapshot, BudgetState


def period_index_for(slo: ServiceSLO, now: float) -> int:
    """Deterministic index of the tumbling period containing ``now``."""
    return math.floor(now / slo.budget_period_seconds)


def period_bounds(slo: ServiceSLO, index: int) -> tuple[float, float]:
    """Return ``[start, end)`` epoch boundaries for period ``index``."""
    start = index * slo.budget_period_seconds
    return start, start + slo.budget_period_seconds


def compute_budget(log: EventLog, slo: ServiceSLO, now: float) -> BudgetSnapshot:
    """Compute the *raw* budget snapshot for the period containing ``now``.

    No traffic yields ``state == WITHIN`` with zero spent/remaining rather
    than a fabricated availability: budget exhaustion cannot be decided
    without requests. Latching of exhaustion and explicit resets are the
    service layer's responsibility (see :class:`SloService`).
    """
    index = period_index_for(slo, now)
    start, end = period_bounds(slo, index)
    # Only observations up to "now" and within the period count.
    total, failures = log.window_counts(start, min(end, now))
    allowed = (1.0 - slo.target_availability) * total
    spent = float(failures)
    remaining = allowed - spent
    state = BudgetState.EXHAUSTED if total > 0 and spent > allowed else BudgetState.WITHIN
    return BudgetSnapshot(
        period_index=index,
        period_start=start,
        period_end=end,
        state=state,
        total=total,
        failures=failures,
        allowed_failures=allowed,
        budget_spent=spent,
        budget_remaining=remaining,
    )
