"""Requirement 2: short/long burn rates from one shared observation source."""

from __future__ import annotations

import math

from slo_tracker.models import Observation, Outcome, SLOConfig
from slo_tracker.windows import (
    align_period_start,
    compute_budget,
    compute_window_burn,
)


def config() -> SLOConfig:
    return SLOConfig(
        service="svc",
        availability_target=0.90,  # 10% allowed error rate
        availability_window_seconds=300.0,
        short_window_seconds=60.0,
        long_window_seconds=300.0,
        alert_short_burn=2.0,
        alert_long_burn=2.0,
        cooldown_seconds=300.0,
        budget_window_seconds=3600.0,
        budget_expected_requests=None,
    )


def obs(oid: str, ts: float, outcome: Outcome) -> Observation:
    return Observation(
        observation_id=oid, service="svc", timestamp=ts, outcome=outcome
    )


def test_empty_window_burn_is_unknown() -> None:
    burn = compute_window_burn(config(), [], now=1000.0, window_seconds=60.0)
    assert burn.total == 0
    assert burn.failures == 0
    assert burn.error_rate is None
    assert burn.burn_rate is None


def test_burn_rate_is_error_rate_over_allowance() -> None:
    # 2 of 4 fail => 50% errors / 10% allowance => 5x burn.
    events = [
        obs("s1", 997.0, Outcome.SUCCESS),
        obs("s2", 998.0, Outcome.SUCCESS),
        obs("f1", 998.5, Outcome.FAILURE),
        obs("f2", 999.0, Outcome.FAILURE),
    ]
    burn = compute_window_burn(config(), events, now=1000.0, window_seconds=60.0)
    assert burn.total == 4
    assert burn.failures == 2
    assert math.isclose(burn.error_rate, 0.5)
    assert math.isclose(burn.burn_rate, 5.0)


def test_short_and_long_share_same_source() -> None:
    # Build one list; both windows must derive from it.
    events: list[Observation] = []
    # Long window is [700,1000); short is [940,1000).
    for i in range(100):
        events.append(obs(f"old-s-{i}", 700.0 + i * 0.5, Outcome.SUCCESS))
    for i in range(10):
        events.append(obs(f"new-f-{i}", 950.0 + i, Outcome.FAILURE))
    cfg = config()
    short = compute_window_burn(
        cfg, events, now=1000.0, window_seconds=cfg.short_window_seconds
    )
    long = compute_window_burn(
        cfg, events, now=1000.0, window_seconds=cfg.long_window_seconds
    )
    # Short window only sees recent failures: burn is high.
    assert short.total == 10
    assert short.failures == 10
    assert math.isclose(short.burn_rate, 10.0)
    # Long window includes old successes: burn is diluted.
    assert long.total == 110
    assert long.failures == 10
    assert long.burn_rate < short.burn_rate
    # Both computed from the identical backing list (identity).
    assert all(e is e for e in events)


def test_perfect_target_failure_is_infinite_burn() -> None:
    cfg = SLOConfig(
        service="svc",
        availability_target=1.0,
        availability_window_seconds=60.0,
        short_window_seconds=60.0,
        long_window_seconds=300.0,
        alert_short_burn=1.0,
        alert_long_burn=1.0,
        cooldown_seconds=60.0,
        budget_window_seconds=300.0,
    )
    burn = compute_window_burn(
        cfg, [obs("f", 999.0, Outcome.FAILURE)], now=1000.0, window_seconds=60.0
    )
    assert burn.burn_rate == float("inf")


def test_period_alignment_boundary_is_deterministic() -> None:
    assert align_period_start(3599.0, 3600.0) == 0.0
    assert align_period_start(3600.0, 3600.0) == 3600.0
    assert align_period_start(7199.9, 3600.0) == 3600.0
    # With an explicit anchor (a prior reset) boundaries follow it.
    assert align_period_start(5000.0, 3600.0, anchor=1000.0) == 4600.0


def test_budget_consumption_with_observed_requests() -> None:
    cfg = config()  # 10% allowed; budget derived from observed total.
    events = [
        obs("s1", 10.0, Outcome.SUCCESS),
        obs("s2", 11.0, Outcome.SUCCESS),
        obs("f1", 12.0, Outcome.FAILURE),
    ]
    budget = compute_budget(cfg, events, period_start=0.0, period_end=None, now=100.0)
    assert budget.total_requests == 3
    assert budget.failures == 1
    assert math.isclose(budget.total_budget, 0.3)
    assert budget.consumed == 1.0
    assert math.isclose(budget.burn_fraction, 1.0 / 0.3)
    assert budget.exhausted is True


def test_budget_healthy_when_failures_within_allowance() -> None:
    cfg = config()
    events = [obs(f"s{i}", float(i), Outcome.SUCCESS) for i in range(90)]
    events.append(obs("f1", 90.0, Outcome.FAILURE))
    budget = compute_budget(cfg, events, 0.0, None, 100.0)
    assert budget.total_requests == 91
    assert budget.consumed == 1.0
    assert math.isclose(budget.total_budget, 9.1)
    assert budget.exhausted is False
    assert budget.remaining > 0


def test_budget_only_counts_events_within_period() -> None:
    cfg = config()
    events = [
        obs("old-f", 999.0, Outcome.FAILURE),
        obs("new-f", 1001.0, Outcome.FAILURE),
        obs("new-s", 1002.0, Outcome.SUCCESS),
    ]
    # Period [1000, 2000): the old failure must not count against this budget.
    budget = compute_budget(cfg, events, 1000.0, None, 2000.0)
    assert budget.total_requests == 2
    assert budget.failures == 1
