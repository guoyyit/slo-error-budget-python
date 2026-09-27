"""Requirement 1: availability targets and unknown state on empty windows."""

from __future__ import annotations

from slo_tracker.models import (
    AvailabilityStatus,
    Observation,
    Outcome,
    SLOConfig,
)
from slo_tracker.windows import compute_availability


def config() -> SLOConfig:
    return SLOConfig(
        service="svc",
        availability_target=0.99,
        availability_window_seconds=300.0,
        short_window_seconds=60.0,
        long_window_seconds=300.0,
        alert_short_burn=14.4,
        alert_long_burn=14.4,
        cooldown_seconds=300.0,
        budget_window_seconds=3600.0,
        budget_expected_requests=None,
    )


def obs(oid: str, ts: float, outcome: Outcome) -> Observation:
    return Observation(
        observation_id=oid,
        service="svc",
        timestamp=ts,
        outcome=outcome,
        ingest_time=ts,
    )


def test_empty_window_is_unknown_not_zero_or_hundred() -> None:
    result = compute_availability(config(), [], now=1000.0)
    assert result.status is AvailabilityStatus.UNKNOWN
    assert result.ratio is None
    assert result.total == 0
    assert result.reason == "no_observations_in_window"


def test_window_with_only_old_observations_is_unknown() -> None:
    # Events older than the rolling 300s window do not count.
    events = [
        obs("o1", 600.0, Outcome.SUCCESS),
        obs("o2", 699.0, Outcome.FAILURE),
    ]
    result = compute_availability(config(), events, now=1000.0)
    assert result.status is AvailabilityStatus.UNKNOWN
    assert result.total == 0


def test_all_success_reports_one_hundred_known() -> None:
    events = [obs(f"o{i}", 900.0 + i, Outcome.SUCCESS) for i in range(5)]
    result = compute_availability(config(), events, now=1000.0)
    assert result.status is AvailabilityStatus.KNOWN
    assert result.ratio == 1.0
    assert result.total == 5
    assert result.failures == 0


def test_mixed_results_ratio() -> None:
    events = [
        obs("o1", 900.0, Outcome.SUCCESS),
        obs("o2", 901.0, Outcome.SUCCESS),
        obs("o3", 902.0, Outcome.SUCCESS),
        obs("o4", 903.0, Outcome.FAILURE),
    ]
    result = compute_availability(config(), events, now=1000.0)
    assert result.status is AvailabilityStatus.KNOWN
    assert result.total == 4
    assert result.successes == 3
    assert result.failures == 1
    assert result.ratio == 0.75


def test_event_at_window_start_included_end_excluded() -> None:
    # [700, 1000): event at exactly 700 included, event at 1000 excluded.
    events = [
        obs("at-start", 700.0, Outcome.SUCCESS),
        obs("at-end", 1000.0, Outcome.FAILURE),
    ]
    result = compute_availability(config(), events, now=1000.0)
    assert result.total == 1
    assert result.successes == 1
