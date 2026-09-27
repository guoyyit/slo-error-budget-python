"""Availability semantics, idempotency and late/out-of-order ingestion."""
from __future__ import annotations

import pytest

from slo_tracker.clock import ManualClock
from slo_tracker.config import ServiceSLO
from slo_tracker.eventlog import EventLog
from slo_tracker.models import AvailabilityState, IngestStatus, Observation
from slo_tracker.service import SloService
from slo_tracker.windows import window_metric


def make_service(clock: ManualClock | None = None) -> SloService:
    slo = ServiceSLO(
        name="svc",
        target_availability=0.9,
        long_window_seconds=600.0,
        short_window_seconds=60.0,
        budget_period_seconds=1000.0,
    )
    return SloService([slo], clock=clock or ManualClock(1000.0))


def test_empty_window_is_unknown_not_hundred_percent():
    metric = window_metric(EventLog(), 60.0, now=100.0, error_fraction=0.1)
    assert metric.state is AvailabilityState.UNKNOWN
    assert metric.availability is None
    assert metric.burn_rate is None
    assert metric.total == 0


def test_service_queries_unknown_before_any_traffic():
    svc = make_service()
    br = svc.burn_rate("svc", now=1000.0)
    assert br.short_window.state is AvailabilityState.UNKNOWN
    assert br.long_window.state is AvailabilityState.UNKNOWN
    assert br.short_window.availability is None


def test_availability_and_burn_rate_values():
    svc = make_service()
    for i in range(9):
        r = svc.ingest(Observation(f"ok{i}", "svc", 995.0 + i * 0.1, True))
        assert r.status is IngestStatus.ACCEPTED
    assert svc.ingest(Observation("bad", "svc", 999.5, False)).status is IngestStatus.ACCEPTED
    br = svc.burn_rate("svc", now=1000.0)
    assert br.short_window.total == 10
    assert br.short_window.failures == 1
    assert br.short_window.availability == pytest.approx(0.9)
    assert br.short_window.burn_rate == pytest.approx(1.0)


def test_duplicate_replay_is_rejected_and_not_double_counted():
    svc = make_service(ManualClock(1000.0))
    event = Observation("dup-1", "svc", 999.0, False)
    assert svc.ingest(event).status is IngestStatus.ACCEPTED
    second = svc.ingest(event)
    third = svc.ingest(Observation("dup-1", "svc", 999.0, False))
    assert second.status is IngestStatus.DUPLICATE
    assert third.status is IngestStatus.DUPLICATE
    assert "already committed" in second.detail
    assert svc.event_count("svc") == 1


def test_out_of_order_events_are_accepted_and_windowed_by_timestamp():
    svc = make_service(ManualClock(1000.0))
    for ts in (999.0, 998.0, 997.0):
        assert svc.ingest(Observation(f"e-{ts}", "svc", ts, True)).status is IngestStatus.ACCEPTED
    br = svc.burn_rate("svc", now=1000.0)
    assert br.short_window.total == 3
    assert svc.ingest(Observation("old-but-retained", "svc", 905.0, True)).status is IngestStatus.ACCEPTED
    br = svc.burn_rate("svc", now=1000.0)
    assert br.short_window.total == 3
    assert br.long_window.total == 4


def test_late_event_beyond_retention_is_rejected_with_distinct_reason():
    svc = make_service(ManualClock(2000.0))
    result = svc.ingest(Observation("too-late", "svc", 100.0, False))
    assert result.status is IngestStatus.LATE_OUTSIDE_RETENTION
    assert "retention" in result.detail
    assert svc.event_count("svc") == 0


def test_future_event_is_rejected_with_distinct_reason():
    svc = make_service(ManualClock(1000.0))
    result = svc.ingest(Observation("future", "svc", 5000.0, True))
    assert result.status is IngestStatus.FUTURE_EVENT
    assert "ahead" in result.detail
    assert svc.event_count("svc") == 0


def test_unknown_service_rejected():
    result = make_service().ingest(Observation("x", "ghost", 999.0, True))
    assert result.status is IngestStatus.UNKNOWN_SERVICE


def test_late_event_cannot_enter_a_window_anchored_before_it():
    svc = make_service(ManualClock(1000.0))
    for i in range(10):
        svc.ingest(Observation(f"h{i}", "svc", 990.0 + i * 0.5, True))
    before = svc.burn_rate("svc", now=994.0)
    assert svc.ingest(Observation("late-ok", "svc", 995.0, False)).status is IngestStatus.ACCEPTED
    after = svc.burn_rate("svc", now=994.0)
    assert after.short_window.total == before.short_window.total
