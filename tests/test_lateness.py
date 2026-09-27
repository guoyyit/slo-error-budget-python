"""Requirement 4: late/out-of-order observations, duplicates, reject reasons."""

from __future__ import annotations

from slo_tracker.clock import FakeClock
from slo_tracker.models import (
    AlertConditionState,
    Observation,
    Outcome,
    RejectReason,
)
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

from .conftest import make_config, obs


def test_duplicate_replay_is_rejected_and_not_double_counted(service: SLOService) -> None:
    accepted, reason, _ = service.ingest(obs("x1", 950.0, Outcome.SUCCESS))
    assert accepted
    # Replay the exact same observation (including id).
    accepted2, reason2, _ = service.ingest(obs("x1", 950.0, Outcome.SUCCESS))
    assert accepted2 is False
    assert reason2 is RejectReason.DUPLICATE

    snap = service.query("svc")
    assert snap.availability.total == 1
    assert snap.short_burn.total == 1


def test_duplicate_and_late_have_distinguishable_reasons(service: SLOService) -> None:
    assert service.ingest(obs("dup", 950.0, Outcome.SUCCESS))[0] is True
    dup_reason = service.ingest(obs("dup", 950.0, Outcome.SUCCESS))[1]
    assert dup_reason is RejectReason.DUPLICATE

    # Trigger an alert to seal history (watermark = 1000 - 300 = 700).
    _fire_alert(service)
    late_reason = service.ingest(obs("brand-new-old-event", 650.0, Outcome.SUCCESS))[1]
    assert late_reason is RejectReason.TOO_LATE
    assert dup_reason is not late_reason


def _fire_alert(service: SLOService) -> None:
    # 4 events with 2 failures in both short and long window => 5x burn >= 2.
    service.ingest(obs("f1", 996.0, Outcome.FAILURE))
    service.ingest(obs("f2", 997.0, Outcome.FAILURE))
    service.ingest(obs("s1", 998.0, Outcome.SUCCESS))
    service.ingest(obs("s2", 999.0, Outcome.SUCCESS))


def test_out_of_order_within_window_is_accepted(service: SLOService) -> None:
    # Arrive out of event-time order but inside the accepted horizon.
    assert service.ingest(obs("b", 980.0, Outcome.SUCCESS))[0] is True
    assert service.ingest(obs("a", 970.0, Outcome.FAILURE))[0] is True
    snap = service.query("svc")
    assert snap.availability.total == 2
    assert snap.availability.failures == 1


def test_late_success_after_alert_does_not_change_fired_result(
    tmp_data_dir, clock: FakeClock
) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    _fire_alert(svc)
    first = svc.alert_status("svc")
    assert first.state is AlertConditionState.FIRING
    fired_at = first.last_fired_at

    # A success arriving after the alert, with event time inside the now
    # current short window, is still accepted (it is new, not sealed history).
    accepted, reason, _ = svc.ingest(obs("new-success", 999.5, Outcome.SUCCESS))
    # 999.5 >= watermark 700 -> accepted
    assert accepted, reason

    # But a genuinely late event (before watermark) is refused.
    accepted2, reason2, _ = svc.ingest(obs("late-success", 699.0, Outcome.SUCCESS))
    assert accepted2 is False
    assert reason2 is RejectReason.TOO_LATE

    # The originally fired episode timestamp is unchanged.
    second = svc.alert_status("svc")
    assert second.last_fired_at == fired_at


def test_unknown_service_rejected(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    accepted, reason, detail = svc.ingest(
        Observation("z", "nope", 1000.0, Outcome.SUCCESS)
    )
    assert accepted is False
    assert reason is RejectReason.UNKNOWN_SERVICE
    assert "not configured" in detail


def test_persistence_dedup_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    assert svc.ingest(obs("persisted", 950.0, Outcome.SUCCESS))[0] is True

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    accepted, reason, _ = restarted.ingest(obs("persisted", 950.0, Outcome.SUCCESS))
    assert accepted is False
    assert reason is RejectReason.DUPLICATE
