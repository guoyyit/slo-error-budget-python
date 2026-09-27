"""Requirement 7: local persistence and crash recovery."""

from __future__ import annotations

from slo_tracker.clock import FakeClock
from slo_tracker.models import (
    AlertConditionState,
    Outcome,
    RejectReason,
)
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

from .conftest import make_config, obs


def test_committed_observations_survive_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    svc.ingest(obs("a", 990.0, Outcome.SUCCESS))
    svc.ingest(obs("b", 991.0, Outcome.FAILURE))

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    snap = restarted.query("svc")
    assert {o.observation_id for o in snap.observations} == {"a", "b"}
    assert snap.availability.total == 2
    assert snap.availability.failures == 1


def test_alert_state_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    svc.ingest(obs("f1", 996.0, Outcome.FAILURE))
    svc.ingest(obs("f2", 997.0, Outcome.FAILURE))
    assert svc.alert_status("svc").state is AlertConditionState.FIRING

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    alert = restarted.alert_status("svc")
    assert alert.state is AlertConditionState.FIRING
    assert alert.last_fired_at == 1000.0
    assert alert.cooldown_until == 1300.0


def test_exhausted_budget_latch_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config(budget_expected_requests=1000.0))
    clock.set(1200.0)
    for i in range(100):
        svc.ingest(obs(f"f{i}", 1000.5 + i * 0.01, Outcome.FAILURE))
    assert svc.query("svc").budget.exhausted is True

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    assert restarted.query("svc").budget.exhausted is True
    # Latch survives even after successes post-restart.
    restarted.ingest(obs("ok1", 1002.0, Outcome.SUCCESS))
    assert restarted.query("svc").budget.exhausted is True


def test_sealing_watermark_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    svc.ingest(obs("f1", 996.0, Outcome.FAILURE))
    svc.ingest(obs("f2", 997.0, Outcome.FAILURE))

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    accepted, reason, _ = restarted.ingest(obs("old", 500.0, Outcome.SUCCESS))
    assert accepted is False
    assert reason is RejectReason.TOO_LATE


def test_unfinished_write_not_counted_as_observation(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    svc.ingest(obs("committed", 990.0, Outcome.SUCCESS))

    # Simulate a crash that leaves a partial (un-framed / torn) tail write.
    log = tmp_data_dir / "observations.jsonl"
    with log.open("ab") as handle:
        handle.write(b'7.999.{"observation_id": "torn", "se')

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    ids = {o.observation_id for o in restarted.query("svc").observations}
    assert ids == {"committed"}

    # The torn id was never committed, so re-submitting it is a fresh accept
    # (not a duplicate).
    accepted, reason, _ = restarted.ingest(obs("torn", 991.0, Outcome.SUCCESS))
    assert accepted is True, reason is None


def test_recovery_idempotent_across_multiple_restarts(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    svc.ingest(obs("x", 990.0, Outcome.SUCCESS))

    for _ in range(3):
        again = SLOService(EventStore(tmp_data_dir), clock)
        assert len(again.query("svc").observations) == 1
        # Duplicate dedup still works after each restart.
        accepted, reason, _ = again.ingest(obs("x", 990.0, Outcome.SUCCESS))
        assert accepted is False and reason is RejectReason.DUPLICATE
