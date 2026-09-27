"""Local WAL persistence: crash recovery and torn-write rejection."""
from __future__ import annotations

import os

from slo_tracker.clock import ManualClock
from slo_tracker.config import BurnRateRule, ServiceSLO, StoreConfig
from slo_tracker.models import AlertState, BudgetState, IngestStatus, Observation
from slo_tracker.service import SloService


def build(clock: ManualClock, data_dir: str) -> SloService:
    slo = ServiceSLO(
        name="svc",
        target_availability=0.9,
        long_window_seconds=600.0,
        short_window_seconds=60.0,
        budget_period_seconds=1000.0,
        rules=(BurnRateRule("p", 600.0, 60.0, 5.0, 5.0, cooldown_seconds=300.0),),
    )
    return SloService([slo], clock=clock, store=StoreConfig(data_dir))


def test_observations_budget_and_alerts_survive_restart(tmp_path):
    d = str(tmp_path)
    clock = ManualClock(1500.0)
    svc = build(clock, d)
    for i in range(5):
        assert svc.ingest(Observation(f"ok{i}", "svc", 1450.0 + i, True)).status is IngestStatus.ACCEPTED
    for i in range(5):
        assert svc.ingest(Observation(f"bad{i}", "svc", 1455.0 + i, False)).status is IngestStatus.ACCEPTED
    assert svc.budget("svc").state is BudgetState.EXHAUSTED
    alert = svc.alerts("svc")[0]
    assert alert.state in (AlertState.FIRING, AlertState.SUPPRESSED)
    svc.close()

    # Simulate process restart by rebuilding from the same directory.
    svc2 = build(clock, d)
    assert svc2.event_count("svc") == 10
    assert svc2.budget("svc").state is BudgetState.EXHAUSTED
    alert2 = svc2.alerts("svc")[0]
    assert alert2.state in (AlertState.FIRING, AlertState.SUPPRESSED)
    assert alert2.last_fired_at is not None
    svc2.close()


def test_explicit_reset_survives_restart(tmp_path):
    d = str(tmp_path)
    clock = ManualClock(1500.0)
    svc = build(clock, d)
    svc.ingest(Observation("b1", "svc", 1450.0, False))
    svc.ingest(Observation("b2", "svc", 1451.0, False))
    assert svc.budget("svc").state is BudgetState.EXHAUSTED
    assert svc.reset_budget("svc").state is BudgetState.RESET
    svc.close()

    svc2 = build(clock, d)
    assert svc2.budget("svc").state is BudgetState.RESET
    svc2.close()


def test_duplicate_still_rejected_after_restart(tmp_path):
    d = str(tmp_path)
    clock = ManualClock(1500.0)
    svc = build(clock, d)
    svc.ingest(Observation("once", "svc", 1450.0, True))
    svc.close()
    svc2 = build(clock, d)
    r = svc2.ingest(Observation("once", "svc", 1450.0, True))
    assert r.status is IngestStatus.DUPLICATE
    assert svc2.event_count("svc") == 1
    svc2.close()


def test_torn_partial_write_is_not_counted_as_observation(tmp_path):
    d = str(tmp_path)
    wal = os.path.join(d, "wal.log")
    clock = ManualClock(1500.0)
    svc = build(clock, d)
    svc.ingest(Observation("good", "svc", 1450.0, True))
    svc.close()

    # Simulate a crash in the middle of appending a record.
    with open(wal, "ab") as fh:
        fh.write(b"ab12cd34:{\"type\":\"obs\",\"event_id\":\"partial\"")
    # No trailing newline and the checksum is wrong: must be discarded.

    svc2 = build(clock, d)
    assert svc2.event_count("svc") == 1
    # The genuine event is intact; replay after truncation stays stable.
    svc2.close()
    svc3 = build(clock, d)
    assert svc3.event_count("svc") == 1
    svc3.close()
