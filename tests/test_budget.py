"""Error-budget exhaustion, latching, explicit reset and period rollover."""
from __future__ import annotations

from slo_tracker.clock import ManualClock
from slo_tracker.config import ServiceSLO
from slo_tracker.models import BudgetState, IngestStatus, Observation
from slo_tracker.service import SloService


def make_service(clock: ManualClock) -> SloService:
    slo = ServiceSLO(
        name="svc",
        target_availability=0.9,
        long_window_seconds=600.0,
        short_window_seconds=60.0,
        budget_period_seconds=1000.0,
    )
    return SloService([slo], clock=clock)


def test_no_traffic_budget_is_within_not_exhausted():
    svc = make_service(ManualClock(1500.0))
    snap = svc.budget("svc")
    assert snap.state is BudgetState.WITHIN
    assert snap.total == 0
    assert snap.failures == 0


def test_budget_exhausts_when_failures_exceed_allowance():
    clock = ManualClock(1500.0)
    svc = make_service(clock)
    # period [1000, 2000): 8 successes + 2 failures on 10 requests;
    # allowance = 1.0, so 2 failures exhausts.
    for i in range(8):
        svc.ingest(Observation(f"ok{i}", "svc", 1100.0 + i, True))
    for i in range(2):
        svc.ingest(Observation(f"bad{i}", "svc", 1200.0 + i, False))
    snap = svc.budget("svc")
    assert snap.state is BudgetState.EXHAUSTED
    assert snap.period_index == 1
    assert snap.period_start == 1000.0
    assert snap.period_end == 2000.0
    assert snap.budget_remaining < 0


def test_exhaustion_does_not_auto_reset_inside_period():
    clock = ManualClock(1500.0)
    svc = make_service(clock)
    for i in range(8):
        svc.ingest(Observation(f"ok{i}", "svc", 1100.0 + i, True))
    for i in range(2):
        svc.ingest(Observation(f"bad{i}", "svc", 1200.0 + i, False))
    assert svc.budget("svc").state is BudgetState.EXHAUSTED
    # Many later successes dilute the ratio, but the latch must persist.
    for i in range(900):
        assert svc.ingest(Observation(f"rec{i}", "svc", 1300.0 + i * 0.0005, True)).status is IngestStatus.ACCEPTED
    assert svc.budget("svc").state is BudgetState.EXHAUSTED


def test_explicit_reset_marks_period_reset_and_persists():
    clock = ManualClock(1500.0)
    svc = make_service(clock)
    svc.ingest(Observation("b1", "svc", 1100.0, False))
    svc.ingest(Observation("b2", "svc", 1101.0, False))
    assert svc.budget("svc").state is BudgetState.EXHAUSTED
    reset = svc.reset_budget("svc")
    assert reset.state is BudgetState.RESET
    # stays RESET, never silently returns to WITHIN or EXHAUSTED
    assert svc.budget("svc").state is BudgetState.RESET


def test_new_period_recomputes_at_fixed_boundary():
    clock = ManualClock(1500.0)
    svc = make_service(clock)
    for i in range(8):
        svc.ingest(Observation(f"ok{i}", "svc", 1100.0 + i, True))
    for i in range(2):
        svc.ingest(Observation(f"bad{i}", "svc", 1200.0 + i, False))
    assert svc.budget("svc").state is BudgetState.EXHAUSTED
    # Roll into the next period [2000, 3000): no observations there yet.
    clock.set(2500.0)
    snap = svc.budget("svc")
    assert snap.period_index == 2
    assert snap.period_start == 2000.0
    assert snap.period_end == 3000.0
    assert snap.state is BudgetState.WITHIN
    assert snap.total == 0
    # Old-period failures must not leak across the boundary.
    assert snap.failures == 0
