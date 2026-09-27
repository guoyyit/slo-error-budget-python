"""Requirement 3: error-budget exhaustion, latch and explicit reset."""

from __future__ import annotations

from slo_tracker.clock import FakeClock
from slo_tracker.models import Outcome
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

from .conftest import make_config, obs


def test_budget_exhausts_and_then_latches_even_if_errors_stop(
    service: SLOService, clock: FakeClock
) -> None:
    # Expected requests = 1000 over the window, target 90% => budget 100 errors.
    service.configure(make_config(budget_expected_requests=1000.0))
    clock.set(2000.0)

    for i in range(100):
        accepted, reason, _ = service.ingest(obs(f"f{i}", 1050.0 + i * 0.01, Outcome.FAILURE))
        assert accepted, reason

    snap = service.query("svc")
    assert snap.budget.exhausted is True
    assert snap.budget.consumed == 100.0

    # Many successes afterwards must NOT auto-clear the exhausted state.
    for i in range(500):
        service.ingest(obs(f"s{i}", 1070.0 + i * 0.01, Outcome.SUCCESS))
    snap2 = service.query("svc")
    assert snap2.budget.exhausted is True


def test_explicit_reset_starts_fresh_budget_at_deterministic_boundary(
    service: SLOService, clock: FakeClock
) -> None:
    service.configure(make_config(budget_expected_requests=1000.0))
    clock.set(2000.0)
    for i in range(100):
        service.ingest(obs(f"f{i}", 1050.0 + i * 0.01, Outcome.FAILURE))
    assert service.query("svc").budget.exhausted is True

    clock.set(5000.0)
    budget = service.reset_budget("svc", at=5000.0)
    assert budget.period_start == 5000.0
    assert budget.exhausted is False
    assert budget.total_requests == 0

    # The new period counts only events at/after the boundary.
    clock.set(5002.0)
    service.ingest(obs("new-f", 5001.0, Outcome.FAILURE))
    snap = service.query("svc")
    assert snap.budget.period_start == 5000.0
    assert snap.budget.failures == 1
    # Old failures (before boundary) do not count against the new budget.
    assert snap.budget.exhausted is False


def test_reset_without_explicit_time_uses_now(service: SLOService, clock: FakeClock) -> None:
    clock.set(7777.0)
    budget = service.reset_budget("svc")
    assert budget.period_start == 7777.0


def test_budget_rolls_by_configured_window_when_not_exhausted(
    service: SLOService, clock: FakeClock
) -> None:
    # First period anchored at configure time (clock=1000), window 3600s.
    service.ingest(obs("old-s", 1500.0, Outcome.SUCCESS))
    assert service.query("svc").budget.period_start == 1000.0

    # Advance beyond the fixed boundary at 4600; ingest/query rolls period.
    clock.set(4650.0)
    service.ingest(obs("new-s", 4620.0, Outcome.SUCCESS))
    clock.set(4700.0)
    snap = service.query("svc")
    assert snap.budget.period_start == 4600.0
    # The 1500 event belongs to the previous period, not the new one.
    assert snap.budget.total_requests == 1
    assert snap.budget.failures == 0


def test_exhausted_budget_does_not_roll_automatically(
    service: SLOService, clock: FakeClock
) -> None:
    service.configure(make_config(budget_expected_requests=1000.0))
    clock.set(2000.0)
    for i in range(100):
        service.ingest(obs(f"f{i}", 1500.0 + i * 0.01, Outcome.FAILURE))
    assert service.query("svc").budget.exhausted is True
    anchor = service.query("svc").budget.period_start

    # Cross many budget windows; the exhausted period still must not roll.
    clock.set(50000.0)
    snap = service.query("svc")
    assert snap.budget.exhausted is True
    assert snap.budget.period_start == anchor


def test_budget_state_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config(budget_expected_requests=1000.0))
    clock.set(2000.0)
    for i in range(100):
        svc.ingest(obs(f"f{i}", 1050.0 + i * 0.01, Outcome.FAILURE))

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    snap = restarted.query("svc")
    assert snap.budget.exhausted is True
    assert snap.budget.failures == 100

    # Explicit reset after restart anchors a new period durably.
    clock.set(6000.0)
    restarted.reset_budget("svc")
    again = SLOService(EventStore(tmp_data_dir), clock)
    assert again.query("svc").budget.exhausted is False
    assert again.query("svc").budget.period_start == 6000.0
