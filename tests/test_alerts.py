"""Multi-window alerting: dual-window condition, cooldown freeze, resolution."""
from __future__ import annotations

from slo_tracker.alerts import condition_met, evaluate_rule, initial_snapshot
from slo_tracker.config import BurnRateRule
from slo_tracker.models import AlertState

RULE = BurnRateRule(
    name="page",
    long_window_seconds=600.0,
    short_window_seconds=60.0,
    long_burn_rate_threshold=10.0,
    short_burn_rate_threshold=10.0,
    cooldown_seconds=300.0,
)


def test_requires_both_windows_to_breach():
    assert not condition_met(RULE, 20.0, 1.0)
    assert not condition_met(RULE, 1.0, 20.0)
    assert not condition_met(RULE, None, 20.0)
    assert condition_met(RULE, 20.0, 20.0)
    assert condition_met(RULE, 10.0, 10.0)  # threshold is inclusive


def test_fire_suppressed_then_resolve_only_after_cooldown():
    s = initial_snapshot(RULE)
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1000.0)
    assert s.state is AlertState.FIRING
    assert s.last_fired_at == 1000.0
    assert s.cooldown_until == 1300.0
    # Sustained breach inside cooldown: suppressed, same last_fired_at.
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1100.0)
    assert s.state is AlertState.SUPPRESSED
    assert s.firing is True
    assert s.last_fired_at == 1000.0
    assert "silenced" in s.reason
    # Condition clearing DURING cooldown (e.g. late success events) must not
    # change the alerted result: still frozen as suppressed.
    s = evaluate_rule(RULE, 0.5, 0.5, s, 1200.0)
    assert s.state is AlertState.SUPPRESSED
    assert s.firing is False
    assert s.last_fired_at == 1000.0
    assert "frozen" in s.reason
    # Only after the cooldown can the alert resolve.
    s = evaluate_rule(RULE, 0.5, 0.5, s, 1300.0)
    assert s.state is AlertState.RESOLVED
    assert s.firing is False


def test_flapping_inside_one_cooldown_emits_single_notification():
    # Fire, condition clears and returns multiple times within cooldown:
    # the notification record must remain the single original one.
    s = initial_snapshot(RULE)
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1000.0)
    s = evaluate_rule(RULE, 0.5, 0.5, s, 1100.0)
    assert s.state is AlertState.SUPPRESSED
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1150.0)
    assert s.state is AlertState.SUPPRESSED
    s = evaluate_rule(RULE, 0.5, 0.5, s, 1200.0)
    assert s.state is AlertState.SUPPRESSED
    assert s.last_fired_at == 1000.0


def test_fresh_rebreach_after_cooldown_fires_new_notification():
    s = initial_snapshot(RULE)
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1000.0)
    s = evaluate_rule(RULE, 0.5, 0.5, s, 1300.0)
    assert s.state is AlertState.RESOLVED
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1301.0)
    assert s.state is AlertState.FIRING
    assert s.last_fired_at == 1301.0


def test_no_traffic_never_fires():
    s = initial_snapshot(RULE)
    s = evaluate_rule(RULE, None, None, s, 1000.0)
    assert s.state is AlertState.INACTIVE
    assert s.firing is False


def test_sustained_condition_notifications_spaced_by_cooldown():
    s = initial_snapshot(RULE)
    fires = []
    for t in range(1000, 2600, 50):
        s = evaluate_rule(RULE, 20.0, 20.0, s, float(t))
        if s.state is AlertState.FIRING:
            fires.append(s.last_fired_at)
    # Fires at t=1000, then again only after cooldown at 1300+.
    assert fires == [1000.0, 1300.0, 1600.0, 1900.0, 2200.0, 2500.0]


def test_late_success_events_during_cooldown_do_not_alter_fired_record():
    s = initial_snapshot(RULE)
    s = evaluate_rule(RULE, 20.0, 20.0, s, 1000.0)
    # Late data makes the current condition false at several evaluations.
    for t in (1050.0, 1100.0, 1250.0):
        s = evaluate_rule(RULE, 0.0, 0.0, s, t)
        assert s.state is AlertState.SUPPRESSED
        assert s.last_fired_at == 1000.0
        assert s.cooldown_until == 1300.0
