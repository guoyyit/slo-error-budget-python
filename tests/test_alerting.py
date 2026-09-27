"""Requirement 6: multi-window alert states and cooldown/silence window."""

from __future__ import annotations

from slo_tracker.clock import FakeClock
from slo_tracker.models import AlertConditionState, Outcome
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

from .conftest import make_config, obs


def _fire(service: SLOService, clock: FakeClock) -> None:
    # Two failures inside both short and long windows => >=2x burn.
    service.ingest(obs("f1", clock.now() - 4.0, Outcome.FAILURE))
    service.ingest(obs("f2", clock.now() - 3.0, Outcome.FAILURE))


def test_fires_when_both_windows_breach(
    service: SLOService, clock: FakeClock
) -> None:
    _fire(service, clock)
    alert = service.alert_status("svc")
    assert alert.state is AlertConditionState.FIRING
    assert alert.breached is True
    assert alert.last_fired_at == clock.now()
    assert alert.cooldown_until == clock.now() + 300.0
    assert alert.short_window.burn_rate >= 2.0
    assert alert.long_window.burn_rate >= 2.0


def test_no_alert_when_only_one_window_breaches(
    tmp_data_dir, clock: FakeClock
) -> None:
    # Dilute the long window with many old successes so only the short window
    # exceeds the threshold.
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    for i in range(200):
        svc.ingest(obs(f"old-{i}", 800.0 + i * 0.5, Outcome.SUCCESS))
    svc.ingest(obs("new-f1", 997.0, Outcome.FAILURE))
    svc.ingest(obs("new-f2", 998.0, Outcome.FAILURE))
    alert = svc.alert_status("svc")
    assert alert.short_window.burn_rate >= 2.0
    assert alert.long_window.burn_rate < 2.0
    assert alert.breached is False
    assert alert.state is AlertConditionState.IDLE


def test_persistent_breach_does_not_refire_during_cooldown(
    service: SLOService, clock: FakeClock
) -> None:
    _fire(service, clock)
    first = service.alert_status("svc")
    fired_at = first.last_fired_at

    # Keep the breach continuously true by feeding fresh failures; advance time
    # but stay inside the cooldown window.
    clock.set(1100.0)
    service.ingest(obs("f3", 1096.0, Outcome.FAILURE))
    service.ingest(obs("f4", 1097.0, Outcome.FAILURE))
    alert = service.alert_status("svc")
    assert alert.breached is True
    assert alert.state is AlertConditionState.FIRING
    # Same persistent episode: last_fired_at must not move.
    assert alert.last_fired_at == fired_at


def test_repeat_hit_after_resolve_inside_quiet_window_suppressed(
    tmp_data_dir, clock: FakeClock
) -> None:
    # A longer cooldown than the long window lets the condition genuinely
    # clear (failures age out) while the quiet window is still active.
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config(cooldown_seconds=900.0))
    _fire(svc, clock)

    # Firing failures at ~996 age out of the 300s long window by t=1300,
    # but the quiet window [1000,1900) is still running.
    clock.set(1310.0)
    assert svc.alert_status("svc").state is AlertConditionState.RESOLVED

    # Re-breach at 1310: cooldown (until 1900) still active -> suppress.
    svc.ingest(obs("g1", 1307.0, Outcome.FAILURE))
    svc.ingest(obs("g2", 1308.0, Outcome.FAILURE))
    alert = svc.alert_status("svc")
    assert alert.breached is True
    assert alert.state is AlertConditionState.SUPPRESSED_COOLDOWN
    assert alert.suppressed is True
    assert "cooldown" in alert.detail
    # No new firing episode recorded.
    assert alert.last_fired_at == 1000.0


def test_new_episode_alerts_after_cooldown_expires(
    service: SLOService, clock: FakeClock
) -> None:
    _fire(service, clock)
    clock.set(1400.0)  # condition clears
    assert service.alert_status("svc").state is AlertConditionState.RESOLVED

    # Past the 300s quiet window a genuinely new incident fires again.
    clock.set(1450.0)
    service.ingest(obs("h1", 1447.0, Outcome.FAILURE))
    service.ingest(obs("h2", 1448.0, Outcome.FAILURE))
    alert = service.alert_status("svc")
    assert alert.state is AlertConditionState.FIRING
    assert alert.suppressed is False
    assert alert.last_fired_at == 1450.0


def test_alert_state_survives_restart(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    _fire(svc, clock)

    restarted = SLOService(EventStore(tmp_data_dir), clock)
    alert = restarted.alert_status("svc")
    assert alert.state is AlertConditionState.FIRING
    assert alert.last_fired_at == 1000.0
    assert alert.cooldown_until == 1300.0
