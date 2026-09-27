"""End-to-end alert integration via the service plus loopback HTTP API."""
from __future__ import annotations

import pytest

from slo_tracker.api import create_app
from slo_tracker.clock import ManualClock
from slo_tracker.config import BurnRateRule, ServiceSLO
from slo_tracker.models import AlertState, IngestStatus, Observation
from slo_tracker.service import SloService

RULE = BurnRateRule("page", 600.0, 60.0, 5.0, 5.0, cooldown_seconds=300.0)


def make_service(clock: ManualClock) -> SloService:
    slo = ServiceSLO(
        name="svc",
        target_availability=0.9,
        long_window_seconds=600.0,
        short_window_seconds=60.0,
        budget_period_seconds=10000.0,
        max_future_skew_seconds=10.0,
        rules=(RULE,),
    )
    return SloService([slo], clock=clock)


def failures(svc: SloService, n: int, t0: float, prefix: str) -> None:
    # 100% failures burn hard: burn rate = 1/0.1 = 10 >= 5 in both windows.
    for i in range(n):
        r = svc.ingest(Observation(f"{prefix}-{i}", "svc", t0 + i * 0.1, False))
        assert r.status is IngestStatus.ACCEPTED


def test_alert_fires_then_late_events_preserve_fired_record():
    clock = ManualClock(1000.0)
    svc = make_service(clock)
    failures(svc, 10, 995.0, "f")
    a = svc.alerts("svc")[0]
    assert a.state is AlertState.FIRING
    assert a.last_fired_at == 1000.0

    # Late-arriving failure events with timestamps inside the current window
    # (they land within retention but are delivered after the alert fired)
    # must not produce a new alert notification during cooldown.
    clock.set(1100.0)
    failures(svc, 5, 1050.0, "late-fail")
    a = svc.alerts("svc")[0]
    assert a.state is AlertState.SUPPRESSED
    assert a.last_fired_at == 1000.0
    assert a.cooldown_until == 1300.0

    # Late-arriving success events that make the current condition false also
    # cannot change or revoke the already alerted result inside cooldown.
    for i in range(200):
        assert svc.ingest(Observation(f"late-ok-{i}", "svc", 1055.0 + i * 0.2, True)).status is IngestStatus.ACCEPTED
    a = svc.alerts("svc")[0]
    assert a.state is AlertState.SUPPRESSED
    assert a.last_fired_at == 1000.0
    assert a.cooldown_until == 1300.0

    # Only after the cooldown boundary can the alert resolve.
    clock.set(1301.0)
    a = svc.alerts("svc")[0]
    assert a.state is AlertState.RESOLVED


def test_shared_data_source_for_long_and_short_windows():
    clock = ManualClock(1000.0)
    svc = make_service(clock)
    failures(svc, 10, 995.0, "f")
    br = svc.burn_rate("svc")
    # Every event counted by the short window must also be inside the long
    # window because both are derived from the identical committed log.
    assert br.long_window.total >= br.short_window.total
    assert br.short_window.failures <= br.long_window.failures


def test_long_short_windows_share_committed_set_after_rejects():
    clock = ManualClock(1000.0)
    svc = make_service(clock)
    failures(svc, 6, 995.0, "f")
    # rejected inputs never enter either window
    assert svc.ingest(Observation("dup", "svc", 995.0, False)).status is IngestStatus.ACCEPTED
    assert svc.ingest(Observation("dup", "svc", 995.0, False)).status is IngestStatus.DUPLICATE
    # budget period start is 0 (10000s period, now 1000): event older than
    # the period start is beyond retention.
    assert svc.ingest(Observation("ancient", "svc", -9500.0, False)).status is IngestStatus.LATE_OUTSIDE_RETENTION
    br = svc.burn_rate("svc")
    assert br.short_window.failures == br.long_window.failures == 7


@pytest.fixture()
def client():
    from fastapi.testclient import TestClient

    clock = ManualClock(1000.0)
    svc = make_service(clock)
    return TestClient(create_app(svc))


def test_api_unknown_window_then_ingest_and_query(client):
    r = client.get("/services/svc/burn-rate")
    assert r.status_code == 200
    body = r.json()
    assert body["short_window"]["state"] == "unknown"

    r = client.post(
        "/observations",
        json={"event_id": "o1", "service": "svc", "timestamp": 999.0, "success": False},
    )
    assert r.status_code == 202 and r.json()["accepted"] is True
    r = client.get("/services/svc/burn-rate")
    assert r.json()["short_window"]["state"] == "ok"
    assert r.json()["short_window"]["failures"] == 1


def test_api_rejection_explained_and_unknown_service_404(client):
    r = client.post(
        "/observations",
        json={"event_id": "o1", "service": "svc", "timestamp": 999.0, "success": True},
    )
    assert r.status_code == 202
    r = client.post(
        "/observations",
        json={"event_id": "o1", "service": "svc", "timestamp": 999.0, "success": True},
    )
    body = r.json()
    assert body["status"] == "duplicate" and body["accepted"] is False
    assert client.get("/services/ghost/budget").status_code == 404


def test_api_budget_reset_endpoint(client):
    for i in range(3):
        client.post(
            "/observations",
            json={"event_id": f"b{i}", "service": "svc", "timestamp": 990.0 + i, "success": False},
        )
    r = client.get("/services/svc/budget")
    assert r.json()["state"] == "exhausted"
    r = client.post("/services/svc/budget/reset")
    assert r.json()["state"] == "reset"
    r = client.get("/services/svc/alerts")
    assert "alerts" in r.json()
