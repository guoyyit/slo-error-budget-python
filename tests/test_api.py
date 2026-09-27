"""End-to-end HTTP tests over loopback via the FastAPI test client."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from slo_tracker.api import create_app  # noqa: E402
from slo_tracker.clock import FakeClock  # noqa: E402
from slo_tracker.service import SLOService  # noqa: E402
from slo_tracker.storage import EventStore  # noqa: E402

from .conftest import make_config  # noqa: E402


@pytest.fixture
def client(tmp_path) -> TestClient:
    clock = FakeClock(1000.0)
    service = SLOService(EventStore(tmp_path / "data"), clock)
    service.configure(make_config(budget_expected_requests=1000.0))
    app = create_app(service=service, clock=clock, store=service._store)
    return TestClient(app)


CONFIG_BODY = {
    "service": "svc",
    "availability_target": 0.90,
    "availability_window_seconds": 300.0,
    "short_window_seconds": 60.0,
    "long_window_seconds": 300.0,
    "alert_short_burn": 2.0,
    "alert_long_burn": 2.0,
    "cooldown_seconds": 300.0,
    "budget_window_seconds": 3600.0,
    "budget_expected_requests": 1000.0,
}


def test_health_and_unknown_service_404(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    resp = client.get("/services/nope")
    assert resp.status_code == 404


def test_configure_then_empty_availability_unknown(client: TestClient) -> None:
    resp = client.post("/services", json=CONFIG_BODY)
    assert resp.status_code == (201 if resp.status_code != 200 else 200)
    av = client.get("/services/svc/availability").json()
    assert av["status"] == "unknown"
    assert av["ratio"] is None
    assert av["reason"] == "no_observations_in_window"


def test_observe_and_query(client: TestClient) -> None:
    payload = {"observation_id": "o1", "timestamp": 999.0, "outcome": "success"}
    resp = client.post("/services/svc/observe", json=payload)
    assert resp.status_code == 202
    assert resp.json()["accepted"] is True

    snap = client.get("/services/svc").json()
    assert snap["availability"]["status"] == "known"
    assert snap["availability"]["total"] == 1
    assert snap["availability"]["ratio"] == 1.0


def test_duplicate_returns_409(client: TestClient) -> None:
    body = {"observation_id": "dup", "timestamp": 999.0, "outcome": "success"}
    assert client.post("/services/svc/observe", json=body).status_code == 202
    resp = client.post("/services/svc/observe", json=body)
    assert resp.status_code == 409
    assert resp.json()["reason"] == "duplicate"


def test_unknown_service_observe_404(tmp_path) -> None:
    clock = FakeClock(1000.0)
    service = SLOService(EventStore(tmp_path / "d2"), clock)
    client = TestClient(create_app(service=service, clock=clock, store=service._store))
    resp = client.post(
        "/services/ghost/observe",
        json={"observation_id": "x", "timestamp": 999.0, "outcome": "success"},
    )
    assert resp.status_code == 404
    assert resp.json()["reason"] == "unknown_service"


def test_reset_endpoint(client: TestClient) -> None:
    client.post(
        "/services/svc/observe",
        json={"observation_id": "f", "timestamp": 999.0, "outcome": "failure"},
    )
    resp = client.post("/services/svc/reset", json={"at": 5000.0})
    assert resp.status_code == 200
    assert resp.json()["period_start"] == 5000.0
    assert resp.json()["exhausted"] is False
