"""Shared fixtures/helpers for SLO service tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from slo_tracker.clock import FakeClock
from slo_tracker.models import Observation, Outcome, SLOConfig
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore


def make_config(**overrides) -> SLOConfig:
    base = {
        "service": "svc",
        "availability_target": 0.90,
        "availability_window_seconds": 300.0,
        "short_window_seconds": 60.0,
        "long_window_seconds": 300.0,
        "alert_short_burn": 2.0,
        "alert_long_burn": 2.0,
        "cooldown_seconds": 300.0,
        "budget_window_seconds": 3600.0,
        "budget_expected_requests": None,
    }
    base.update(overrides)
    return SLOConfig(**base)


def obs(oid: str, ts: float, outcome: Outcome, service: str = "svc") -> Observation:
    return Observation(
        observation_id=oid, service=service, timestamp=ts, outcome=outcome
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock(1000.0)


@pytest.fixture
def service(tmp_path: Path, clock: FakeClock) -> SLOService:
    svc = SLOService(EventStore(tmp_path / "data"), clock)
    svc.configure(make_config())
    return svc


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return tmp_path / "data"
