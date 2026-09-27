"""Storage layer tests: durable round-trip and torn-tail recovery."""

from __future__ import annotations

from pathlib import Path

from slo_tracker.models import (
    AlertConditionState,
    AlertStatus,
    BudgetState,
    Observation,
    Outcome,
    SLOConfig,
    WindowBurn,
)
from slo_tracker.storage import EventStore


def make_config() -> SLOConfig:
    return SLOConfig(
        service="svc",
        availability_target=0.99,
        availability_window_seconds=300.0,
        short_window_seconds=60.0,
        long_window_seconds=300.0,
        alert_short_burn=14.4,
        alert_long_burn=14.4,
        cooldown_seconds=300.0,
        budget_window_seconds=3600.0,
        budget_expected_requests=1000.0,
    )


def make_obs(oid: str) -> Observation:
    return Observation(
        observation_id=oid,
        service="svc",
        timestamp=123.5,
        outcome=Outcome.FAILURE,
        ingest_time=124.0,
    )


def make_budget() -> BudgetState:
    return BudgetState(
        service="svc",
        period_start=0.0,
        period_end=None,
        availability_target=0.99,
        total_requests=10,
        failures=5,
        total_budget=10.0,
        consumed=5.0,
        remaining=5.0,
        burn_fraction=0.5,
        exhausted=False,
    )


def make_alert() -> AlertStatus:
    burn = WindowBurn(
        window_seconds=60.0,
        window_start=0.0,
        window_end=60.0,
        total=10,
        failures=5,
        error_rate=0.5,
        burn_rate=50.0,
    )
    return AlertStatus(
        service="svc",
        state=AlertConditionState.FIRING,
        short_window=burn,
        long_window=burn,
        last_fired_at=60.0,
        cooldown_until=360.0,
        breached=True,
        suppressed=False,
        detail="breached",
    )


def test_observation_round_trip(tmp_path: Path) -> None:
    store = EventStore(tmp_path)
    store.append_observation(make_obs("a"))
    store.append_observation(make_obs("b"))
    reloaded = EventStore(tmp_path).load_observations()
    assert [o.observation_id for o in reloaded] == ["a", "b"]
    assert reloaded[0].outcome is Outcome.FAILURE
    assert reloaded[0].timestamp == 123.5
    assert reloaded[0].ingest_time == 124.0


def test_config_round_trip(tmp_path: Path) -> None:
    store = EventStore(tmp_path)
    store.save_config(make_config())
    configs = EventStore(tmp_path).load_configs()
    assert set(configs) == {"svc"}
    assert configs["svc"].availability_target == 0.99
    assert configs["svc"].budget_expected_requests == 1000.0


def test_state_round_trip_latest_wins(tmp_path: Path) -> None:
    store = EventStore(tmp_path)
    store.append_state("svc", make_budget(), make_alert(), watermark=100.0, period_start=0.0)
    states = EventStore(tmp_path).load_states()
    assert set(states) == {"svc"}
    row = states["svc"]
    assert row["watermark"] == 100.0
    budget = store.budget_from_dict(row["budget"])
    alert = store.alert_from_dict(row["alert"])
    assert budget.failures == 5
    assert alert.state is AlertConditionState.FIRING
    assert alert.cooldown_until == 360.0
    assert alert.short_window.burn_rate == 50.0


def test_torn_tail_record_is_discarded(tmp_path: Path) -> None:
    store = EventStore(tmp_path)
    store.append_observation(make_obs("committed-1"))
    store.append_observation(make_obs("committed-2"))

    # Simulate a crash mid-write: append a partial, unframed tail.
    log = tmp_path / "observations.jsonl"
    with log.open("ab") as handle:
        handle.write(b'42.123.{"observation_id": "never-finished", "serv')

    reloaded = EventStore(tmp_path).load_observations()
    assert [o.observation_id for o in reloaded] == [
        "committed-1",
        "committed-2",
    ]


def test_corrupt_crc_tail_is_discarded(tmp_path: Path) -> None:
    store = EventStore(tmp_path)
    store.append_observation(make_obs("good"))
    log = tmp_path / "observations.jsonl"
    with log.open("ab") as handle:
        # Plausible framing but wrong body length/crc.
        handle.write(b'99.999999.{"x": 1}\n')
    reloaded = EventStore(tmp_path).load_observations()
    assert [o.observation_id for o in reloaded] == ["good"]
