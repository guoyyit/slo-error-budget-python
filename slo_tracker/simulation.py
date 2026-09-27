"""Local in-memory pipe simulation.

A small generator acts as the "observation pipe": events are pushed into a
queue and a worker ingests them into :class:`SloService` under a fully
deterministic :class:`ManualClock`. Nothing touches the network or external
services, so the whole scenario is reproducible locally.
"""
from __future__ import annotations

import random
from collections import deque

from .clock import ManualClock
from .config import BurnRateRule, ServiceSLO
from .models import Observation
from .service import SloService

MINUTE = 60.0
HOUR = 3600.0


def build_service(clock: ManualClock, *, persist: bool = False) -> SloService:
    slo = ServiceSLO(
        name="checkout",
        target_availability=0.99,
        long_window_seconds=HOUR,
        short_window_seconds=5 * MINUTE,
        budget_period_seconds=24 * HOUR,
        max_future_skew_seconds=10.0,
        rules=(
            BurnRateRule(
                name="page-fast",
                long_window_seconds=HOUR,
                short_window_seconds=5 * MINUTE,
                long_burn_rate_threshold=10.0,
                short_burn_rate_threshold=10.0,
                cooldown_seconds=30 * MINUTE,
            ),
        ),
    )
    store = None
    if persist:
        import tempfile

        from .config import StoreConfig

        store = StoreConfig(tempfile.mkdtemp(prefix="slo-sim-"))
    return SloService([slo], clock=clock, store=store)


def run_demo(seed: int = 0) -> SloService:
    """Emit healthy traffic, then an incident, then recovery; print states."""
    rng = random.Random(seed)
    clock = ManualClock(24 * HOUR)  # inside budget period index 1
    svc = build_service(clock)
    pipe: deque[Observation] = deque()
    seq = 0

    def emit(duration: float, rate_per_s: float, failure_rate: float) -> None:
        nonlocal seq
        end = clock.now() + duration
        next_at = clock.now()
        while next_at < end:
            success = rng.random() >= failure_rate
            pipe.append(
                Observation(
                    event_id=f"evt-{seq}",
                    service="checkout",
                    timestamp=next_at,
                    success=success,
                )
            )
            seq += 1
            next_at += 1.0 / rate_per_s
        clock.set(end)
        _drain(pipe, svc)

    def _drain(queue: deque[Observation], service: SloService) -> None:
        while queue:
            service.ingest(queue.popleft())

    print("== healthy 10 minutes ==")
    emit(10 * MINUTE, 4.0, 0.0)
    _report(svc, clock.now())

    print("== incident: 50% failures for 5 minutes ==")
    emit(5 * MINUTE, 4.0, 0.5)
    _report(svc, clock.now())

    print("== incident sustained another 5 minutes (cooldown silencing) ==")
    emit(5 * MINUTE, 4.0, 0.5)
    _report(svc, clock.now())

    print("== recovery: healthy for 10 minutes ==")
    emit(10 * MINUTE, 4.0, 0.0)
    _report(svc, clock.now())
    return svc


def _report(svc: SloService, now: float) -> None:
    br = svc.burn_rate("checkout", now=now)
    budget = svc.budget("checkout", now=now)
    alerts = svc.alerts("checkout", now=now)
    short = br.short_window
    long = br.long_window
    print(
        f"t={now:g} short(total={short.total}, burn={_fmt(short.burn_rate)}) "
        f"long(total={long.total}, burn={_fmt(long.burn_rate)}) "
        f"budget={budget.state.value}(failures={budget.failures}, "
        f"allowed={budget.allowed_failures:.2f}) "
        f"alerts={[(a.state.value, a.last_fired_at) for a in alerts]}"
    )


def _fmt(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.2f}"


if __name__ == "__main__":
    run_demo()
