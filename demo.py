"""Locally reproducible end-to-end demo.

Runs entirely in-process with an in-memory loopback pipeline, a fake clock
and an on-disk (temp directory) store. No external services or credentials.

Run with::

    uv run python demo.py
"""

from __future__ import annotations

import tempfile

from slo_tracker.clock import FakeClock
from slo_tracker.models import Observation, Outcome, SLOConfig
from slo_tracker.pipeline import InMemoryPipeline
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

SERVICE = "checkout-api"


def banner(title: str) -> None:
    print(f"\n=== {title} ===")


def o(oid: str, ts: float, outcome: Outcome) -> Observation:
    return Observation(oid, SERVICE, ts, outcome)


def main() -> None:
    data_dir = tempfile.mkdtemp(prefix="slo-demo-")
    clock = FakeClock(1000.0)
    service = SLOService(EventStore(data_dir), clock)
    pipe = InMemoryPipeline(service.ingest)

    config = SLOConfig(
        service=SERVICE,
        availability_target=0.90,          # 10% allowed error rate
        availability_window_seconds=300.0,
        short_window_seconds=60.0,
        long_window_seconds=300.0,
        alert_short_burn=2.0,              # 2x allowance in both windows
        alert_long_burn=2.0,
        cooldown_seconds=900.0,
        budget_window_seconds=3600.0,
        budget_expected_requests=100.0,    # 10 allowed failures
    )
    service.configure(config)
    print(f"data directory: {data_dir}")

    banner("1. Empty window is UNKNOWN, never 0% or 100%")
    snap = service.query(SERVICE)
    print("availability:", snap.availability.status.value,
          "| ratio:", snap.availability.ratio)

    banner("2. Healthy traffic through the in-memory loopback pipe")
    clock.set(1030.0)
    pipe.run_demo([o(f"ok-{i}", 1000.0 + i, Outcome.SUCCESS) for i in range(20)])
    snap = service.query(SERVICE)
    print(f"availability: {snap.availability.ratio:.0%} "
          f"({snap.availability.successes}/{snap.availability.total})")

    banner("3. Failures raise burn rate over threshold -> alert fires")
    clock.set(1050.0)
    pipe.run_demo([o(f"boom-{i}", 1020.0 + i, Outcome.FAILURE) for i in range(5)])
    snap = service.query(SERVICE)
    print(f"short burn: {snap.short_burn.burn_rate:.1f}x | "
          f"long burn: {snap.long_burn.burn_rate:.1f}x")
    print("alert:", snap.alert.state.value, "| last fired at:",
          snap.alert.last_fired_at)
    print("detail:", snap.alert.detail)

    banner("4. Duplicate replay vs. late event have distinct reject reasons")
    _ok, dup, _ = service.ingest(o("ok-0", 1000.0, Outcome.SUCCESS))
    print("replay ok-0 ->", dup.value)
    _ok, late, detail = service.ingest(o("very-old", 100.0, Outcome.SUCCESS))
    print("event before watermark ->", late.value, "|", detail)

    banner("5. Error budget exhausts and latches until explicit reset")
    clock.set(1070.0)
    # 5 failures already counted; add 5 more to reach the budget of 10.
    pipe.run_demo([o(f"f-{i}", 1040.0 + i, Outcome.FAILURE) for i in range(5)])
    snap = service.query(SERVICE)
    print(f"consumed {snap.budget.consumed:.0f}/{snap.budget.total_budget:.0f} "
          f"| exhausted: {snap.budget.exhausted}")
    clock.set(1090.0)
    service.ingest(o("recover-ok", 1050.0, Outcome.SUCCESS))
    print("success after exhaustion -> still exhausted:",
          service.query(SERVICE).budget.exhausted)

    clock.set(5000.0)
    service.reset_budget(SERVICE)
    print("explicit reset -> period start",
          service.query(SERVICE).budget.period_start,
          "| exhausted:", service.query(SERVICE).budget.exhausted)

    banner("6. Restart recovery (same data directory)")
    restarted = SLOService(EventStore(data_dir), clock)
    snap = restarted.query(SERVICE)
    print("observations recovered:", len(snap.observations))
    print("budget period start:", snap.budget.period_start,
          "| exhausted:", snap.budget.exhausted)
    print("alert last fired at:", snap.alert.last_fired_at)

    print("\nDemo finished.")


if __name__ == "__main__":
    main()
