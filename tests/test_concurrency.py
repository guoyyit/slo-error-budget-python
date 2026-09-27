"""Concurrent ingestion and querying must never expose partial writes."""
from __future__ import annotations

import threading

from slo_tracker.clock import ManualClock
from slo_tracker.config import ServiceSLO
from slo_tracker.models import (
    AvailabilityState,
    BudgetSnapshot,
    IngestStatus,
    Observation,
)
from slo_tracker.service import SloService

N_WRITERS = 8
PER_WRITER = 200
N_READERS = 4
READS = 500


def make_service() -> SloService:
    # In-memory only (no store) keeps this test fast and focused on locking.
    slo = ServiceSLO(
        name="svc",
        target_availability=0.5,
        long_window_seconds=100000.0,
        short_window_seconds=100000.0,
        budget_period_seconds=1000000.0,
        max_future_skew_seconds=100000.0,
    )
    return SloService([slo], clock=ManualClock(1000.0))


def test_concurrent_ingest_counts_each_event_once():
    svc = make_service()
    barrier = threading.Barrier(N_WRITERS)

    def writer(wid: int) -> None:
        barrier.wait()
        for i in range(PER_WRITER):
            svc.ingest(
                Observation(
                    event_id=f"w{wid}-e{i}",
                    service="svc",
                    timestamp=1000.0 + i * 0.001,
                    success=(i % 3 != 0),
                )
            )

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(N_WRITERS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert svc.event_count("svc") == N_WRITERS * PER_WRITER


def test_concurrent_queries_always_self_consistent():
    svc = make_service()
    errors: list[str] = []
    stop = threading.Event()

    def writer(wid: int) -> None:
        for i in range(PER_WRITER):
            r = svc.ingest(
                Observation(
                    event_id=f"w{wid}-e{i}",
                    service="svc",
                    timestamp=1000.0 + i * 0.01,
                    success=(i % 2 == 0),
                )
            )
            assert r.status in (IngestStatus.ACCEPTED, IngestStatus.DUPLICATE)

    def reader() -> None:
        for _ in range(READS):
            br = svc.burn_rate("svc", now=500000.0)
            snap: BudgetSnapshot = svc.budget("svc", now=500000.0)
            # Internal consistency of one committed snapshot:
            # failures <= total for every window and for the budget.
            for m in (br.long_window, br.short_window):
                if m.state is AvailabilityState.OK:
                    if not (0 <= m.failures <= m.total):
                        errors.append(f"window inconsistent: {m}")
            if not (0 <= snap.failures <= snap.total):
                errors.append(f"budget inconsistent: {snap}")
            # Long and short windows derive from the same event set, so the
            # smaller window can never contain more events than the larger.
            if br.short_window.total > br.long_window.total:
                errors.append("short window exceeds long window")

    ws = [threading.Thread(target=writer, args=(w,)) for w in range(N_WRITERS)]
    rs = [threading.Thread(target=reader) for _ in range(N_READERS)]
    for t in ws + rs:
        t.start()
    for t in ws + rs:
        t.join()
    stop.set()
    assert not errors, errors
    assert svc.event_count("svc") == N_WRITERS * PER_WRITER


def test_duplicates_under_race_count_once():
    svc = make_service()
    payload = [Observation(f"shared-{i}", "svc", 1000.0 + i, True) for i in range(100)]

    def worker() -> None:
        for o in payload:
            svc.ingest(o)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert svc.event_count("svc") == 100
