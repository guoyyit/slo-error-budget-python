"""Requirement 5: concurrent reporting and querying self-consistency."""

from __future__ import annotations

import threading

from slo_tracker.clock import FakeClock
from slo_tracker.models import Outcome
from slo_tracker.service import SLOService
from slo_tracker.storage import EventStore

from .conftest import make_config, obs


class ThreadSafeFakeClock(FakeClock):
    """FakeClock guarded by a lock for use from multiple threads."""

    def __init__(self, start: float = 0.0) -> None:
        super().__init__(start)
        self._tlock = threading.Lock()

    def now(self) -> float:
        with self._tlock:
            return super().now()


def test_concurrent_ingests_are_all_committed_once(
    tmp_data_dir, clock: FakeClock
) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    # Many successes keep the derived budget healthy; failures spread thin.
    svc.configure(make_config())

    n_threads = 8
    per_thread = 100
    errors: list[Exception] = []

    def worker(tid: int) -> None:
        try:
            for i in range(per_thread):
                idx = tid * per_thread + i
                outcome = Outcome.FAILURE if idx % 50 == 0 else Outcome.SUCCESS
                svc.ingest(obs(f"t{tid}-{i}", 1000.0 + (idx % 250) * 0.5, outcome))
        except Exception as exc:  # noqa: BLE001 - surface any worker failure
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    snap = svc.query("svc")
    expected = n_threads * per_thread
    # Every distinct observation committed exactly once.
    assert len(snap.observations) == expected
    assert len({o.observation_id for o in snap.observations}) == expected


def test_duplicate_ids_under_race_counted_once(tmp_data_dir, clock: FakeClock) -> None:
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())

    accepted_count = 0
    rejected_duplicates = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal accepted_count, rejected_duplicates
        ok, reason, _ = svc.ingest(obs("same-id", 1000.5, Outcome.SUCCESS))
        with lock:
            if ok:
                accepted_count += 1
            elif reason is not None and reason.value == "duplicate":
                rejected_duplicates += 1

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert accepted_count == 1
    assert rejected_duplicates == 19
    assert len(svc.query("svc").observations) == 1


def test_queries_are_self_consistent_during_concurrent_writes(
    tmp_data_dir,
) -> None:
    clock = ThreadSafeFakeClock(1000.0)
    svc = SLOService(EventStore(tmp_data_dir), clock)
    svc.configure(make_config())
    stop = threading.Event()
    inconsistencies: list[str] = []

    def writer() -> None:
        idx = 0
        while not stop.is_set():
            svc.ingest(
                obs(
                    f"w-{idx}",
                    1000.0 + (idx % 200) * 0.5,
                    Outcome.SUCCESS if idx % 3 else Outcome.FAILURE,
                )
            )
            idx += 1

    def reader() -> None:
        while not stop.is_set():
            snap = svc.query("svc")
            # Invariants that must hold for every committed snapshot:
            # 1. availability counts match the returned observation window;
            # 2. no partial counts (successes + failures == total);
            # 3. burn windows are derived from the same committed set.
            av = snap.availability
            if av.status.value == "known" and av.successes + av.failures != av.total:
                inconsistencies.append("availability parts do not sum to total")
            if av.total > len(snap.observations):
                inconsistencies.append("availability total exceeds stored events")
            ids = [o.observation_id for o in snap.observations]
            if len(ids) != len(set(ids)):
                inconsistencies.append("duplicate observation ids in snapshot")

    writers = [threading.Thread(target=writer) for _ in range(4)]
    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in writers + readers:
        t.start()

    clock.advance(0.0)  # keep clock stable during the run
    threading.Event().wait(0.5)
    stop.set()
    for t in writers + readers:
        t.join()

    assert inconsistencies == []
