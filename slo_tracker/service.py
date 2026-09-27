"""Thread-safe facade: observation ingestion and SLO state queries.

Consistency model
-----------------
One re-entrant lock guards every read and write. Within it a query:
ingest -> WAL fsync -> publish to the shared in-memory log, and query ->
derive long/short windows, budget and alerts from that same log. Hence a
query always observes a state derived from one fully committed set of
observations; partial writes are never visible.

Durability
----------
Every committed observation, budget latch/reset and alert transition is
fsynced to the local write-ahead log before being acknowledged. On
construction the WAL is replayed, so observations, exhausted budgets and
alert states survive a process restart.
"""
from __future__ import annotations

import threading
from dataclasses import asdict
from typing import Any

from . import alerts as alerts_mod
from . import budget as budget_mod
from . import windows as windows_mod
from .clock import Clock, SystemClock
from .config import ServiceSLO, StoreConfig
from .eventlog import EventLog
from .models import (
    AlertSnapshot,
    BudgetSnapshot,
    BudgetState,
    BurnRateResult,
    IngestResult,
    IngestStatus,
    Observation,
)
from .persistence import WalWriter, read_wal


class _ServiceRuntime:
    """Mutable per-service state, always accessed under the service lock."""

    __slots__ = ("slo", "log", "exhausted_periods", "reset_periods", "alert_states", "writer")

    def __init__(self, slo: ServiceSLO, writer: WalWriter | None) -> None:
        self.slo = slo
        self.log = EventLog()
        self.writer = writer
        # period_index -> latched / explicitly reset marker
        self.exhausted_periods: set[int] = set()
        self.reset_periods: set[int] = set()
        self.alert_states: dict[str, AlertSnapshot] = {
            rule.name: alerts_mod.initial_snapshot(rule) for rule in slo.rules
        }


class SloService:
    def __init__(
        self,
        slos: list[ServiceSLO],
        clock: Clock | None = None,
        store: StoreConfig | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._store = store
        self._lock = threading.RLock()
        self._writer: WalWriter | None = None
        if store is not None:
            import os

            self._writer = WalWriter(os.path.join(store.data_dir, "wal.log"), fsync=store.fsync)
        self._runtimes: dict[str, _ServiceRuntime] = {}
        for slo in slos:
            self._runtimes[slo.name] = _ServiceRuntime(slo, self._writer)
        if self._writer is not None:
            self._replay()

    # ------------------------------------------------------------------ recovery

    def _replay(self) -> None:
        assert self._writer is not None
        for rec in read_wal(self._writer.path):
            rtype = rec.get("type")
            if rtype == "obs":
                obs = Observation(
                    event_id=rec["event_id"],
                    service=rec["service"],
                    timestamp=float(rec["timestamp"]),
                    success=bool(rec["success"]),
                )
                rt = self._runtimes.get(obs.service)
                if rt is not None:
                    rt.log.add(obs)
            elif rtype == "budget":
                rt = self._runtimes.get(rec.get("service"))
                if rt is None:
                    continue
                idx = int(rec["period_index"])
                if rec.get("action") == "exhausted":
                    rt.exhausted_periods.add(idx)
                elif rec.get("action") == "reset":
                    rt.exhausted_periods.discard(idx)
                    rt.reset_periods.add(idx)
            elif rtype == "alert":
                rt = self._runtimes.get(rec.get("service"))
                if rt is None:
                    continue
                snap = AlertSnapshot(
                    rule_name=rec["rule_name"],
                    state=alerts_mod.AlertState(rec["state"]),
                    firing=bool(rec["firing"]),
                    long_window_seconds=float(rec["long_window_seconds"]),
                    short_window_seconds=float(rec["short_window_seconds"]),
                    long_burn_rate=rec.get("long_burn_rate"),
                    short_burn_rate=rec.get("short_burn_rate"),
                    last_fired_at=rec.get("last_fired_at"),
                    cooldown_until=rec.get("cooldown_until"),
                    reason=rec.get("reason", ""),
                )
                known = {rule.name for rule in rt.slo.rules}
                if snap.rule_name in known:
                    rt.alert_states[snap.rule_name] = snap

    def _append(self, record: dict[str, Any]) -> None:
        if self._writer is not None:
            self._writer.append(record)

    # ------------------------------------------------------------------ ingest

    def ingest(self, observation: Observation) -> IngestResult:
        with self._lock:
            rt = self._runtimes.get(observation.service)
            if rt is None:
                return IngestResult(
                    IngestStatus.UNKNOWN_SERVICE,
                    observation.event_id,
                    f"service '{observation.service}' is not configured",
                )
            if observation.event_id in rt.log:
                return IngestResult(
                    IngestStatus.DUPLICATE,
                    observation.event_id,
                    "event_id already committed; replay is not counted twice",
                )
            now = self._clock.now()
            skew = observation.timestamp - now
            if skew > rt.slo.max_future_skew_seconds:
                return IngestResult(
                    IngestStatus.FUTURE_EVENT,
                    observation.event_id,
                    f"timestamp {observation.timestamp:g} is {skew:g}s ahead of "
                    f"clock {now:g} (tolerance {rt.slo.max_future_skew_seconds:g}s)",
                )
            age = now - observation.timestamp
            if age > rt.slo.retention_seconds:
                return IngestResult(
                    IngestStatus.LATE_OUTSIDE_RETENTION,
                    observation.event_id,
                    f"event is {age:g}s old, beyond retention "
                    f"{rt.slo.retention_seconds:g}s; it could no longer affect any "
                    f"window or budget period",
                )

            # Durable commit first: only fsynced WAL records are acknowledged.
            self._append(
                {
                    "type": "obs",
                    "event_id": observation.event_id,
                    "service": observation.service,
                    "timestamp": observation.timestamp,
                    "success": observation.success,
                }
            )
            # publish after the durable point; add() cannot fail for a new id
            rt.log.add(observation)
            return IngestResult(IngestStatus.ACCEPTED, observation.event_id, "committed")

    # ------------------------------------------------------------------ queries

    def _runtime(self, service: str) -> _ServiceRuntime:
        rt = self._runtimes.get(service)
        if rt is None:
            raise KeyError(f"unknown service '{service}'")
        return rt

    def burn_rate(self, service: str, *, now: float | None = None) -> BurnRateResult:
        with self._lock:
            rt = self._runtime(service)
            return windows_mod.burn_rate(rt.log, rt.slo, self._at(now))

    @staticmethod
    def _replace_budget_state(snap: BudgetSnapshot, state: BudgetState) -> BudgetSnapshot:
        return BudgetSnapshot(
            period_index=snap.period_index,
            period_start=snap.period_start,
            period_end=snap.period_end,
            state=state,
            total=snap.total,
            failures=snap.failures,
            allowed_failures=snap.allowed_failures,
            budget_spent=snap.budget_spent,
            budget_remaining=snap.budget_remaining,
        )

    def budget(self, service: str, *, now: float | None = None) -> BudgetSnapshot:
        with self._lock:
            rt = self._runtime(service)
            at = self._at(now)
            raw = budget_mod.compute_budget(rt.log, rt.slo, at)
            idx = raw.period_index
            if idx in rt.reset_periods:
                return self._replace_budget_state(raw, BudgetState.RESET)
            if raw.state is BudgetState.EXHAUSTED and idx not in rt.exhausted_periods:
                # First detection of exhaustion: latch it durably. Once
                # latched it never clears automatically inside the period.
                rt.exhausted_periods.add(idx)
                self._append(
                    {
                        "type": "budget",
                        "service": service,
                        "action": "exhausted",
                        "period_index": idx,
                        "at": at,
                    }
                )
            if idx in rt.exhausted_periods:
                return self._replace_budget_state(raw, BudgetState.EXHAUSTED)
            return raw

    def reset_budget(self, service: str, *, now: float | None = None) -> BudgetSnapshot:
        """Explicitly reset the current period's latched exhaustion.

        The reset is scoped to the current budget period and persisted. A
        later period recomputes from its own observations at the fixed
        period boundary.
        """
        with self._lock:
            rt = self._runtime(service)
            at = self._at(now)
            idx = budget_mod.period_index_for(rt.slo, at)
            if idx not in rt.reset_periods:
                rt.exhausted_periods.discard(idx)
                rt.reset_periods.add(idx)
                self._append(
                    {
                        "type": "budget",
                        "service": service,
                        "action": "reset",
                        "period_index": idx,
                        "at": at,
                    }
                )
            raw = budget_mod.compute_budget(rt.log, rt.slo, at)
            return self._replace_budget_state(raw, BudgetState.RESET)

    def alerts(self, service: str, *, now: float | None = None) -> list[AlertSnapshot]:
        with self._lock:
            rt = self._runtime(service)
            at = self._at(now)
            result: list[AlertSnapshot] = []
            br = windows_mod.burn_rate(rt.log, rt.slo, at)
            for rule in rt.slo.rules:
                prev = rt.alert_states[rule.name]
                nxt = alerts_mod.evaluate_rule(
                    rule,
                    br.long_window.burn_rate,
                    br.short_window.burn_rate,
                    prev,
                    at,
                )
                # Persist only real lifecycle transitions, not metric refresh.
                if (
                    nxt.state != prev.state
                    or nxt.last_fired_at != prev.last_fired_at
                    or nxt.cooldown_until != prev.cooldown_until
                ):
                    rt.alert_states[rule.name] = nxt
                    d = asdict(nxt)
                    d.update({"type": "alert", "service": service})
                    d["state"] = nxt.state.value
                    self._append(d)
                result.append(nxt)
            return result

    # ------------------------------------------------------------------ misc

    def _at(self, now: float | None) -> float:
        return float(now) if now is not None else self._clock.now()

    def event_count(self, service: str) -> int:
        with self._lock:
            return len(self._runtime(service).log)

    def close(self) -> None:
        with self._lock:
            if self._writer is not None:
                self._writer.close()
