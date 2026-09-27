"""Concurrency-safe SLO service: ingest, query, reset, alert evaluation.

Concurrency model
-----------------
One :class:`threading.RLock` guards all per-service in-memory state. Every
mutation appends a durable record *while holding the lock* and only updates
in-memory state afterwards, so a query always observes a self-consistent set
of committed observations (no partial writes).

Late / duplicate handling (Requirement 4)
-----------------------------------------
* Duplicates (same ``observation_id``) are rejected with ``DUPLICATE``.
* When an alert fires, the long-window left edge at that moment becomes a
  sealing watermark. Events older than the watermark are rejected with
  ``TOO_LATE`` because they precede sealed, already-signalled history and
  must not change an alerted result.

Budget latch (Requirement 3)
----------------------------
Once a budget is exhausted it stays exhausted until :meth:`reset_budget`
anchors a fresh period at an explicit, deterministic boundary.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field

from .clock import Clock
from .models import (
    AlertConditionState,
    AlertStatus,
    AvailabilityResult,
    BudgetState,
    Observation,
    Outcome,
    RejectReason,
    SLOConfig,
    ServiceSnapshot,
    WindowBurn,
)
from .storage import EventStore
from .windows import (
    align_period_start,
    compute_availability,
    compute_budget,
    compute_window_burn,
)


@dataclass
class _Runtime:
    config: SLOConfig
    observations: list[Observation] = field(default_factory=list)
    seen_ids: set[str] = field(default_factory=set)
    # Sealing watermark: -inf until an alert seals history.
    watermark: float = float("-inf")
    # Budget period anchor.
    period_start: float = 0.0
    # Latch: once exhausted the period stays exhausted until explicit reset.
    exhausted_latch: bool = False
    # Alert condition machine.
    alert_state: AlertConditionState = AlertConditionState.IDLE
    last_fired_at: float | None = None
    cooldown_until: float | None = None


class SLOService:
    """Coordinates storage, window computation and alert state."""

    def __init__(self, store: EventStore, clock: Clock) -> None:
        self._store = store
        self._clock = clock
        self._lock = threading.RLock()
        self._services: dict[str, _Runtime] = {}
        self._recover()

    # ---------------------------------------------------------------- setup

    def _recover(self) -> None:
        """Rebuild in-memory state from durable storage."""
        with self._lock:
            for config in self._store.load_configs().values():
                rt = _Runtime(config=config)
                self._services[config.service] = rt
            for obs in self._store.load_observations():
                rt = self._services.get(obs.service)
                if rt is None:
                    # Observation for a service whose config is gone: keep it
                    # out of the active index but it remains durable.
                    continue
                rt.observations.append(obs)
                rt.seen_ids.add(obs.observation_id)
            states = self._store.load_states()
            for service, row in states.items():
                rt = self._services.get(service)
                if rt is None:
                    continue
                budget = self._store.budget_from_dict(row["budget"])
                alert = self._store.alert_from_dict(row["alert"])
                rt.watermark = float(row.get("watermark", float("-inf")))
                rt.period_start = float(row.get("period_start", budget.period_start))
                rt.exhausted_latch = budget.exhausted
                rt.alert_state = alert.state
                rt.last_fired_at = alert.last_fired_at
                rt.cooldown_until = alert.cooldown_until

    def configure(self, config: SLOConfig) -> None:
        """Register or update a service configuration (durable)."""
        with self._lock:
            self._store.save_config(config)
            existing = self._services.get(config.service)
            if existing is None:
                rt = _Runtime(
                    config=config,
                    # The first budget period begins when monitoring starts;
                    # later periods are fixed multiples of the window beyond it.
                    period_start=self._clock.now(),
                )
                self._services[config.service] = rt
            else:
                existing.config = config

    # --------------------------------------------------------------- ingest

    def ingest(
        self, observation: Observation
    ) -> tuple[bool, RejectReason | None, str]:
        """Validate and commit one observation.

        Returns ``(accepted, reason, detail)``. The durability write happens
        inside the service lock; in-memory state is updated only after the
        record is committed.
        """
        with self._lock:
            rt = self._services.get(observation.service)
            if rt is None:
                return (
                    False,
                    RejectReason.UNKNOWN_SERVICE,
                    f"service '{observation.service}' is not configured",
                )

            # Duplicate is checked first so replaying an old-but-valid id is
            # reported as DUPLICATE, not as TOO_LATE.
            if observation.observation_id in rt.seen_ids:
                return (
                    False,
                    RejectReason.DUPLICATE,
                    f"observation_id '{observation.observation_id}' already committed",
                )

            if observation.timestamp < rt.watermark:
                return (
                    False,
                    RejectReason.TOO_LATE,
                    (
                        f"event time {observation.timestamp} is older than the "
                        f"sealed watermark {rt.watermark}; history is sealed"
                    ),
                )

            stamped = Observation(
                observation_id=observation.observation_id,
                service=observation.service,
                timestamp=observation.timestamp,
                outcome=observation.outcome,
                ingest_time=self._clock.now(),
            )
            # Durable commit first. If this raises, nothing changes in memory.
            self._store.append_observation(stamped)

            # Re-check duplicate after the durable write to guard against a
            # same-id record already persisted outside this process.
            if stamped.observation_id in rt.seen_ids:
                return (
                    False,
                    RejectReason.DUPLICATE,
                    f"observation_id '{stamped.observation_id}' already committed",
                )
            rt.observations.append(stamped)
            rt.seen_ids.add(stamped.observation_id)

            self._evaluate_and_persist(rt)
            return True, None, "committed"

    # ------------------------------------------------------------ evaluation

    @staticmethod
    def _is_breached(
        cfg: SLOConfig,
        observations: list[Observation],
        now: float,
    ) -> tuple[bool, WindowBurn, WindowBurn]:
        short = compute_window_burn(
            cfg, observations, now, cfg.short_window_seconds
        )
        long = compute_window_burn(
            cfg, observations, now, cfg.long_window_seconds
        )

        def over(burn: WindowBurn, threshold: float) -> bool:
            rate = burn.burn_rate
            return rate is not None and (
                math.isinf(rate) or rate >= threshold
            )

        breached = over(short, cfg.alert_short_burn) and over(
            long, cfg.alert_long_burn
        )
        return breached, short, long

    def _transition(
        self, rt: _Runtime, now: float
    ) -> tuple[AlertConditionState, bool, WindowBurn, WindowBurn, str]:
        """Pure alert transition. Returns ``(state, fired_now, short, long, detail)``.

        Rules:
        * breach + episode currently FIRING (never resolved) -> keep FIRING,
          no repeat notification for the same persistent condition;
        * breach after the condition had resolved, within the quiet window ->
          SUPPRESSED_COOLDOWN;
        * breach outside any quiet window -> a new FIRING episode;
        * no breach -> RESOLVED if an episode ever fired, else IDLE.

        The quiet window runs from ``last_fired_at`` for ``cooldown_seconds``;
        resolving early does not shorten it.
        """
        breached, short, long = self._is_breached(rt.config, rt.observations, now)
        in_cooldown = rt.cooldown_until is not None and now < rt.cooldown_until

        if not breached:
            if rt.alert_state is AlertConditionState.IDLE:
                return AlertConditionState.IDLE, False, short, long, "no breach"
            return (
                AlertConditionState.RESOLVED,
                False,
                short,
                long,
                "condition cleared; alert resolved",
            )

        if rt.alert_state is AlertConditionState.FIRING:
            return (
                AlertConditionState.FIRING,
                False,
                short,
                long,
                "condition still breached; active episode (no repeat alert)",
            )

        if in_cooldown:
            return (
                AlertConditionState.SUPPRESSED_COOLDOWN,
                False,
                short,
                long,
                f"breached but suppressed during cooldown until {rt.cooldown_until}",
            )

        return (
            AlertConditionState.FIRING,
            True,
            short,
            long,
            "multi-window burn thresholds exceeded; alert fired",
        )

    def _render_alert(self, rt: _Runtime, now: float) -> AlertStatus:
        """Pure read: render alert state without mutating or persisting."""
        state, _fired, short, long, detail = self._transition(rt, now)
        return AlertStatus(
            service=rt.config.service,
            state=state,
            short_window=short,
            long_window=long,
            last_fired_at=rt.last_fired_at,
            cooldown_until=rt.cooldown_until,
            breached=bool(
                short.burn_rate is not None
                and long.burn_rate is not None
            )
            and state
            in (
                AlertConditionState.FIRING,
                AlertConditionState.SUPPRESSED_COOLDOWN,
            ),
            suppressed=state is AlertConditionState.SUPPRESSED_COOLDOWN,
            detail=detail,
        )

    def _current_period_start(self, rt: _Runtime, now: float) -> float:
        """Return the active budget period start.

        Periods follow fixed multiples of ``budget_window_seconds`` aligned to
        the epoch origin, or to the last explicit :meth:`reset_budget` anchor
        (which itself sits on a boundary), so the old/new budget boundary is
        deterministic. An exhausted period is latched and never rolls into a
        fresh budget without an explicit reset.
        """
        if rt.exhausted_latch:
            return rt.period_start
        return align_period_start(
            now, rt.config.budget_window_seconds, anchor=rt.period_start
        )

    def _evaluate_and_persist(self, rt: _Runtime) -> None:
        """Recompute derived state for one ingest and durably record it."""
        now = self._clock.now()
        alert, fired, _changed = self._commit_alert_transition(rt, now)
        if fired:
            # Sealing watermark: the long-window left edge for this firing.
            rt.watermark = max(
                rt.watermark, now - rt.config.long_window_seconds
            )

        period_start = self._current_period_start(rt, now)
        budget = compute_budget(
            rt.config, rt.observations, period_start, None, now
        )
        if budget.exhausted:
            rt.exhausted_latch = True
        elif rt.exhausted_latch:
            # Re-wrap the computed budget to preserve the latch until reset.
            budget = BudgetState(
                service=budget.service,
                period_start=budget.period_start,
                period_end=budget.period_end,
                availability_target=budget.availability_target,
                total_requests=budget.total_requests,
                failures=budget.failures,
                total_budget=budget.total_budget,
                consumed=budget.consumed,
                remaining=budget.remaining,
                burn_fraction=budget.burn_fraction,
                exhausted=True,
            )

        self._store.append_state(
            rt.config.service,
            budget,
            alert,
            watermark=rt.watermark,
            period_start=rt.period_start,
        )

    # ---------------------------------------------------------------- reset

    def reset_budget(self, service: str, at: float | None = None) -> BudgetState:
        """Explicitly anchor a fresh error-budget period.

        The new period starts at ``at`` (defaulting to now). That instant is
        the deterministic old/new budget boundary; the exhausted latch is
        cleared for the new period.
        """
        with self._lock:
            rt = self._require(service)
            anchor = self._clock.now() if at is None else at
            rt.period_start = anchor
            rt.exhausted_latch = False
            budget = compute_budget(rt.config, rt.observations, anchor, None, self._clock.now())
            self._store.append_state(
                service,
                budget,
                self._render_alert(rt, self._clock.now()),
                watermark=rt.watermark,
                period_start=anchor,
            )
            return budget

    # ---------------------------------------------------------------- query

    def _require(self, service: str) -> _Runtime:
        rt = self._services.get(service)
        if rt is None:
            raise KeyError(f"service '{service}' is not configured")
        return rt

    def _commit_alert_transition(
        self, rt: _Runtime, now: float
    ) -> tuple[AlertStatus, bool, bool]:
        """Advance the alert machine. Returns ``(status, fired_now, changed)``.

        Time-driven transitions (e.g. the condition clearing as events age
        out) also become committed state, keeping reads and the durable log
        consistent with what was observed.
        """
        state, fired, short, long, detail = self._transition(rt, now)
        changed = state is not rt.alert_state
        if fired:
            rt.last_fired_at = now
            rt.cooldown_until = now + rt.config.cooldown_seconds
            state = AlertConditionState.FIRING
        rt.alert_state = state
        status = AlertStatus(
            service=rt.config.service,
            state=state,
            short_window=short,
            long_window=long,
            last_fired_at=rt.last_fired_at,
            cooldown_until=rt.cooldown_until,
            breached=state
            in (
                AlertConditionState.FIRING,
                AlertConditionState.SUPPRESSED_COOLDOWN,
            ),
            suppressed=state is AlertConditionState.SUPPRESSED_COOLDOWN,
            detail=detail,
        )
        return status, fired, changed or fired

    def query(self, service: str) -> ServiceSnapshot:
        """Return a self-consistent snapshot from one committed point in time."""
        with self._lock:
            rt = self._require(service)
            now = self._clock.now()
            availability = compute_availability(
                rt.config, rt.observations, now
            )
            short = compute_window_burn(
                rt.config, rt.observations, now, rt.config.short_window_seconds
            )
            long = compute_window_burn(
                rt.config, rt.observations, now, rt.config.long_window_seconds
            )
            period_start = self._current_period_start(rt, now)
            budget = compute_budget(
                rt.config, rt.observations, period_start, None, now
            )
            if rt.exhausted_latch:
                budget = BudgetState(
                    service=budget.service,
                    period_start=budget.period_start,
                    period_end=budget.period_end,
                    availability_target=budget.availability_target,
                    total_requests=budget.total_requests,
                    failures=budget.failures,
                    total_budget=budget.total_budget,
                    consumed=budget.consumed,
                    remaining=budget.remaining,
                    burn_fraction=budget.burn_fraction,
                    exhausted=True,
                )
            # Advance time-driven alert transitions and commit any change so
            # the returned status is a committed, recoverable state.
            alert, _fired, alert_changed = self._commit_alert_transition(rt, now)
            if alert_changed:
                self._store.append_state(
                    rt.config.service,
                    budget,
                    alert,
                    watermark=rt.watermark,
                    period_start=rt.period_start,
                )
            return ServiceSnapshot(
                config=rt.config,
                evaluated_at=now,
                # Defensive copy: callers cannot mutate committed state.
                observations=list(rt.observations),
                availability=availability,
                short_burn=short,
                long_burn=long,
                budget=budget,
                alert=alert,
                watermark=rt.watermark,
            )

    def availability(self, service: str) -> AvailabilityResult:
        return self.query(service).availability  # type: ignore[return-value]

    def alert_status(self, service: str) -> AlertStatus:
        return self.query(service).alert  # type: ignore[return-value]

    def shutdown(self) -> None:
        """Durably flush everything before process exit."""
        with self._lock:
            self._store.close()
