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

    def _render_alert(self, rt: _Runtime, now: float) -> AlertStatus:
        """Pure read: render current alert state without mutating anything.

        An *active* firing episode (the most recent event fired and has not
        resolved) reports FIRING while the condition still holds. A repeat hit
        during the quiet window of an episode that had already resolved is
        shown as SUPPRESSED_COOLDOWN.
        """
        breached, short, long = self._is_breached(rt.config, rt.observations, now)
        episode_active = (
            rt.alert_state is AlertConditionState.FIRING
        )
        in_cooldown = rt.cooldown_until is not None and now < rt.cooldown_until

        if not breached:
            resolved = rt.last_fired_at is not None
            state = (
                AlertConditionState.RESOLVED if resolved else AlertConditionState.IDLE
            )
            detail = "condition cleared" if resolved else "no breach"
        elif episode_active:
            state = AlertConditionState.FIRING
            detail = "multi-window burn thresholds exceeded; alert active"
        elif in_cooldown:
            state = AlertConditionState.SUPPRESSED_COOLDOWN
            detail = (
                f"breached but suppressed during cooldown until "
                f"{rt.cooldown_until}"
            )
        else:
            state = AlertConditionState.FIRING
            detail = "multi-window burn thresholds exceeded; alert firing"

        return AlertStatus(
            service=rt.config.service,
            state=state,
            short_window=short,
            long_window=long,
            last_fired_at=rt.last_fired_at,
            cooldown_until=rt.cooldown_until,
            breached=breached,
            suppressed=state is AlertConditionState.SUPPRESSED_COOLDOWN,
            detail=detail,
        )

    def _commit_alert(self, rt: _Runtime, now: float) -> tuple[AlertStatus, bool]:
        """Commit alert-machine transitions at ingest time.

        Returns ``(status, fired_new_episode)``. A new firing episode is only
        recorded when the condition is breached outside an active cooldown.
        A condition that clears ends the episode (cooldown discarded) so a
        later, genuinely new breach can alert immediately.
        """
        breached, short, long = self._is_breached(rt.config, rt.observations, now)
        in_cooldown = rt.cooldown_until is not None and now < rt.cooldown_until
        episode_active = rt.alert_state is AlertConditionState.FIRING

        fired = False
        if not breached:
            # Condition no longer holds: end any open episode so its cooldown
            # cannot suppress a future, distinct incident.
            rt.alert_state = AlertConditionState.RESOLVED
            rt.cooldown_until = None
        elif episode_active:
            # Same persistent episode: keep firing; do not re-notify.
            rt.alert_state = AlertConditionState.FIRING
        elif in_cooldown:
            # Repeat hit within the quiet window of a resolved episode.
            rt.alert_state = AlertConditionState.SUPPRESSED_COOLDOWN
        else:
            # A genuinely new episode starts.
            rt.last_fired_at = now
            rt.cooldown_until = now + rt.config.cooldown_seconds
            rt.alert_state = AlertConditionState.FIRING
            fired = True

        status = self._render_alert(rt, now)
        return status, fired

    def _current_period_start(self, rt: _Runtime, now: float) -> float:
        """Return the active period start, rolling the fixed window if due.

        Periods are anchored at multiples of ``budget_window_seconds`` (epoch
        origin, or the last explicit reset anchor), giving a deterministic
        old/new budget boundary.
        """
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
        alert, fired = self._commit_alert(rt, now)
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

    def _alert_snapshot(self, rt: _Runtime) -> AlertStatus:
        return self._render_alert(rt, self._clock.now())

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
            alert = self._render_alert(rt, now)
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
