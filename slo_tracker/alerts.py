"""Multi-window burn-rate alert evaluation with cooldown silencing.

A rule fires only when the *long* window burn rate is at/above its
threshold **and** the *short* window burn rate is at/above its threshold
simultaneously. The long window guards against page fatigue; the short
window provides the fast signal. Unknown (no-traffic) windows never breach.

State machine::

    INACTIVE   --condition true-->  FIRING     (one notification recorded)
    FIRING / SUPPRESSED while now < cooldown_until: FROZEN
        - a still-true condition  -> SUPPRESSED (repeat hit silenced)
        - a false condition (incl. one caused by late-arriving events)
          -> SUPPRESSED as well; the notification already sent is neither
             re-paged nor revoked; ``firing`` reflects the current condition
    after cooldown:
        condition false -> RESOLVED
        condition true  -> FIRING (a genuinely new notification)
    RESOLVED   --condition true, cooldown elapsed--> FIRING
    RESOLVED   --condition true, still in cooldown--> SUPPRESSED

Because the notification state is frozen for the whole cooldown window,
late/out-of-order observations arriving in that interval cannot alter an
already alerted result or trigger repeated alerts for one sustained
incident. The observable reasons distinguish "freshly fired",
"repeat hit suppressed", "late data frozen during cooldown" and
"resolved".
"""
from __future__ import annotations

from .config import BurnRateRule
from .models import AlertSnapshot, AlertState


def initial_snapshot(rule: BurnRateRule) -> AlertSnapshot:
    return AlertSnapshot(
        rule_name=rule.name,
        state=AlertState.INACTIVE,
        firing=False,
        long_window_seconds=rule.long_window_seconds,
        short_window_seconds=rule.short_window_seconds,
        long_burn_rate=None,
        short_burn_rate=None,
        last_fired_at=None,
        cooldown_until=None,
        reason="never evaluated",
    )


def condition_met(
    rule: BurnRateRule, long_burn: float | None, short_burn: float | None
) -> bool:
    """Both windows must breach; unknown (no-traffic) windows never breach."""
    if long_burn is None or short_burn is None:
        return False
    return (
        long_burn >= rule.long_burn_rate_threshold
        and short_burn >= rule.short_burn_rate_threshold
    )


def _copy(
    snap: AlertSnapshot,
    *,
    state: AlertState | None = None,
    firing: bool | None = None,
    long_burn: float | None = None,
    short_burn: float | None = None,
    last_fired_at: float | None = None,
    cooldown_until: float | None = None,
    reason: str | None = None,
) -> AlertSnapshot:
    return AlertSnapshot(
        rule_name=snap.rule_name,
        state=state if state is not None else snap.state,
        firing=snap.firing if firing is None else firing,
        long_window_seconds=snap.long_window_seconds,
        short_window_seconds=snap.short_window_seconds,
        long_burn_rate=long_burn if long_burn is not None else snap.long_burn_rate,
        short_burn_rate=short_burn if short_burn is not None else snap.short_burn_rate,
        last_fired_at=snap.last_fired_at if last_fired_at is None else last_fired_at,
        cooldown_until=snap.cooldown_until if cooldown_until is None else cooldown_until,
        reason=snap.reason if reason is None else reason,
    )


def evaluate_rule(
    rule: BurnRateRule,
    long_burn: float | None,
    short_burn: float | None,
    state: AlertSnapshot,
    now: float,
) -> AlertSnapshot:
    """Advance one rule's state machine given the current burn rates."""
    met = condition_met(rule, long_burn, short_burn)
    in_cooldown = state.cooldown_until is not None and now < state.cooldown_until

    if state.state in (AlertState.FIRING, AlertState.SUPPRESSED) and in_cooldown:
        # Cooldown freeze: the notification recorded at last_fired_at is
        # final for this interval. Late/out-of-order data can neither re-fire
        # nor resolve it; the exposed state stays explainable.
        if met:
            return _copy(
                state,
                state=AlertState.SUPPRESSED,
                firing=True,
                long_burn=long_burn,
                short_burn=short_burn,
                reason=(
                    f"repeat hit at t={now:g} silenced; cooldown ends at "
                    f"t={state.cooldown_until:g} (last fired t={state.last_fired_at:g})"
                ),
            )
        return _copy(
            state,
            state=AlertState.SUPPRESSED,
            firing=False,
            long_burn=long_burn,
            short_burn=short_burn,
            reason=(
                f"condition no longer breached at t={now:g} (possibly due to "
                f"late data) but the alert raised at t={state.last_fired_at:g} "
                f"is frozen until cooldown ends t={state.cooldown_until:g}"
            ),
        )

    if not met:
        if state.state in (AlertState.FIRING, AlertState.SUPPRESSED):
            return _copy(
                state,
                state=AlertState.RESOLVED,
                firing=False,
                long_burn=long_burn,
                short_burn=short_burn,
                reason=f"condition cleared after cooldown at t={now:g}; resolved",
            )
        # INACTIVE / RESOLVED stay as they are; refresh metrics only.
        return _copy(state, long_burn=long_burn, short_burn=short_burn)

    if state.state == AlertState.INACTIVE:
        why = "both windows breached thresholds: alert fired"
    elif state.state == AlertState.RESOLVED:
        why = "fresh breach after prior resolution: alert fired"
    else:
        # FIRING/SUPPRESSED reached here only once the cooldown elapsed.
        why = "breach still present after cooldown: new notification fired"
    return _copy(
        state,
        state=AlertState.FIRING,
        firing=True,
        long_burn=long_burn,
        short_burn=short_burn,
        last_fired_at=now,
        cooldown_until=now + rule.cooldown_seconds,
        reason=why,
    )
