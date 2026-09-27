"""Local loopback entrypoint.

Run with::

    uv run uvicorn main:app --host 127.0.0.1 --port 8000

A demo service "checkout" is configured with a 99.9% availability target,
1h/5m burn windows, a 1-day error-budget period and two multi-window
alerting rules. Committed data persists in ``./.data/wal.log`` and is
replayed on restart; no external services or credentials are used.
"""
from __future__ import annotations

import os

from slo_tracker.api import create_app
from slo_tracker.clock import SystemClock
from slo_tracker.config import BurnRateRule, ServiceSLO, StoreConfig
from slo_tracker.service import SloService

HOUR = 3600.0
DAY = 86400.0

CHECKOUT_SLO = ServiceSLO(
    name="checkout",
    target_availability=0.999,
    long_window_seconds=HOUR,
    short_window_seconds=5 * 60.0,
    budget_period_seconds=DAY,
    max_future_skew_seconds=60.0,
    rules=(
        # Google SRE-style multi-window, multi-burnrate thresholds.
        BurnRateRule(
            name="page-fast",
            long_window_seconds=HOUR,
            short_window_seconds=5 * 60.0,
            long_burn_rate_threshold=14.4,
            short_burn_rate_threshold=14.4,
            cooldown_seconds=30 * 60.0,
        ),
        BurnRateRule(
            name="ticket-slow",
            long_window_seconds=6 * HOUR,
            short_window_seconds=30 * 60.0,
            long_burn_rate_threshold=6.0,
            short_burn_rate_threshold=6.0,
            cooldown_seconds=2 * HOUR,
        ),
    ),
)

DATA_DIR = os.environ.get("SLO_DATA_DIR", os.path.join(os.path.dirname(__file__), ".data"))

service = SloService(
    [CHECKOUT_SLO],
    clock=SystemClock(),
    store=StoreConfig(DATA_DIR, fsync=True),
)
app = create_app(service)
