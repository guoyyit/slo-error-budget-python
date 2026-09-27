"""FastAPI application exposing the SLO service over loopback.

Endpoints
---------
``POST /services``                configure a service SLO and windows
``POST /services/{svc}/observe``  report one request observation
``GET  /services/{svc}``          self-consistent snapshot
``GET  /services/{svc}/availability``
``GET  /services/{svc}/budget``
``GET  /services/{svc}/alert``
``POST /services/{svc}/reset``    explicitly anchor a fresh budget period

Only the local process and on-disk store are used; no external services or
credentials are required.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .clock import Clock, SystemClock
from .models import (
    Observation,
    Outcome,
    RejectReason,
    SLOConfig,
)
from .service import SLOService
from .storage import EventStore


# --------------------------------------------------------------- request models


class SLOConfigIn(BaseModel):
    service: str
    availability_target: float = Field(gt=0.0, le=1.0)
    availability_window_seconds: float = Field(gt=0.0)
    short_window_seconds: float = Field(gt=0.0)
    long_window_seconds: float = Field(gt=0.0)
    alert_short_burn: float = Field(gt=0.0)
    alert_long_burn: float = Field(gt=0.0)
    cooldown_seconds: float = Field(gt=0.0)
    budget_window_seconds: float = Field(gt=0.0)
    budget_expected_requests: float | None = Field(default=None, gt=0.0)


class ObservationIn(BaseModel):
    observation_id: str
    timestamp: float
    outcome: Outcome


class ResetIn(BaseModel):
    at: float | None = None


# --------------------------------------------------------------- serialization


def _window_burn_json(burn: Any) -> dict[str, Any] | None:
    if burn is None:
        return None
    return {
        "window_seconds": burn.window_seconds,
        "window_start": burn.window_start,
        "window_end": burn.window_end,
        "total": burn.total,
        "failures": burn.failures,
        "error_rate": burn.error_rate,
        "burn_rate": burn.burn_rate,
    }


def _snapshot_json(snap: Any) -> dict[str, Any]:
    av = snap.availability
    budget = snap.budget
    alert = snap.alert
    return {
        "service": snap.config.service,
        "evaluated_at": snap.evaluated_at,
        "availability": {
            "status": av.status.value,
            "window_start": av.window_start,
            "window_end": av.window_end,
            "total": av.total,
            "successes": av.successes,
            "failures": av.failures,
            "ratio": av.ratio,
            "reason": av.reason,
        },
        "burn": {
            "short": _window_burn_json(snap.short_burn),
            "long": _window_burn_json(snap.long_burn),
        },
        "budget": {
            "period_start": budget.period_start,
            "period_end": budget.period_end,
            "total_budget": budget.total_budget,
            "consumed": budget.consumed,
            "remaining": budget.remaining,
            "burn_fraction": budget.burn_fraction,
            "total_requests": budget.total_requests,
            "failures": budget.failures,
            "exhausted": budget.exhausted,
        },
        "alert": {
            "state": alert.state.value,
            "breached": alert.breached,
            "suppressed": alert.suppressed,
            "last_fired_at": alert.last_fired_at,
            "cooldown_until": alert.cooldown_until,
            "short_window": _window_burn_json(alert.short_window),
            "long_window": _window_burn_json(alert.long_window),
            "detail": alert.detail,
        },
        "watermark": snap.watermark,
    }


# --------------------------------------------------------------- app factory


def create_app(
    service: SLOService | None = None,
    store: EventStore | None = None,
    clock: Clock | None = None,
    data_dir: str | None = None,
) -> FastAPI:
    """Build the FastAPI app.

    By default a real on-disk :class:`EventStore`, :class:`SystemClock` and
    :class:`SLOService` are wired. Tests may inject their own instances.
    """
    app = FastAPI(title="SLO Error-Budget Tracker", version="0.1.0")

    if clock is None:
        clock = SystemClock()
    if store is None:
        store = EventStore(data_dir or os.environ.get("SLO_DATA_DIR", "./data"))
    if service is None:
        service = SLOService(store, clock)

    app.state.service = service
    app.state.store = store
    app.state.clock = clock

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/services", status_code=201)
    def configure_service(payload: SLOConfigIn) -> dict[str, Any]:
        config = SLOConfig(**payload.model_dump())
        service.configure(config)
        return {"service": config.service, "configured": True}

    @app.post("/services/{svc}/observe", status_code=202)
    def observe(svc: str, payload: ObservationIn) -> JSONResponse:
        observation = Observation(
            observation_id=payload.observation_id,
            service=svc,
            timestamp=payload.timestamp,
            outcome=payload.outcome,
        )
        accepted, reason, detail = service.ingest(observation)
        body: dict[str, Any] = {
            "accepted": accepted,
            "reason": reason.value if reason is not None else None,
            "detail": detail,
        }
        if accepted:
            return JSONResponse(status_code=202, content=body)
        status_code = {
            RejectReason.DUPLICATE: 409,
            RejectReason.TOO_LATE: 422,
            RejectReason.UNKNOWN_SERVICE: 404,
            RejectReason.MALFORMED: 400,
        }.get(reason, 400)
        return JSONResponse(status_code=status_code, content=body)

    @app.get("/services/{svc}")
    def get_service(svc: str) -> dict[str, Any]:
        try:
            return _snapshot_json(service.query(svc))
        except KeyError:
            raise HTTPException(status_code=404, detail=f"service '{svc}' not found")

    @app.get("/services/{svc}/availability")
    def get_availability(svc: str) -> dict[str, Any]:
        try:
            av = service.availability(svc)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"service '{svc}' not found")
        return {
            "service": av.service,
            "status": av.status.value,
            "window_start": av.window_start,
            "window_end": av.window_end,
            "total": av.total,
            "successes": av.successes,
            "failures": av.failures,
            "ratio": av.ratio,
            "reason": av.reason,
        }

    @app.get("/services/{svc}/budget")
    def get_budget(svc: str) -> dict[str, Any]:
        try:
            budget = service.query(svc).budget
        except KeyError:
            raise HTTPException(status_code=404, detail=f"service '{svc}' not found")
        assert budget is not None
        return {
            "service": budget.service,
            "period_start": budget.period_start,
            "period_end": budget.period_end,
            "total_budget": budget.total_budget,
            "consumed": budget.consumed,
            "remaining": budget.remaining,
            "burn_fraction": budget.burn_fraction,
            "total_requests": budget.total_requests,
            "failures": budget.failures,
            "exhausted": budget.exhausted,
        }

    @app.get("/services/{svc}/alert")
    def get_alert(svc: str) -> dict[str, Any]:
        try:
            alert = service.alert_status(svc)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"service '{svc}' not found")
        return {
            "service": alert.service,
            "state": alert.state.value,
            "breached": alert.breached,
            "suppressed": alert.suppressed,
            "last_fired_at": alert.last_fired_at,
            "cooldown_until": alert.cooldown_until,
            "detail": alert.detail,
            "short_window": _window_burn_json(alert.short_window),
            "long_window": _window_burn_json(alert.long_window),
        }

    @app.post("/services/{svc}/reset", status_code=200)
    def reset_budget(svc: str, payload: ResetIn | None = None) -> dict[str, Any]:
        try:
            budget = service.reset_budget(svc, at=payload.at if payload else None)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"service '{svc}' not found")
        return {
            "service": budget.service,
            "period_start": budget.period_start,
            "exhausted": budget.exhausted,
            "total_budget": budget.total_budget,
            "consumed": budget.consumed,
        }

    return app
