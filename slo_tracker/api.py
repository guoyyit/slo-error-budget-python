"""FastAPI application exposing ingestion and query endpoints on loopback.

Everything is local: the service holds its data in memory plus a local WAL
file; no external services or credentials are involved.
"""
from __future__ import annotations

from dataclasses import asdict
from enum import Enum

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .models import (
    AlertSnapshot,
    BudgetSnapshot,
    BurnRateResult,
    IngestResult,
    Observation,
)
from .service import SloService


class ObservationIn(BaseModel):
    event_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    timestamp: float
    success: bool


def _jsonable(value: object) -> object:
    """Convert dataclasses / enums into plain JSON-friendly values."""
    if isinstance(value, Enum):
        return value.value
    if hasattr(value, "__dataclass_fields__"):
        return {k: _jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def create_app(service: SloService) -> FastAPI:
    app = FastAPI(title="SLO Error Budget Tracker", version="0.1.0")

    @app.post("/observations", status_code=202)
    def ingest(obs: ObservationIn) -> dict[str, object]:
        result: IngestResult = service.ingest(
            Observation(
                event_id=obs.event_id,
                service=obs.service,
                timestamp=obs.timestamp,
                success=obs.success,
            )
        )
        body = _jsonable(result)
        assert isinstance(body, dict)
        # Rejected observations are still explained, but not 2xx success.
        if result.status.value != "accepted":
            body["accepted"] = False
            return body
        body["accepted"] = True
        return body

    @app.get("/services/{service_name}/burn-rate")
    def burn_rate(service_name: str) -> dict[str, object]:
        try:
            result: BurnRateResult = service.burn_rate(service_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _jsonable(result)  # type: ignore[return-value]

    @app.get("/services/{service_name}/budget")
    def budget(service_name: str) -> dict[str, object]:
        try:
            result: BudgetSnapshot = service.budget(service_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _jsonable(result)  # type: ignore[return-value]

    @app.post("/services/{service_name}/budget/reset")
    def reset_budget(service_name: str) -> dict[str, object]:
        try:
            result = service.reset_budget(service_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _jsonable(result)  # type: ignore[return-value]

    @app.get("/services/{service_name}/alerts")
    def alerts(service_name: str) -> dict[str, object]:
        try:
            result: list[AlertSnapshot] = service.alerts(service_name)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"alerts": _jsonable(result)}

    return app
