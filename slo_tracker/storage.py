"""Local persistence: append-only JSONL logs with fsync durability.

Layout inside ``directory``::

    observations.jsonl   one committed Observation per line
    state.jsonl          one committed service-state transition per line
    configs/<service>.json   latest configuration per service (durable rewrite)

A record is considered committed only after it has been written, flushed and
fsynced. Each record is additionally framed with a leading length and a
trailing CRC so a torn trailing write left by a crash can be detected and
discarded during recovery rather than mistaken for a successful observation.
"""

from __future__ import annotations

import json
import os
import zlib
from pathlib import Path
from typing import Any

from .models import (
    AlertConditionState,
    AlertStatus,
    BudgetState,
    Observation,
    Outcome,
    SLOConfig,
    WindowBurn,
)

_OBS_FILE = "observations.jsonl"
_STATE_FILE = "state.jsonl"
_CONFIG_DIR = "configs"


def _frame(payload: dict[str, Any]) -> bytes:
    """Frame one record as ``<length>.<crc>.<json>\\n``."""
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    crc = zlib.crc32(body) & 0xFFFFFFFF
    return f"{len(body)}.{crc}.".encode("ascii") + body + b"\n"


def _read_framed(path: Path) -> list[dict[str, Any]]:
    """Read all intact framed records; drop the first corrupt/torn tail record."""
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        for line in handle:
            # Record format is len.crc.body and the JSON body may itself
            # contain '.', so split with maxsplit=2.
            parts = line.rstrip(b"\n").split(b".", 2)
            if len(parts) != 3:
                break
            length_bytes, crc_bytes, body_bytes = parts
            length = int(length_bytes.decode("ascii"))
            crc = int(crc_bytes.decode("ascii"))
            if len(body_bytes) != length:
                break  # torn write: stop, discard this and anything after
            if (zlib.crc32(body_bytes) & 0xFFFFFFFF) != crc:
                break  # corrupt record: stop
            try:
                records.append(json.loads(body_bytes.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError):
                break
    return records


def _append_framed(path: Path, payload: dict[str, Any]) -> None:
    """Append, flush and fsync one framed record."""
    data = _frame(payload)
    with path.open("ab") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


class EventStore:
    """Durable storage for one data directory."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        (self.directory / _CONFIG_DIR).mkdir(parents=True, exist_ok=True)
        self._obs_path = self.directory / _OBS_FILE
        self._state_path = self.directory / _STATE_FILE
        self._config_dir = self.directory / _CONFIG_DIR
        # Touch the logs so readers never see a missing file unexpectedly.
        self._obs_path.touch(exist_ok=True)
        self._state_path.touch(exist_ok=True)

    # -- observations ----------------------------------------------------

    def append_observation(self, observation: Observation) -> None:
        _append_framed(self._obs_path, self._observation_to_dict(observation))

    def load_observations(self) -> list[Observation]:
        return [
            self._observation_from_dict(row)
            for row in _read_framed(self._obs_path)
        ]

    # -- state -----------------------------------------------------------

    def append_state(
        self,
        service: str,
        budget: BudgetState,
        alert: AlertStatus,
        watermark: float,
        period_start: float,
    ) -> None:
        _append_framed(
            self._state_path,
            {
                "service": service,
                "budget": self._budget_to_dict(budget),
                "alert": self._alert_to_dict(alert),
                "watermark": watermark,
                "period_start": period_start,
            },
        )

    def load_states(self) -> dict[str, dict[str, Any]]:
        """Return the last committed state record per service."""
        latest: dict[str, dict[str, Any]] = {}
        for row in _read_framed(self._state_path):
            latest[row["service"]] = row
        return latest

    # -- configs ---------------------------------------------------------

    def save_config(self, config: SLOConfig) -> None:
        path = self._config_dir / f"{config.service}.json"
        tmp = path.with_suffix(".json.tmp")
        body = json.dumps(
            self._config_to_dict(config), separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        with tmp.open("wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        self._fsync_dir()

    def load_configs(self) -> dict[str, SLOConfig]:
        configs: dict[str, SLOConfig] = {}
        for path in sorted(self._config_dir.glob("*.json")):
            with path.open("rb") as handle:
                configs_path_data = json.loads(handle.read().decode("utf-8"))
            configs[configs_path_data["service"]] = self._config_from_dict(
                configs_path_data
            )
        return configs

    def close(self) -> None:
        self._fsync_dir()

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(str(self.directory), os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # -- (de)serialization ----------------------------------------------

    @staticmethod
    def _observation_to_dict(obs: Observation) -> dict[str, Any]:
        return {
            "observation_id": obs.observation_id,
            "service": obs.service,
            "timestamp": obs.timestamp,
            "outcome": obs.outcome.value,
            "ingest_time": obs.ingest_time,
        }

    @staticmethod
    def _observation_from_dict(row: dict[str, Any]) -> Observation:
        return Observation(
            observation_id=row["observation_id"],
            service=row["service"],
            timestamp=float(row["timestamp"]),
            outcome=Outcome(row["outcome"]),
            ingest_time=row.get("ingest_time"),
        )

    @staticmethod
    def _config_to_dict(config: SLOConfig) -> dict[str, Any]:
        return {
            "service": config.service,
            "availability_target": config.availability_target,
            "availability_window_seconds": config.availability_window_seconds,
            "short_window_seconds": config.short_window_seconds,
            "long_window_seconds": config.long_window_seconds,
            "alert_short_burn": config.alert_short_burn,
            "alert_long_burn": config.alert_long_burn,
            "cooldown_seconds": config.cooldown_seconds,
            "budget_window_seconds": config.budget_window_seconds,
            "budget_expected_requests": config.budget_expected_requests,
        }

    @staticmethod
    def _config_from_dict(row: dict[str, Any]) -> SLOConfig:
        return SLOConfig(
            service=row["service"],
            availability_target=float(row["availability_target"]),
            availability_window_seconds=float(row["availability_window_seconds"]),
            short_window_seconds=float(row["short_window_seconds"]),
            long_window_seconds=float(row["long_window_seconds"]),
            alert_short_burn=float(row["alert_short_burn"]),
            alert_long_burn=float(row["alert_long_burn"]),
            cooldown_seconds=float(row["cooldown_seconds"]),
            budget_window_seconds=float(row["budget_window_seconds"]),
            budget_expected_requests=(
                float(row["budget_expected_requests"])
                if row.get("budget_expected_requests") is not None
                else None
            ),
        )

    @staticmethod
    def _budget_to_dict(budget: BudgetState) -> dict[str, Any]:
        return {
            "service": budget.service,
            "period_start": budget.period_start,
            "period_end": budget.period_end,
            "availability_target": budget.availability_target,
            "total_requests": budget.total_requests,
            "failures": budget.failures,
            "total_budget": budget.total_budget,
            "consumed": budget.consumed,
            "remaining": budget.remaining,
            "burn_fraction": budget.burn_fraction,
            "exhausted": budget.exhausted,
        }

    @staticmethod
    def _budget_from_dict(row: dict[str, Any]) -> BudgetState:
        return BudgetState(
            service=row["service"],
            period_start=float(row["period_start"]),
            period_end=row.get("period_end"),
            availability_target=float(row["availability_target"]),
            total_requests=int(row["total_requests"]),
            failures=int(row["failures"]),
            total_budget=float(row["total_budget"]),
            consumed=float(row["consumed"]),
            remaining=float(row["remaining"]),
            burn_fraction=(
                float(row["burn_fraction"])
                if row.get("burn_fraction") is not None
                else None
            ),
            exhausted=bool(row["exhausted"]),
        )

    def _window_burn_to_dict(self, burn: WindowBurn | None) -> dict[str, Any] | None:
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

    def _window_burn_from_dict(
        self, row: dict[str, Any] | None
    ) -> WindowBurn | None:
        if row is None:
            return None
        return WindowBurn(
            window_seconds=float(row["window_seconds"]),
            window_start=float(row["window_start"]),
            window_end=float(row["window_end"]),
            total=int(row["total"]),
            failures=int(row["failures"]),
            error_rate=row["error_rate"],
            burn_rate=row["burn_rate"],
        )

    def _alert_to_dict(self, alert: AlertStatus) -> dict[str, Any]:
        return {
            "service": alert.service,
            "state": alert.state.value,
            "short_window": self._window_burn_to_dict(alert.short_window),
            "long_window": self._window_burn_to_dict(alert.long_window),
            "last_fired_at": alert.last_fired_at,
            "cooldown_until": alert.cooldown_until,
            "breached": alert.breached,
            "suppressed": alert.suppressed,
            "detail": alert.detail,
        }

    def _alert_from_dict(self, row: dict[str, Any]) -> AlertStatus:
        return AlertStatus(
            service=row["service"],
            state=AlertConditionState(row["state"]),
            short_window=self._window_burn_from_dict(row.get("short_window")),
            long_window=self._window_burn_from_dict(row.get("long_window")),
            last_fired_at=row.get("last_fired_at"),
            cooldown_until=row.get("cooldown_until"),
            breached=bool(row["breached"]),
            suppressed=bool(row["suppressed"]),
            detail=row["detail"],
        )

    # Accessors used by the service during recovery.
    def budget_from_dict(self, row: dict[str, Any]) -> BudgetState:
        return self._budget_from_dict(row)

    def alert_from_dict(self, row: dict[str, Any]) -> AlertStatus:
        return self._alert_from_dict(row)
