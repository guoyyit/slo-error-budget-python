# SLO Error-Budget Tracker (Python)

A fully local service-level-objective tracker. It ingests timestamped
request success/failure observations, computes availability and error-budget
burn rate over rolling windows, raises multi-window burn-rate alerts with
cooldown silencing, and persists every committed observation to a local
write-ahead log so state survives a process restart.

No external services, databases or credentials are required. Observation
input is simulated either over the local loopback HTTP API
(`uvicorn main:app`, bound to `127.0.0.1`) or via an in-memory pipe
(`python -m slo_tracker.simulation`).

## Architecture

```
observations ──► SloService ──► WAL (fsynced JSON lines + CRC32)
                 │   (single lock: atomic publish/query)
                 ├── EventLog            (one shared, idempotent, ordered source)
                 ├── windows.burn_rate   (long + short rolling windows)
                 ├── budget.compute      (tumbling budget periods + latch)
                 └── alerts.evaluate     (multi-window rules + cooldown freeze)
```

| Module | Responsibility |
| --- | --- |
| `slo_tracker/models.py` | Immutable value objects and state enums |
| `slo_tracker/config.py` | `ServiceSLO`, `BurnRateRule`, `StoreConfig` |
| `slo_tracker/clock.py` | `SystemClock` / deterministic `ManualClock` |
| `slo_tracker/eventlog.py` | Single shared source of observations, idempotent by `event_id`, kept timestamp-sorted |
| `slo_tracker/windows.py` | Rolling-window availability and burn rate |
| `slo_tracker/budget.py` | Deterministic tumbling budget periods |
| `slo_tracker/alerts.py` | Multi-window alert state machine with cooldown freeze |
| `slo_tracker/persistence.py` | Append-only CRC-protected, fsynced WAL and crash recovery |
| `slo_tracker/service.py` | Thread-safe facade: validation, ingestion, latching, queries |
| `slo_tracker/api.py` | FastAPI loopback endpoints |
| `slo_tracker/simulation.py` | Deterministic in-memory pipe demo (healthy → incident → recovery) |

## Window semantics

* A query at evaluation time `t` with window length `W` covers event
  timestamps in the **half-open range `(t - W, t]`**.
* **Availability** = successes / total requests in the window.
* **Burn rate** = (observed failure fraction) / (allowed failure fraction),
  where allowed failure fraction = `1 - target_availability`. A burn rate
  of `1.0` consumes the error budget exactly in pace with the SLO;
  `14.4` means budget is consumed 14.4x too fast.
* **Empty windows are `unknown`**, never 100% or 0% available. The response
  carries `"state": "unknown"` with `availability` and `burn_rate` set to
  `null`, plus the raw `total`/`failures` counts.
* **Long and short windows derive from the same committed `EventLog`**, so
  they can never disagree about the underlying observations. The short
  window drives fast alerting; the long window guards budget exhaustion and
  page fatigue.

## Error-budget semantics

* Budget is tracked over deterministic **tumbling periods** anchored at the
  Unix epoch: period `k` is `[k*budget_period_seconds, (k+1)*budget_period_seconds)`
  (`period_index`, `period_start`, `period_end` are exposed in every snapshot).
* Allowed failures in a period =
  `(1 - target_availability) * requests_in_period`.
* Once failures exceed the allowance the period is **latched as
  `exhausted`** and does **not** auto-reset while the period is open, even
  if later successes dilute the ratio.
* The latch is cleared in exactly two ways:
  1. **Explicit reset** — `POST /services/{name}/budget/reset` marks the
     current period `reset` (persisted).
  2. **Period rollover** — a new period is recomputed only from observations
     inside its fixed boundaries; old failures never leak across a boundary.

## Late, out-of-order and duplicate observations

Every ingestion returns a status with an explanatory `detail`:

| Status | Meaning |
| --- | --- |
| `accepted` | Durably committed and published |
| `duplicate` | The `event_id` is already committed; replaying it is never counted twice |
| `late_outside_retention` | Event age exceeds `max(long_window, budget_period)`; it can no longer affect any window or period |
| `future_event` | Timestamp is more than `max_future_skew_seconds` ahead of the clock |
| `unknown_service` | The service is not configured |

Out-of-order events within retention are accepted and ordered by timestamp.
An already emitted alert result is **frozen for the whole cooldown
interval**: late-arriving failures cannot re-trigger a notification, and
late-arriving successes cannot revoke the raised alert in that interval.

## Alerting semantics

A rule fires only when **both** the long and the short window burn rates are
at or above their thresholds at the same evaluation. State machine:

```
inactive ─both windows breach→ firing (one notification)
firing/suppressed during cooldown → frozen as suppressed:
    • condition still true  → "repeat hit silenced"
    • condition false (incl. due to late data) → "frozen until cooldown"
after cooldown:
    condition false → resolved
    condition true  → firing (genuinely new notification)
```

`reason` on every snapshot distinguishes these cases. `last_fired_at` and
`cooldown_until` make the suppression window fully explainable.

## Concurrency and consistency

One re-entrant lock serializes ingestion (`WAL fsync → in-memory publish`)
and all queries. Each query therefore observes state derived from one fully
committed observation set — partial writes are never visible, and
long/short/budget/alert figures within a query are mutually consistent.

## Durability and crash recovery

* WAL record format: `<crc32>:<compact-json>\n`; every append is flushed and
  `fsync`ed before `accepted` is returned.
* On startup the WAL prefix is validated record by record. The first record
  with a bad/missing checksum (including a torn, newline-less partial write
  from a mid-write crash) and everything after it is discarded and the file
  truncated there. An unfinished write can therefore never be recovered as a
  committed observation.
* Replaying the WAL restores observations, per-period budget
  exhausted/reset markers and alert lifecycle state.

## Configuration (`slo_tracker/config.py`)

`ServiceSLO`

| Field | Meaning |
| --- | --- |
| `name` | Service identifier used in URLs and observations |
| `target_availability` | In `(0, 1)`, e.g. `0.999` |
| `long_window_seconds` / `short_window_seconds` | Rolling analysis windows; short ≤ long |
| `budget_period_seconds` | Tumbling error-budget period length |
| `max_future_skew_seconds` | Tolerance for future-dated events |
| `rules` | Tuple of `BurnRateRule` |

`BurnRateRule`

| Field | Meaning |
| --- | --- |
| `name` | Rule name |
| `long_window_seconds` / `short_window_seconds` | Per-rule windows (short ≤ long) |
| `long_burn_rate_threshold` / `short_burn_rate_threshold` | Both must be breached simultaneously |
| `cooldown_seconds` | Interval during which one sustained condition notifies once |

`StoreConfig`

| Field | Meaning |
| --- | --- |
| `data_dir` | Directory holding `wal.log` |
| `fsync` | Durability flag (default `true`; set `false` only for tests) |

## HTTP API (local loopback)

```
POST /observations
GET  /services/{name}/burn-rate
GET  /services/{name}/budget
POST /services/{name}/budget/reset
GET  /services/{name}/alerts
```

Rejected observations return HTTP 202 with `"accepted": false` and a
machine-readable `status`/`detail`; querying an unknown service returns 404.

## Running locally

```bash
uv sync

# in-memory deterministic demo (healthy traffic, incident, suppression, recovery)
uv run python -m slo_tracker.simulation

# loopback HTTP service (WAL in ./.data, override with SLO_DATA_DIR)
uv run uvicorn main:app --host 127.0.0.1 --port 8000

# tests
uv run pytest
```
