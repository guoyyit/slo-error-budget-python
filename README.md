# SLO 可用性与错误预算燃尽跟踪

一套**完全本地可复现**的服务水平目标（SLO）监控能力：接收带时间戳与成败结果的请求观测事件，
按滚动窗口计算可用性与错误预算燃尽率，使用短/长双窗口燃尽阈值产生告警，并将观测、预算与告警
状态持久化到本地磁盘，进程崩溃重启后不丢失。无外部服务、无消息中间件、无真实凭据依赖。

## 功能与需求对照

| # | 需求 | 实现位置 |
|---|------|----------|
| 1 | 可用性目标 + 空窗口返回「未知」而非 0%/100% | `windows.compute_availability`，`models.AvailabilityStatus` |
| 2 | 长/短窗口燃尽率由**同一份观测**推导 | `windows.compute_window_burn`（同一 `observations` 列表） |
| 3 | 预算消耗/恢复；耗尽后**锁存**，仅显式重置或按窗口重算，边界确定 | `service.SLOService.reset_budget`、`_current_period_start` |
| 4 | 迟到/乱序观测；迟到不改已告警结果；重放不重复计入；拒绝原因可区分 | `service.ingest`、`models.RejectReason`、密封水位线 |
| 5 | 并发上报/查询；读到的是某个已提交集合上的自洽结果 | `service` 中的 `threading.RLock` + 先落盘后改内存 |
| 6 | 多窗口告警状态 + 静默窗口；同一持续条件不重复触发，冷却内可解释抑制 | `service._transition` 状态机、`models.AlertConditionState` |
| 7 | 本地持久化 + 崩溃恢复；未完成写入不算成功观测 | `storage.EventStore`（长度 + CRC 帧、`fsync`） |
| 8 | 自动化测试与文档 | `tests/`、本 README |

## 快速开始

```bash
uv sync                      # 安装依赖（仅 fastapi/uvicorn + pytest）
uv run pytest                # 运行全部测试
uv run python demo.py        # 端到端本地演示（内存管道 + 临时目录）
uv run python main.py        # 启动 HTTP API（127.0.0.1:8000）
```

打开 `http://127.0.0.1:8000/docs` 可查看交互式接口文档。数据目录由环境变量
`SLO_DATA_DIR` 控制（默认 `./data`）。

观测入口有两种：

- **直接调用**：`SLOService.ingest(Observation(...))`；
- **本地内存管道（回环）**：`slo_tracker.pipeline.InMemoryPipeline`，线程安全的进程内队列，
  `submit()` 入队、`drain()` 交付到服务，用于模拟本地回环/内存消息管道，无需外部 broker。

## 窗口语义（重要）

所有时间单位均为秒，时间戳是「事件时间」（事件发生时刻），与接收时刻 `ingest_time` 分开记录。

- 窗口统一为**半开区间 `[start, end)`**，评估时刻为 `now`。
  - 事件时间恰好等于窗口左边界计入；恰好等于右边界（`now`）不计入。
- **可用性窗口**：`[now - availability_window_seconds, now)`。
  - 可用性 = 成功数 / 总请求数。
  - 窗口内**没有任何请求时返回 `status = "unknown"`、`ratio = null`、`reason = "no_observations_in_window"`**，
    绝不会把空闲误报为 100% 或 0%。
- **短/长燃尽窗口**：`[now - short_window_seconds, now)` 与 `[now - long_window_seconds, now)`。
  - 燃尽率 = 实际错误率 / 允许错误率，其中允许错误率 = `1 - availability_target`。
  - 两个窗口都从**同一份已提交观测**推导，只是长度不同；短窗口用于快速告警，长窗口用于判断趋势。
  - 窗口内无观测时，`error_rate` 与 `burn_rate` 均为 `null`（未知）。
- **错误预算周期**：
  - 首个周期在 `configure` 时刻锚定；之后按 `budget_window_seconds` 的固定倍数滚动，
    因此旧/新预算边界是**确定的**：`period_start + k * budget_window_seconds`。
  - 预算总额 = `budget_expected_requests * (1 - availability_target)`；
    若 `budget_expected_requests` 为空，则退化为「按当前周期内实际观测请求数」派生预算。
  - 消耗 = 周期内失败请求数；`burn_fraction = consumed / total_budget`。

### 预算耗尽与恢复（需求 3）

- 当 `consumed >= total_budget` 时预算进入 `exhausted`，并**锁存**：
  即使随后全部成功、或跨过多个预算窗口，也**不会自动重置**，`period_start` 不滚动。
- 解除方式二选一：
  1. **显式重置**：`POST /services/{svc}/reset`（或 `reset_budget(svc, at=...)`），
     在给定时刻锚定一个全新周期，该时刻即确定的旧/新边界，耗尽锁存清除；
  2. **按配置窗口重算**：仅在**未耗尽**时，评估时刻进入下一个固定窗口会自动滚动到新周期。

### 迟到、乱序与去重（需求 4）

- `observation_id` 全局去重；重放同一观测返回 `reason = "duplicate"`（HTTP 409），不重复计入。
- 事件按事件时间落入对应窗口，**乱序但仍在接受范围内**的观测正常计入。
- 当告警触发时，会把当时的长窗口左边缘记为**密封水位线 watermark = now - long_window_seconds**，
  此后事件时间早于水位线的观测被拒绝，`reason = "too_late"`（HTTP 422），
  从而保证迟到的成败事件**无法改变已经告警的结果**。
- 未配置的服务返回 `unknown_service`（HTTP 404）。
- 拒绝判定顺序：先查重（`duplicate`），再查水位线（`too_late`），原因始终可区分。

### 多窗口告警与静默窗口（需求 6）

触发条件：短窗口燃尽率 `>= alert_short_burn` **且**长窗口燃尽率 `>= alert_long_burn`。

状态机（`AlertConditionState`）：

- `idle`：从未告警且未命中；
- `firing`：命中阈值，正在告警（同一持续条件只在开始时记录一次 `last_fired_at`，不重复触发）；
- `suppressed_cooldown`：条件曾解除、在静默窗口 `[last_fired_at, last_fired_at + cooldown_seconds)`
  内再次命中，被抑制，`detail` 说明冷却到何时；
- `resolved`：曾经告警但当前条件已不成立。

静默窗口在首次告警时开启，持续 `cooldown_seconds`。条件提前解除不会缩短静默窗口；
静默窗口过期后再次命中视为**新的事件**，可再次 `firing`。

### 持久化与崩溃恢复（需求 7）

数据目录下：

- `observations.jsonl`：每条已提交观测一行；
- `state.jsonl`：每次预算/告警状态转移一行（保留每个服务的最后一条）；
- `configs/<service>.json`：服务配置。

每条记录以 `长度.CRC.负载` 成帧，写入后 `flush + fsync` 才算**已提交**。
恢复时顺序读取，一旦遇到长度不符或 CRC 不匹配的尾部记录（崩溃导致的撕裂写）即停止并丢弃，
因此**未完成写入绝不会被当作成功观测**。

### 并发一致性（需求 5）

- 每个服务的所有读写由同一把 `threading.RLock` 保护。
- 上报采用「**先持久化落盘（持锁）→ 成功后再更新内存**」，落盘失败则内存不变，杜绝部分写入。
- 查询返回防御性拷贝，`availability / short_burn / long_burn / budget / alert`
  全部基于同一时刻、同一份已提交观测计算，是自洽快照。

## 配置项

`SLOConfig`（`POST /services` 请求体）：

| 字段 | 含义 |
|------|------|
| `service` | 服务名（主键） |
| `availability_target` | 可用性目标，`(0,1]`，如 0.99 |
| `availability_window_seconds` | 可用性滚动窗口长度 |
| `short_window_seconds` | 短燃尽窗口长度（快速告警） |
| `long_window_seconds` | 长燃尽窗口长度（趋势/预算判断） |
| `alert_short_burn` | 短窗口燃尽率阈值 |
| `alert_long_burn` | 长窗口燃尽率阈值（两个都满足才告警） |
| `cooldown_seconds` | 告警静默/冷却窗口长度 |
| `budget_window_seconds` | 错误预算固定周期长度 |
| `budget_expected_requests` | 周期内预期请求量；为空则按实际观测派生预算 |

## HTTP 接口

- `POST /services`：配置服务；
- `POST /services/{svc}/observe`：上报一条观测（202 接受；409 重复；422 迟到；404 未知服务）；
- `GET  /services/{svc}`：自洽快照（可用性、短/长燃尽、预算、告警）；
- `GET  /services/{svc}/availability`、`/budget`、`/alert`：分项查询；
- `POST /services/{svc}/reset`：显式重置预算周期；
- `GET  /health`：健康检查。

## 代码结构

```
slo_tracker/
  models.py     枚举与不可变数据类型
  clock.py      SystemClock / FakeClock（测试可确定性推进时间）
  windows.py    纯函数：窗口切片、可用性、燃尽率、预算、周期边界对齐
  storage.py    本地 JSONL 成帧持久化与崩溃恢复
  service.py    并发安全的核心服务（上报/查询/重置/告警状态机/恢复）
  pipeline.py   本地内存回环管道
  api.py        FastAPI 应用
tests/          覆盖迟到乱序、重复、预算耗尽恢复、并发、重启恢复、静默抑制、HTTP
demo.py         本地端到端演示
main.py         HTTP 入口
```
