# async-job-runner-python

本地可运行的后台任务提交与执行服务（FastAPI + asyncio，无外部服务依赖）。
解决多主体并发提交、幂等去重、归属隔离、并发配额、取消与失败时的
任务状态一致性与可解释性。

## 快速开始

```bash
uv sync                       # 安装依赖（fastapi[standard]，测试只用标准库）
uv run uvicorn main:app --reload
```

所有接口通过请求头 `X-Principal: <主体名>` 标识调用主体。

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /jobs` | 提交任务（201 新建；幂等重放 200） |
| `GET  /jobs/{id}` | 查询自己的任务（404 不存在 / 403 越权，可区分） |
| `POST /jobs/{id}/cancel` | 请求取消自己的任务 |
| `GET  /health` | 健康检查 |

提交请求体：

```json
{
  "type": "sleep",
  "payload": {"seconds": 2},
  "idempotency_key": "optional-key",
  "quota": 4,
  "timeout": 10.0
}
```

`idempotency_key` 也可通过请求头 `Idempotency-Key` 传递。
内置任务类型：

- `succeed`：立即成功，返回 `{"value": payload.value}`；
- `sleep`：按 `payload.seconds` 休眠，协作式响应取消，用于并发/配额/超时测试；
- `fail`：确定性**业务失败**；
- `crash`：确定性**系统崩溃**（抛出 `RuntimeError`）。

示例：

```bash
curl -s -X POST localhost:8000/jobs -H 'X-Principal: alice' \
  -H 'Content-Type: application/json' \
  -d '{"type":"sleep","payload":{"seconds":1},"timeout":5}'
```

## 任务状态机

```
                 ┌──────────┐
   submit ─────▶ │ PENDING  │ ── 排队中被取消 ──▶ ┌───────────┐
                 └────┬─────┘                      │ CANCELLED │
                      │ 调度执行                   └───────────┘
                 ┌────▼─────┐   成功               （终态）
                 │ RUNNING  │ ──────────────────▶ ┌───────────┐
                 └────┬─────┘                      │ SUCCEEDED │
        业务失败/崩溃 │ 超时                        └───────────┘
                 ┌────▼─────┐                      （终态）
                 │  FAILED  │
                 └──────────┘                   ┌───────────┐
                  （终态，fail_reason:            │  CANCELLED│
                   business | crash | timeout）  └───────────┘
RUNNING 收到取消请求后：协作处理器在检查点抛取消信号，或本次执行结束，
最终落定 CANCELLED。
```

不变量：

1. `SUCCEEDED / FAILED / CANCELLED` 为终态，任何写入终态的路径都先检查
   当前状态，**终态不可回退、不可重复执行**（`attempt` 恒为 1）。
2. 对终态任务调用取消返回 `409 terminal_state`。
3. 所有状态变更都在同一事件循环线程、且持有同一把 `asyncio.Lock` 时完成，
   状态观察不需要额外加锁。

## 幂等语义

- 幂等键的作用域是 **(调用主体, key)**：不同主体使用相同键互不影响。
- 同键 **且 type 与 payload 完全一致**：返回同一个任务，HTTP 200 且
  `replayed=true`，无论原任务处于何种状态（包括终态）都**只执行一次**。
- 同键 **但 type/payload 不一致**：拒绝并返回 `409 idempotency_conflict`，
  错误信息说明原因；该提交不创建任务、不占额度。
- 不带幂等键的提交每次都新建任务。
- 并发同键提交由锁串行化：N 个并发请求中恰好 1 个新建，其余全部重放。

## 配额语义

- 每个主体有独立的并发运行额度，默认 4，可在每次提交时用 `quota` 覆盖
  （`quota` 是该主体的额度上限，不是“为本次任务预留 N 个槽位”）。
- 提交时若该主体运行数已达上限：返回 `429 quota_exceeded`，
  **不创建任务、不登记幂等键、不占用任何额度**。
- “检查额度 + 占用槽位”在同一临界区内完成，并发竞态下实际运行数
  **不可能超过上限**。
- 槽位恰好释放一次，释放路径只有两处：
  - 任务在 PENDING 窗口被取消：由 `cancel()` 释放（执行协程若随后启动，
    会发现终态直接返回，不会二次释放）；
  - 任务进入过 RUNNING：由执行协程的 `finally` 释放，
    成功 / 失败 / 取消 / 超时四条终态路径全覆盖。
- 任务不排队：超额即拒绝；调用方稍后或额度释放后重试即可。

## 取消语义

- 取消 PENDING 任务：立即终结为 CANCELLED，额度当场释放。
- 取消 RUNNING 任务：`cancel_requested` 置位并通过 `CancelToken` 通知；
  返回时任务可能仍是 RUNNING（取消是异步落定的），查询可观察到
  `cancel_requested=true`。协作式处理器（如内置 `sleep`）在检查点尽快停止；
  即使处理器自然结束，取消请求也优先于成功结果，最终状态为 CANCELLED。
- 任务**不会因取消而重试**（“在本次执行结束后确实停止”）。

## 错误分类规则

错误分两个层级，互不混淆：

| 类别 | 触发条件 | fail_reason（任务视图） | error_code（HTTP） | HTTP |
| --- | --- | --- | --- | --- |
| 业务性错误 | 处理器主动抛 `JobError`（如 `fail` 类型） | `business` | 处理器声明的 code | 任务 `FAILED`；提交类错误 4xx |
| 系统性错误-崩溃 | 处理器抛出非预期异常（如 `RuntimeError`） | `crash` | 异常类名 | 任务 `FAILED` |
| 系统性错误-超时 | 执行超过 `timeout` | `timeout` | `timeout` | 任务 `FAILED` |

请求类业务错误（任务本身不进入 FAILED，直接拒绝请求）：

| code | HTTP | 含义 |
| --- | --- | --- |
| `unauthenticated` | 401 | 缺少 `X-Principal` |
| `job_not_found` | 404 | 任务 ID 不存在 |
| `permission_denied` | 403 | 任务存在但属于其他主体（与 404 严格区分） |
| `idempotency_conflict` | 409 | 同键不同载荷 |
| `quota_exceeded` | 429 | 并发额度已满 |
| `terminal_state` | 409 | 对终态任务执行取消 |
| `invalid_request` | 400 | 未知任务类型 / 参数非法 |

失败任务的可解释字段：`status`、`fail_reason`、`error_code`、
`error_message` 同时保留，业务失败与系统失败可明确区分。

## 本地验证

```bash
# 全部 23 个用例，日志中打印关键输入(INPUT)与判定依据(ASSERT)
uv run python run_tests.py

# 重复 5 轮验证并发结论可重复
uv run python run_tests.py -v 5
```

测试覆盖：

- 并发提交（asyncio 20 路同键 / 线程池 32 路 HTTP 同键）只建一个任务、只执行一次；
- 幂等键一致重放 200、载荷冲突 409、键按主体隔离；
- 配额竞态（50 路并发、额度 5，恰好接受 5 个）、超额不占键不占槽、
  成功/失败/崩溃/超时/取消后槽位全部释放、无泄漏无重复释放；
- 越权 403 与不存在 404 可区分、跨主体取消被拒且任务继续运行；
- 取消 PENDING / RUNNING / 终态三种路径；超时与异常分类保留。

也可启动真实服务后用 curl 手工验证（见上）。

## 代码结构

```
jobrunner/
  errors.py    # 业务性错误 vs 系统性错误的完整类型体系
  models.py    # JobStatus 状态机、FailReason、Job 快照
  handlers.py  # CancelToken 与内置任务类型/处理器注册表
  manager.py   # JobManager：锁、幂等表、配额计数、调度与终态落定
  api.py       # FastAPI 路由、主体识别、错误码映射
tests/         # 服务层 + HTTP 层并发测试（标准库 unittest）
run_tests.py   # 带判定日志的测试入口
main.py        # uvicorn 入口
```
