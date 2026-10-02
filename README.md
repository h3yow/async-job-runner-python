# async-job-runner（本地后台任务提交与执行）

仅依赖项目已有依赖（FastAPI）与 Python 标准库（`threading` / 内存存储），
**无需任何外部服务**即可在本地运行的后台任务系统，面向多主体并发场景，
保证任务状态在重复提交、并发配额、取消、失败、超时下始终一致且可解释。

## 快速开始

```bash
# 安装依赖（项目已用 uv 管理）
uv sync

# 启动 HTTP 服务
uv run uvicorn main:app --reload

# 或直接运行测试（仅用标准库 unittest，无需 pytest）
.venv/bin/python -m unittest tests.test_jobrunner -v
```

调用时通过请求头 `X-Owner-Id` 标识调用主体（principal），用于归属隔离与配额。

### HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /jobs` | 提交任务（需 `X-Owner-Id`） |
| `GET /jobs/{id}` | 查询单个任务（404 不存在 / 403 越权，可区分） |
| `GET /jobs` | 列出属于自己的全部任务 |
| `POST /jobs/{id}/cancel` | 请求取消任务 |
| `GET /health` | 健康检查 |

提交请求体：

```json
{
  "handler": "sleep",
  "payload": {"seconds": 1},
  "idempotency_key": "order-123",
  "timeout": 5.0,
  "wait": false
}
```

- `wait`（可选，默认 `false`）：并发额度已满时的准入策略。
  - `false`（**老行为，保持不变**）：立即返回 `429 QUOTA_EXCEEDED`，不创建任务、不占额度。
  - `true`（**新增排队模式**）：任务先进入该主体的 FIFO 等待队列（状态 `QUEUED`，
    不占并发额度），额度腾出后**严格按提交先后**自动开始执行；
    若等待队列也达到容量上限，返回 `429 QUEUE_FULL`，同样不创建任务、不占任何额度或队列位。
- 队列容量按主体配置，默认 16；`set_queue_limit(owner, 0)` 可对某主体完全关闭排队能力。

内置处理器：`echo`、`sleep`（可被取消中断，`{"fail": true}` 时返回业务错误）、
`fail_biz`（业务失败）、`boom`（系统性失败）。自定义处理器只需
`register_handler("name", fn)`，签名为 `(payload: dict, cancel_event: Event) -> Any`。

### curl 示例

```bash
curl -s -X POST localhost:8000/jobs -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' \
  -d '{"handler":"sleep","payload":{"seconds":0.5},"idempotency_key":"k1"}'

curl -s localhost:8000/jobs/<job_id> -H 'X-Owner-Id: alice'
curl -s -X POST localhost:8000/jobs/<job_id>/cancel -H 'X-Owner-Id: alice'

# 额度紧张时排队等待（而不是立刻吃 429）
curl -s -X POST localhost:8000/jobs -H 'X-Owner-Id: alice' \
  -H 'Content-Type: application/json' \
  -d '{"handler":"sleep","payload":{"seconds":1},"idempotency_key":"k2","wait":true}'
# -> 201，额度满时返回体 status 为 QUEUED；腾出额度后自动变 RUNNING
```

## 任务状态机

```
                                                    提交成功（原子占用配额）
   PENDING（锁内瞬态，外部不可见） ──────────────────────────────▶ RUNNING
      │                                                             │
      │  wait=true 且额度已满                                         ├─ 处理器正常返回 ──▶ SUCCEEDED
      ▼                                                              ├─ JobExecutionError ─▶ FAILED(BUSINESS)
   QUEUED（外部可见：可查询、可取消；不占并发额度；超时不起算）          ├─ 未受控异常 ──────▶ FAILED(SYSTEM)
      │                                                              ├─ 超过 timeout ─────▶ FAILED(TIMEOUT)
      │              额度腾出，FIFO 提升（队首出列 + 原子占额）          └─ 协作式取消生效 ───▶ CANCELLED
      └────────────────────────────────────────────▶ RUNNING
      │
      └─ 等待中取消（原子出列）──────────────────────▶ CANCELLED
```

- `SUCCEEDED / FAILED / CANCELLED` 为**终态**：终态不可回退、不可重复执行，
  对终态任务再次取消返回 `409 INVALID_STATE_TRANSITION`，原终态保持不变。
- 所有状态流转由 `JobStore` 在同一把锁内按白名单 `ALLOWED_TRANSITIONS` 校验，
  并发竞态下只有一次转换能成功。

### 排队（QUEUED）语义

- **两种模式并存、互不影响**：`wait=false`（默认）维持「满了立即拒绝」的老行为；
  `wait=true` 进入等待。非等待提交在额度有空位时直接运行，**不会**因为队列里
  有人在等而被拦下；等待任务只在额度释放（成功/失败/超时/取消）时按 FIFO 提升。
- **严格 FIFO、不许插队**：提升在同一把存储锁内完成
  「队首出列 → `QUEUED→RUNNING` → 计数 +1 → 落 `started_at`」，
  因此真实进入处理器的顺序必然等于提交顺序。
- **等待中可查询**：`GET /jobs/{id}` / `GET /jobs` 能观察到明确的 `QUEUED` 状态。
- **等待中取消**：取消请求原子地「出列 + `QUEUED→CANCELLED`」，立即返回终态；
  被取消的任务永远不再被提升执行，**不占用也不释放任何并发额度**
  （等待从不占额度），其幂等键与任务记录保留，便于事后审计。
- **超时从真正开始执行起算**：等待期间根本不启动执行线程，`join(timeout)` 的计时
  起点是提升为 RUNNING 的时刻；排队等了多久都不会被计入执行超时。
- **队列容量**：`JobService.set_queue_limit(owner, n)`（默认 16，`0` 表示禁止排队）。
  队列满后的排队提交返回 `429 QUEUE_FULL`，不创建任务、不登记幂等键、
  不占额度也不占队列位——事后可用同一个幂等键重新提交。
- **既不空转也不超额**：`finish_running()` 在释放配额的同一把锁内立刻提升等待者，
  不会出现「已有空闲额度、队列却压着不动」；提升与计数同锁也保证实际在跑的任务数
  始终 `≤` 主体上限。执行线程的启动回调一律在存储锁**外**触发，避免持锁回调。
- 关停（`shutdown`）时仍在队列中的任务统一落 `CANCELLED`（它们从未占用额度）。

## 幂等语义

- 幂等域为 **(调用主体, `idempotency_key`)**，不同主体使用相同的键互不影响。
- 提交时对 `{handler, payload, timeout}` 做规范化 SHA-256 哈希（键序无关）。
  - **键相同且哈希一致**：返回**同一个任务 ID**，处理器只执行一次，
    响应中 `idempotent_replay=true`。
  - **键相同但哈希不一致**：拒绝提交，返回 `409 IDEMPOTENCY_CONFLICT`，
    错误信息同时给出既有哈希与新哈希前缀，原因可解释。
- 「幂等查重 → 任务创建 → 登记幂等键」在同一把存储锁内原子完成；
  20 个线程同时以相同键提交，最终只有 1 个任务、处理器只执行 1 次。

## 配额语义

- 每个主体有独立的并发上限（默认 1，可用 `JobService.set_quota` 调整）。
- **提交是原子的**：`JobStore.submit_job()` 在同一把锁内完成
  「幂等查重 → 查上限 → 计数 +1 → 任务以 RUNNING 落库」。
  - 超额时，`wait=false` 在任务创建**之前**被拒绝（`429 QUOTA_EXCEEDED`），
    **不创建任务、不占用任何额度**，也不存在「排队占坑」。
  - `wait=true` 时任务以 QUEUED 落库并追加到该主体 FIFO 队尾，
    **不占并发额度**；队列也满时返回 `429 QUEUE_FULL`，同样什么都不创建。
  - 竞态下实际并发不可能超过上限（测试以高频监控线程观测峰值，断言峰值 ≤ 上限）。
- 任务进入任一终态时，`finish_running()` 在同一把锁内完成
  「RUNNING→终态 + 计数 −1 + 按 FIFO 提升等待者」。成功、业务失败、系统失败、
  超时、运行中取消五条路径都会释放配额并立即唤醒队首；等待中取消不经过计数。
- 计数恒等式：`running_count(owner) == 该 owner 的 RUNNING 任务数`
  （QUEUED 不计入）。重复释放会触发 `AssertionError`（显式失败），
  而不是静默把计数减成负数。

## 取消与超时

- 取消事件在任务落库**之前**预注册（事件先于任务对其它线程可见），无注册窗口期竞态。
- **等待中取消**：任务处于 QUEUED 时，取消在同一把存储锁内原子完成
  「出列 + 落 CANCELLED」，调用立即得到终态；此后它绝不会被提升执行，
  且不占/不释放额度。若取消请求与提升恰好竞态（任务已变 RUNNING），
  自动退化为运行中协作式取消，语义仍然确定。
- 取消是**协作式**的：取消运行中的任务只置位一个 `threading.Event`，
  处理器在检查点（内置 `sleep` 每 20ms 检查一次）观测后抛出 `TaskCancelled`，
  本次执行结束时任务确实进入 `CANCELLED`，随后额度释放并唤醒队首。
- 终态优先级是确定的：**执行异常 > 取消 > 成功**。
  处理器若在被取消的同时仍正常返回，按取消收口，保证「取消了就是取消了」。
- **超时只覆盖实际执行窗口**：排队等待期间不启动线程、不计时，
  `timeout` 从任务被提升为 RUNNING 那一刻起算（`started_at` 在提升时落定）。
- 超时：执行线程 `join(timeout)` 到时后，任务**立即**落 `FAILED/TIMEOUT`
  并释放配额、提升队首；同时置位取消事件，协作良好的处理器会尽快停止
  （Python 无法强杀线程，迟到的执行结果一律丢弃，绝不覆盖终态）。

## 错误分类规则

| 类别 | 来源 | HTTP / 错误码 |
| --- | --- | --- |
| 业务错误 `BUSINESS` | 处理器主动抛 `JobExecutionError(category=BUSINESS)`；未知处理器名 | 任务 `FAILED`，`error_category=BUSINESS`；提交期未知处理器为 `422 INVALID_REQUEST` |
| 系统错误 `SYSTEM` | 处理器抛出未受控的任意异常 | 任务 `FAILED`，`error_category=SYSTEM`，`error_code=SYSTEM_UNCAUGHT_EXCEPTION` |
| 超时 `TIMEOUT` | 执行超过提交时指定的 `timeout` | 任务 `FAILED`，`error_category=TIMEOUT`，`error_code=JOB_TIMEOUT` |
| 任务不存在 | ID 不存在 | `404 JOB_NOT_FOUND` |
| 越权访问 | ID 存在但属于其他主体 | `403 PERMISSION_DENIED`（与 404 严格区分） |
| 幂等冲突 | 同键不同载荷 | `409 IDEMPOTENCY_CONFLICT` |
| 超额 | 超出主体并发上限（`wait=false`，默认） | `429 QUOTA_EXCEEDED` |
| 等待队列满 | 额度已满且 FIFO 队列达到容量上限（`wait=true`） | `429 QUEUE_FULL` |
| 非法状态流转 | 终态回退 / 重复执行 / 取消终态任务 | `409 INVALID_STATE_TRANSITION` |

业务错误与系统错误是任务快照中**不同的 `error_category` 值**，永不混同；
错误响应统一形如：

```json
{"error": {"code": "QUOTA_EXCEEDED", "message": "...", "job_id": null}}
```

## 本地验证方法

```bash
# 1) 单元 + 并发 + 排队 + HTTP 测试（可重复运行；日志打印关键输入与判定依据）
.venv/bin/python -m unittest tests.test_jobrunner tests.test_queue -v

# 2) 稳定性：连续多轮跑，验证竞态/交叠结论可重复
for i in $(seq 1 10); do
  .venv/bin/python -m unittest tests.test_jobrunner tests.test_queue 2>&1 | tail -1
done

# 3) 起真实服务手动验证
uv run uvicorn main:app --port 8000
```

原有用例（`tests/test_jobrunner.py`，15 个用例）覆盖：

- 成功路径与结果回传；
- 幂等重放（同 ID、只执行一次）、同键不同载荷冲突、**20 线程同键并发**；
- 配额超额拒绝且不占额度、**30 线程配额竞态峰值不超上限**、失败/取消释放额度；
- 404 与 403 可区分、跨主体取消被拒、幂等键按主体隔离；
- 取消运行中任务、终态任务不可再取消；
- 超时进入 `FAILED/TIMEOUT`、业务错误与系统错误分类不混同；
- HTTP 层 201/403/404/409/429 全链路；
- HTTP 排队端到端：QUEUED 可见、`429 QUEUE_FULL` 与直拒 `QUOTA_EXCEEDED` 并存、
  等待中取消立即终态、队列自动消化。

排队专项用例（`tests/test_queue.py`，11 个用例）覆盖：

- **准入顺序**：上限 2 占满后 3 个任务排队，用处理器内部真实进入时刻断言
  启动顺序恰为提交顺序，不许插队；直拒模式与排队模式并存互不影响；
- **等待中取消**：取消立即返回 `CANCELLED`、出队且永不执行、不触碰额度；
  QUEUED→CANCELLED 后再取消仍为 409；取消队首后剩余任务按序提升；
- **队列满**：`429 QUEUE_FULL`，不占额度/队列位、不登记幂等键，
  队列消化后用同一幂等键可重新提交；队列容量 0 等价于关闭排队；
- **超时语义**：等待 ~0.5s 不计入 0.25s 执行超时（任务成功），
  起执行后的真实超时仍进入 `FAILED/TIMEOUT`；
- **交叠峰值**：超时占位释放 + 等待中/运行中取消 + FIFO 准入密集交叠，
  监控线程以 1ms 粒度观测，断言峰值 ≤ 上限、三类终态正确、额度归零；
- **突发消化**：30 个排队任务峰值恰为上限（有空闲额度立即提升、不空转），
  全部成功且计数归零。

## 代码结构

```
jobrunner/
  models.py             # Job / JobStatus（含 QUEUED）/ 状态机白名单 / ErrorCategory
  errors.py             # 统一错误类型（HTTP 状态 + 错误码，含 QUEUE_FULL）
  store.py              # 内存存储 + 一把锁：原子提交/FIFO 队列/提升/终结/归属/配额
  handlers.py           # 处理器协议与注册表、TaskCancelled
  builtin_handlers.py   # echo / sleep / fail_biz / boom
  executor.py           # 执行线程、排队元数据暂存、提升后启动、协作式取消、超时收口
  service.py            # 对外门面：提交(wait)/查询/取消、配额与队列容量配置、载荷哈希
  api.py                # FastAPI 路由与统一异常处理（提交体含 wait）
main.py                 # uvicorn 入口
tests/test_jobrunner.py # 原有测试套件（标准库 unittest）+ HTTP 排队端到端
tests/test_queue.py     # 排队专项：FIFO / 等待取消 / 队列满 / 超时起算 / 交叠峰值
```
