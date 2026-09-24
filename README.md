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
  "timeout": 5.0
}
```

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
```

## 任务状态机

```
                提交成功（原子占用配额）
   （不存在外部可见的排队态）
 PENDING ───────────────────────────▶ RUNNING
    │                                   │
    │ （PENDING 仅为提交锁内的瞬态）       ├─ 处理器正常返回 ──▶ SUCCEEDED
    │                                   ├─ JobExecutionError ─▶ FAILED(BUSINESS)
    └─ （外部无法在 PENDING 观察到任务）   ├─ 未受控异常 ──────▶ FAILED(SYSTEM)
                                        ├─ 超过 timeout ─────▶ FAILED(TIMEOUT)
                                        └─ 协作式取消生效 ───▶ CANCELLED
```

- `SUCCEEDED / FAILED / CANCELLED` 为**终态**：终态不可回退、不可重复执行，
  对终态任务再次取消返回 `409 INVALID_STATE_TRANSITION`，原终态保持不变。
- 所有状态流转由 `JobStore.transition / finish_running` 在同一把锁内
  按白名单 `ALLOWED_TRANSITIONS` 校验，并发竞态下只有一次转换能成功。

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
  - 超额提交在任务创建**之前**被拒绝（`429 QUOTA_EXCEEDED`），
    **不创建任务、不占用任何额度**，也不存在「排队占坑」。
  - 竞态下实际并发不可能超过上限（测试以高频监控线程观测峰值，断言峰值 ≤ 上限）。
- 任务进入任一终态时，`finish_running()` 在同一把锁内完成
  「RUNNING→终态 + 计数 −1」。成功、业务失败、系统失败、超时、取消
  五条路径都会释放配额。
- 计数恒等式：`running_count(owner) == 该 owner 的 RUNNING 任务数`。
  重复释放会触发 `AssertionError`（显式失败），而不是静默把计数减成负数。

## 取消与超时

- 取消事件在任务落库**之前**预注册（事件先于任务对其它线程可见），无注册窗口期竞态。
- 取消是**协作式**的：取消运行中的任务只置位一个 `threading.Event`，
  处理器在检查点（内置 `sleep` 每 20ms 检查一次）观测后抛出 `TaskCancelled`，
  本次执行结束时任务确实进入 `CANCELLED`，随后额度释放。
- 终态优先级是确定的：**执行异常 > 取消 > 成功**。
  处理器若在被取消的同时仍正常返回，按取消收口，保证「取消了就是取消了」。
- 超时：执行线程 `join(timeout)` 到时后，任务**立即**落 `FAILED/TIMEOUT`
  并释放配额；同时置位取消事件，协作良好的处理器会尽快停止
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
| 超额 | 超出主体并发上限 | `429 QUOTA_EXCEEDED` |
| 非法状态流转 | 终态回退 / 重复执行 / 取消终态任务 | `409 INVALID_STATE_TRANSITION` |

业务错误与系统错误是任务快照中**不同的 `error_category` 值**，永不混同；
错误响应统一形如：

```json
{"error": {"code": "QUOTA_EXCEEDED", "message": "...", "job_id": null}}
```

## 本地验证方法

```bash
# 1) 单元 + 并发 + HTTP 测试（可重复运行；日志打印关键输入与判定依据）
.venv/bin/python -m unittest tests.test_jobrunner -v

# 2) 稳定性：连续多轮跑，验证竞态结论可重复
for i in $(seq 1 10); do
  .venv/bin/python -m unittest tests.test_jobrunner 2>&1 | tail -1
done

# 3) 起真实服务手动验证
uv run uvicorn main:app --port 8000
```

测试覆盖（`tests/test_jobrunner.py`，共 14 个用例）：

- 成功路径与结果回传；
- 幂等重放（同 ID、只执行一次）、同键不同载荷冲突、**20 线程同键并发**；
- 配额超额拒绝且不占额度、**30 线程配额竞态峰值不超上限**、失败/取消释放额度；
- 404 与 403 可区分、跨主体取消被拒、幂等键按主体隔离；
- 取消运行中任务、终态任务不可再取消；
- 超时进入 `FAILED/TIMEOUT`、业务错误与系统错误分类不混同；
- HTTP 层 201/403/404/409/429 全链路。

## 代码结构

```
jobrunner/
  models.py             # Job / JobStatus / 状态机白名单 / ErrorCategory
  errors.py             # 统一错误类型（HTTP 状态 + 错误码）
  store.py              # 内存存储 + 一把锁保证原子提交/终结/归属/配额
  handlers.py           # 处理器协议与注册表、TaskCancelled
  builtin_handlers.py   # echo / sleep / fail_biz / boom
  executor.py           # 执行线程、协作式取消、超时收口
  service.py            # 对外门面：提交/查询/取消、载荷哈希
  api.py                # FastAPI 路由与统一异常处理
main.py                 # uvicorn 入口
tests/test_jobrunner.py # 测试套件（标准库 unittest）
```
