"""FastAPI HTTP 接口。

主体身份通过请求头 ``X-Owner-Id`` 传递（本地单机场景的简单归属隔离方案）。
所有 JobRunnerError 经统一异常处理器输出结构一致、可区分的错误体：

    {"error": {"code": ..., "message": ..., "job_id": ...}}

- 任务不存在          404 JOB_NOT_FOUND
- 越权访问他人任务    403 PERMISSION_DENIED
- 幂等载荷冲突        409 IDEMPOTENCY_CONFLICT
- 终态非法操作        409 INVALID_STATE_TRANSITION
- 超出并发配额        429 QUOTA_EXCEEDED（queue=false 时的老行为）
- 排队容量已满        429 QUEUE_FULL（queue=true 且队列满）
- 参数/处理器不合法   422 INVALID_REQUEST
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header
from pydantic import BaseModel, Field

from .errors import JobRunnerError, MissingPrincipal
from .service import JobService


class SubmitRequest(BaseModel):
    handler: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(min_length=1)
    timeout: float | None = Field(default=None, gt=0)
    # true：并发占满时进入 QUEUED 排队等待；false（默认）：超额直接 429
    queue: bool = False


def _require_owner(x_owner_id: str | None) -> str:
    if not x_owner_id:
        raise MissingPrincipal("missing required header X-Owner-Id")
    return x_owner_id


def create_app(service: JobService | None = None) -> FastAPI:
    from fastapi.responses import JSONResponse

    from .builtin_handlers import register_builtins

    register_builtins()  # 幂等注册，保证开箱即用
    app = FastAPI(title="async-job-runner", version="0.1.0")
    svc = service or JobService()

    @app.exception_handler(JobRunnerError)
    async def _on_job_error(request: Any, exc: JobRunnerError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error": {
                    "code": exc.error_code,
                    "message": exc.message,
                    "job_id": exc.job_id,
                }
            },
        )

    @app.post("/jobs", status_code=201)
    async def submit_job(
        body: SubmitRequest,
        x_owner_id: str | None = Header(default=None),
    ) -> dict[str, Any]:
        owner = _require_owner(x_owner_id)
        job, replayed = svc.submit(
            owner,
            body.handler,
            body.payload,
            idempotency_key=body.idempotency_key,
            timeout=body.timeout,
            queue=body.queue,
        )
        snap = job.snapshot()
        snap["idempotent_replay"] = replayed
        return snap

    @app.get("/jobs/{job_id}")
    async def get_job(
        job_id: str,
        x_owner_id: str | None = Header(default=None),
    ) -> dict[str, Any]:
        owner = _require_owner(x_owner_id)
        return svc.get(owner, job_id)

    @app.get("/jobs")
    async def list_jobs(
        x_owner_id: str | None = Header(default=None),
    ) -> dict[str, Any]:
        owner = _require_owner(x_owner_id)
        return {"jobs": svc.list_jobs(owner)}

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(
        job_id: str,
        x_owner_id: str | None = Header(default=None),
    ) -> dict[str, Any]:
        owner = _require_owner(x_owner_id)
        return svc.cancel(owner, job_id)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    app.state.service = svc
    return app
