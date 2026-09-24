"""FastAPI 装配层：HTTP 接口、调用主体识别与错误映射。"""

from __future__ import annotations

import contextlib
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import JobError
from .manager import JobManager


class SubmitRequest(BaseModel):
    type: str = Field(..., description="任务类型，如 succeed/sleep/fail/crash")
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None)
    quota: int | None = Field(default=None, ge=1)
    timeout: float | None = Field(default=None, gt=0)


def _error_body(code: str, message: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def create_app(manager: JobManager | None = None) -> FastAPI:
    owned = manager is None
    app = FastAPI(title="async-job-runner", version="0.1.0")

    if manager is None:
        manager = JobManager()
    app.state.manager = manager

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            if owned:
                await manager.aclose()  # type: ignore[union-attr]

    app.router.lifespan_context = lifespan

    # ---- 统一错误映射：业务性错误 -> 各自 4xx，未预期错误 -> 500 ----

    @app.exception_handler(JobError)
    async def job_error_handler(_: Request, exc: JobError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content=_error_body(exc.code, exc.message),
        )

    @app.exception_handler(Exception)
    async def unexpected_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=_error_body("internal_error", str(exc) or repr(exc)),
        )

    def unauthenticated() -> JobError:
        err = JobError("missing or empty X-Principal header", code="unauthenticated")
        err.http_status = 401
        return err

    @app.post("/jobs")
    async def submit_job(
        body: SubmitRequest,
        x_principal: str | None = Header(default=None, alias="X-Principal"),
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        owner = x_principal
        if not owner:
            raise unauthenticated()
        key = body.idempotency_key or idempotency_key
        job, created = await app.state.manager.submit(
            owner=owner,
            job_type=body.type,
            payload=body.payload,
            idempotency_key=key or None,
            quota=body.quota,
            timeout=body.timeout,
        )
        # 新建 201；幂等重放 200，且不会触发第二次执行
        return JSONResponse(
            status_code=201 if created else 200,
            content={"job": job.to_view(), "replayed": not created},
        )

    @app.get("/jobs/{job_id}")
    async def get_job(
        job_id: str,
        x_principal: str | None = Header(default=None, alias="X-Principal"),
    ) -> dict[str, Any]:
        if not x_principal:
            raise unauthenticated()
        job = await app.state.manager.get(owner=x_principal, job_id=job_id)
        return {"job": job.to_view()}

    @app.post("/jobs/{job_id}/cancel")
    async def cancel_job(
        job_id: str,
        x_principal: str | None = Header(default=None, alias="X-Principal"),
    ) -> dict[str, Any]:
        if not x_principal:
            raise unauthenticated()
        job = await app.state.manager.cancel(owner=x_principal, job_id=job_id)
        return {"job": job.to_view()}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
