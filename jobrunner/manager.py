"""JobManager：任务提交、查询、取消与并发配额调度的核心。

并发模型：单进程 asyncio，所有状态变更都在事件循环线程内、
且经由同一把 asyncio.Lock 串行化，因此：
- 配额检查与占用是原子的，并发竞态下不会超额；
- 额度只在两处之一释放恰好一次：cancel() 把 PENDING 直接终结时，
  或 _run() 真正开始执行后的 finally 中；
- 终态写入前统一检查 TERMINAL_STATUSES，终态不可回退、不可重复执行。
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Any

from .errors import (
    IdempotencyConflictError,
    InvalidJobRequestError,
    JobError,
    JobNotFoundError,
    JobPermissionDeniedError,
    JobTerminalStateError,
    QuotaExceededError,
)
from .handlers import CancelledByUser, CancelToken, get_handler
from .models import FailReason, Job, JobStatus, TERMINAL_STATUSES

DEFAULT_TIMEOUT = 30.0


class JobManager:
    def __init__(self, default_quota: int = 4) -> None:
        if default_quota < 1:
            raise ValueError("default_quota must be >= 1")
        self._default_quota = default_quota
        self._lock = asyncio.Lock()
        self._jobs: dict[str, Job] = {}
        # (owner, idempotency_key) -> job_id
        self._idem: dict[tuple[str, str], str] = {}
        # owner -> 当前运行中的任务数
        self._running: dict[str, int] = defaultdict(int)
        # job_id -> 取消令牌 / 执行协程
        self._tokens: dict[str, CancelToken] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}

    # ---------------- 提交 ----------------

    async def submit(
        self,
        *,
        owner: str,
        job_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        quota: int | None = None,
        timeout: float | None = None,
    ) -> tuple[Job, bool]:
        """提交任务，返回 (任务, 是否新建)。幂等命中时返回已有任务与 False。"""
        if not owner:
            raise InvalidJobRequestError("owner must be a non-empty string")
        if not isinstance(payload, dict):
            raise InvalidJobRequestError("payload must be an object")
        get_handler(job_type)  # 未知类型直接拒绝，不占用任何状态
        effective_quota = quota if quota is not None else self._default_quota
        if effective_quota < 1:
            raise InvalidJobRequestError("quota must be >= 1")
        effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
        if effective_timeout <= 0:
            raise InvalidJobRequestError("timeout must be > 0")

        async with self._lock:
            if idempotency_key is not None:
                existing_id = self._idem.get((owner, idempotency_key))
                if existing_id is not None:
                    existing = self._jobs[existing_id]
                    if existing.job_type == job_type and existing.payload == payload:
                        return existing, False  # 幂等命中：同一个任务，只执行一次
                    raise IdempotencyConflictError(
                        f"idempotency key {idempotency_key!r} already used with a "
                        f"different payload/type by owner {owner!r}"
                    )

            if self._running[owner] >= effective_quota:
                # 超额拒绝：不创建任务、不登记幂等键、不占用任何额度
                raise QuotaExceededError(
                    f"owner {owner!r} already has {self._running[owner]} running "
                    f"job(s), quota is {effective_quota}"
                )

            job = Job(
                owner=owner,
                job_type=job_type,
                payload=payload,
                idempotency_key=idempotency_key,
                quota=effective_quota,
                timeout=effective_timeout,
            )
            self._jobs[job.id] = job
            if idempotency_key is not None:
                self._idem[(owner, idempotency_key)] = job.id
            self._running[owner] += 1
            token = CancelToken()
            self._tokens[job.id] = token
            self._tasks[job.id] = asyncio.create_task(
                self._run(job, token), name=f"job-{job.id[:8]}"
            )
            return job, True

    # ---------------- 查询 ----------------

    async def get(self, *, owner: str, job_id: str) -> Job:
        """查询任务；不存在 -> JobNotFoundError，越权 -> JobPermissionDeniedError。"""
        async with self._lock:
            return self._lookup(owner, job_id)

    def _lookup(self, owner: str, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise JobNotFoundError(f"job {job_id!r} does not exist")
        if job.owner != owner:
            raise JobPermissionDeniedError(
                f"job {job_id!r} belongs to another owner"
            )
        return job

    # ---------------- 取消 ----------------

    async def cancel(self, *, owner: str, job_id: str) -> Job:
        """取消任务。

        - PENDING：直接终结为 CANCELLED，并在此释放额度（_run 不会执行）；
        - RUNNING：置取消标记并通知协作令牌，本次执行结束后落定 CANCELLED，
          额度由 _run 的 finally 释放；
        - 终态：拒绝，抛 JobTerminalStateError。
        """
        async with self._lock:
            job = self._lookup(owner, job_id)
            if job.status in TERMINAL_STATUSES:
                raise JobTerminalStateError(
                    f"job {job_id!r} is already in terminal state "
                    f"{job.status.value}, cannot cancel"
                )
            job.cancel_requested = True
            if job.status == JobStatus.PENDING:
                job.status = JobStatus.CANCELLED
                job.finished_at = time.monotonic()
                self._release_slot(job)
                task = self._tasks.pop(job.id, None)
                if task is not None:
                    task.cancel()  # 协程可能尚未开始；_run 启动时也会看到终态直接返回
            else:
                token = self._tokens.get(job_id)
                if token is not None:
                    token.request()
            return job

    # ---------------- 执行 ----------------

    async def _run(self, job: Job, token: CancelToken) -> None:
        acquired = False
        try:
            async with self._lock:
                if job.status != JobStatus.PENDING:
                    return  # 提交后、调度前已被取消；额度已由 cancel() 释放
                job.status = JobStatus.RUNNING
                job.started_at = time.monotonic()
                job.attempt += 1
                acquired = True

            handler = get_handler(job.job_type)
            try:
                result = await asyncio.wait_for(
                    handler(job.payload, token), timeout=job.timeout
                )
            except CancelledByUser:
                await self._finish(job, JobStatus.CANCELLED)
            except asyncio.CancelledError:
                await self._finish(job, JobStatus.CANCELLED)
                raise
            except asyncio.TimeoutError:
                await self._finish(
                    job,
                    JobStatus.FAILED,
                    fail_reason=FailReason.TIMEOUT,
                    error_code="timeout",
                    error_message=f"exceeded timeout of {job.timeout}s",
                )
            except JobError as exc:
                # 业务性错误：处理器主动声明的失败
                if job.cancel_requested:
                    await self._finish(job, JobStatus.CANCELLED)
                else:
                    await self._finish(
                        job,
                        JobStatus.FAILED,
                        fail_reason=FailReason.BUSINESS,
                        error_code=exc.code,
                        error_message=exc.message,
                    )
            except Exception as exc:  # noqa: BLE001 - 系统性崩溃必须归类保留
                await self._finish(
                    job,
                    JobStatus.FAILED,
                    fail_reason=FailReason.CRASH,
                    error_code=type(exc).__name__,
                    error_message=str(exc),
                )
            else:
                # 取消请求优先于成功结果：确保“取消后确实停止”
                if job.cancel_requested:
                    await self._finish(job, JobStatus.CANCELLED)
                else:
                    await self._finish(job, JobStatus.SUCCEEDED, result=result)
        finally:
            if acquired:
                async with self._lock:
                    self._release_slot(job)

    async def _finish(
        self,
        job: Job,
        status: JobStatus,
        *,
        fail_reason: FailReason | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        result: Any = None,
    ) -> None:
        async with self._lock:
            if job.status in TERMINAL_STATUSES:
                return  # 终态不可回退、不可重复执行
            job.status = status
            job.fail_reason = fail_reason
            job.error_code = error_code
            job.error_message = error_message
            job.result = result
            job.finished_at = time.monotonic()

    def _release_slot(self, job: Job) -> None:
        """释放额度（调用方须持有锁；每个任务恰好释放一次）。"""
        self._running[job.owner] -= 1
        if self._running[job.owner] <= 0:
            del self._running[job.owner]
        self._tokens.pop(job.id, None)
        self._tasks.pop(job.id, None)

    # ---------------- 生命周期 ----------------

    async def aclose(self) -> None:
        """取消所有尚未结束的执行（测试与服务关闭时使用）。"""
        async with self._lock:
            tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
