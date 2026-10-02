"""对外服务门面：提交 / 查询 / 取消。

职责：
- 参数校验（handler 是否注册等提交期业务校验）；
- 规范化载荷并计算稳定哈希，用于幂等键冲突的可解释判定；
- 协调 store（原子提交/流转）与 executor（执行线程/取消信号）；
- 归属隔离全部下沉到 store.get_owned：不存在 404、越权 403。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from .executor import JobExecutor
from .errors import (
    IdempotencyConflict,
    InvalidRequest,
    InvalidStateTransition,
    QueueFull,
    QuotaExceeded,
)
from .handlers import get_handler
from .models import Job, JobStatus
from .store import JobStore


def canonical_payload_hash(
    handler: str, payload: dict[str, Any], timeout: float | None
) -> str:
    """对「处理器 + 载荷 + 超时」做规范化哈希。

    相同幂等键下，只有三者都一致才视为同一次提交；
    JSON sort_keys 保证键序差异不影响判定。
    """
    canonical = json.dumps(
        {"handler": handler, "arguments": payload, "timeout": timeout},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class JobService:
    def __init__(
        self,
        store: JobStore | None = None,
        executor: JobExecutor | None = None,
    ) -> None:
        self._store = store or JobStore()
        self._executor = executor or JobExecutor(self._store)

    @property
    def store(self) -> JobStore:
        return self._store

    def set_quota(self, owner: str, limit: int) -> None:
        self._store.set_quota(owner, limit)

    def set_queue_limit(self, owner: str, limit: int) -> None:
        """配置主体等待队列容量（0 表示禁止排队，仅保留直拒模式）。"""
        self._store.set_queue_limit(owner, limit)

    def submit(
        self,
        owner: str,
        handler: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        wait: bool = False,
    ) -> tuple[Job, bool]:
        """提交任务，返回 (job, replayed)。

        - 相同 (owner, idempotency_key) 且载荷一致：返回同一个任务，只执行一次；
        - 相同键但载荷不一致：抛 IdempotencyConflict（409）并说明差异；
        - wait=False（默认，老行为）：额度满立即抛 QuotaExceeded（429），
          不创建任务、不占额度；
        - wait=True（新增排队模式）：额度满时任务以 QUEUED 进入该主体 FIFO
          等待队列（不占并发额度、超时尚未起算），额度腾出后自动按提交先后
          开始执行；队列也满时抛 QueueFull（429），同样不创建任务、不占额度。
        """
        arguments = dict(payload or {})
        if not isinstance(idempotency_key, str) or not idempotency_key:
            raise InvalidRequest("idempotency_key must be a non-empty string")
        if get_handler(handler) is None:
            raise InvalidRequest(f"handler {handler!r} is not registered")

        stored_payload = {"handler": handler, "arguments": arguments}
        payload_hash = canonical_payload_hash(handler, arguments, timeout)

        # 先确定真实 ID 并预注册取消事件，使事件先于任务对其它线程可见，
        # 杜绝「任务已 RUNNING 但取消事件尚不存在」的窗口期。
        job_id = Job.new_id()
        event = self._executor.register_event(job_id)
        try:
            job, verdict = self._store.submit_job(
                job_id=job_id,
                owner=owner,
                idem_key=idempotency_key,
                payload=stored_payload,
                payload_hash=payload_hash,
                wait=wait,
            )
        except BaseException:
            self._executor.discard_event(job_id)
            raise

        if verdict == "replay":
            # 重放：预注册事件作废；返回既有任务，任务体只执行一次
            self._executor.discard_event(job_id)
            return job, True
        if verdict == "conflict":
            self._executor.discard_event(job_id)
            existing = self._store.find_idempotent(owner, idempotency_key)
            existing_hash = existing.payload_hash[:12] if existing else "n/a"
            raise IdempotencyConflict(
                "idempotency_key already used with a different request: "
                f"existing payload_hash={existing_hash}, "
                f"new payload_hash={payload_hash[:12]} "
                "(handler/arguments/timeout must match)"
            )
        if verdict == "quota":
            self._executor.discard_event(job_id)
            raise QuotaExceeded(
                f"principal {owner!r} exceeded concurrency limit "
                f"{self._store.get_quota(owner)} "
                f"(running={self._store.running_count(owner)}); "
                "request rejected without consuming any quota"
            )
        if verdict == "queue_full":
            self._executor.discard_event(job_id)
            raise QueueFull(
                f"principal {owner!r} concurrency limit "
                f"{self._store.get_quota(owner)} is fully used and wait queue "
                f"is full (queued={self._store.queued_count(owner)}, "
                f"queue_limit={self._store.get_queue_limit(owner)}); "
                "request rejected without consuming any quota or queue slot"
            )
        if verdict == "queued":
            # 排队：暂存执行参数，此刻不启动线程、不计时、不占额度
            assert job is not None and job.status is JobStatus.QUEUED
            self._executor.mark_queued(job.id, timeout, event)
            return job, False

        assert verdict == "created" and job is not None
        self._executor.launch(job, timeout, event)
        return job, False

    def get(self, owner: str, job_id: str) -> dict[str, Any]:
        return self._store.get_owned(job_id, owner).snapshot()

    def list_jobs(self, owner: str) -> list[dict[str, Any]]:
        return [j.snapshot() for j in self._store.list_owned(owner)]

    def cancel(self, owner: str, job_id: str) -> dict[str, Any]:
        """取消任务，返回发起取消后观察到的快照。

        - 不存在 -> JobNotFound(404)；他人任务 -> PermissionDenied(403)；
        - 终态任务不可取消 -> InvalidStateTransition(409)，终态保持不变；
        - 等待中（QUEUED）：原子出列并立即落 CANCELLED，永远不再被执行、
          不占也不释放任何并发额度，调用即可观察到终态；
        - 运行中：置位取消信号，处理器在检查点停止后落 CANCELLED。
          调用方可随后 get() 轮询确认终态。
        """
        job = self._store.get_owned(job_id, owner)  # 404/403 在此区分
        if job.is_terminal:
            raise InvalidStateTransition(
                f"job {job_id!r} already in terminal state {job.status.value}",
                job_id=job_id,
            )
        if job.status is JobStatus.QUEUED:
            # 原子地「出列 + QUEUED->CANCELLED」；竞态下若已被提升，
            # store 抛 InvalidStateTransition，则退化为运行中取消。
            try:
                cancelled = self._store.cancel_queued(job_id)
            except InvalidStateTransition:
                self._executor.request_cancel(job_id)
                return self._store.get_owned(job_id, owner).snapshot()
            self._executor.request_cancel(job_id)  # 置位信号，幂等无害
            self._executor.discard_queued(job_id)
            return cancelled.snapshot()
        self._executor.request_cancel(job_id)
        return self._store.get_owned(job_id, owner).snapshot()

    def shutdown(self) -> None:
        self._executor.shutdown()
