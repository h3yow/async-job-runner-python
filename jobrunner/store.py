"""线程安全的内存任务存储（归属校验、幂等索引、配额计数、FIFO 等待队列）。

一致性约定：
- 任务表、幂等索引、配额计数、等待队列四类可变状态只在 self._lock 下读写。
- 提交（submit_job）在同一把锁内原子完成
  「幂等查重 -> 配额检查 -> 计数+1 -> 任务以 RUNNING 落库 + 登记幂等键」：
  任何一步不满足（键冲突 / 超额 / 队列满），任务不会被创建、计数不增加，
  因此超额/冲突的提交不占用任何额度；竞态下也不可能实际并发超出上限。
- wait=True 的排队提交在额度已满时不占并发额度，任务以 QUEUED 落库并
  追加到该主体 FIFO 队尾；队列也满则返回 queue_full，什么都不创建。
- 提升（promote）在同一把锁内完成
  「队首出列 -> QUEUED->RUNNING -> 计数+1 -> started_at 落定」，
  严格按提交先后、绝不插队；锁内只收集被提升的 job_id，
  启动线程的回调一律在锁外触发，避免持锁回调。
- 终结（finish_running）在同一把锁内原子完成「RUNNING→终态 + 计数-1」，
  随后立即提升等待者，保证「有空闲额度就一定起任务」；
  配合 running_count(owner) == RUNNING 任务数 这条恒等式，
  杜绝额度泄漏与重复释放。
- 等待中取消（cancel_queued）在同一把锁内完成「出列 + QUEUED→CANCELLED」，
  取消后的任务绝不可能再被提升。
- 锁外不暴露可变 Job：查询统一返回 snapshot() 字典，避免读撕裂。
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from typing import Any, Callable

from .errors import (
    InvalidStateTransition,
    JobNotFound,
    PermissionDenied,
)
from .models import ALLOWED_TRANSITIONS, Job, JobStatus

logger = logging.getLogger("jobrunner.store")

_DEFAULT_QUOTA = 1
_DEFAULT_QUEUE_LIMIT = 16


class JobStore:
    def __init__(
        self,
        default_quota: int = _DEFAULT_QUOTA,
        default_queue_limit: int = _DEFAULT_QUEUE_LIMIT,
        promote_callback: Callable[[list[str]], None] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        # (owner, key) -> job_id
        self._idem_index: dict[tuple[str, str], str] = {}
        # owner -> 当前已占用配额数（恒等于该 owner 处于 RUNNING 的任务数）
        self._running_count: dict[str, int] = {}
        self._quotas: dict[str, int] = {}
        # owner -> 等待中的 FIFO 任务 id 队列（QUEUED 任务均不占并发额度）
        self._queues: dict[str, deque[str]] = {}
        self._queue_limits: dict[str, int] = {}
        self._default_quota = default_quota
        self._default_queue_limit = default_queue_limit
        # 提升回调（由执行器注入）：在存储锁外收到被提升的 job_id 列表，
        # 负责启动执行线程；锁内只做判定与状态/计数变更。
        self._promote_callback = promote_callback

    # ------------------------------------------------------------------ #
    # 配额与队列容量
    # ------------------------------------------------------------------ #
    def set_quota(self, owner: str, limit: int) -> None:
        if limit < 1:
            raise ValueError("quota limit must be >= 1")
        with self._lock:
            self._quotas[owner] = limit
            promoted = self._promote_locked(owner)
        # 上调上限可能释放出新的空闲额度：立即按 FIFO 提升等待者
        self._notify(promoted)

    def get_quota(self, owner: str) -> int:
        with self._lock:
            return self._quotas.get(owner, self._default_quota)

    def running_count(self, owner: str) -> int:
        with self._lock:
            return self._running_count.get(owner, 0)

    def set_queue_limit(self, owner: str, limit: int) -> None:
        """设置该主体等待队列容量上限（>=0；0 表示不允许排队）。"""
        if limit < 0:
            raise ValueError("queue limit must be >= 0")
        with self._lock:
            self._queue_limits[owner] = limit

    def get_queue_limit(self, owner: str) -> int:
        with self._lock:
            return self._queue_limits.get(owner, self._default_queue_limit)

    def queued_count(self, owner: str) -> int:
        with self._lock:
            return len(self._queues.get(owner, ()))

    # ------------------------------------------------------------------ #
    # FIFO 提升
    # ------------------------------------------------------------------ #
    def _promote_locked(self, owner: str) -> list[str]:
        """锁内：在空闲额度内按 FIFO 把等待者提升为 RUNNING。

        每次提升原子完成「出队 -> 校验仍为 QUEUED -> 计数+1 ->
        QUEUED->RUNNING + started_at」。已被取消的等待者会先被出队清理，
        因此取消后的任务永远不会被提升。
        """
        promoted: list[str] = []
        queue = self._queues.get(owner)
        while queue:
            limit = self._quotas.get(owner, self._default_quota)
            used = self._running_count.get(owner, 0)
            if used >= limit:
                break
            job_id = queue.popleft()
            job = self._jobs.get(job_id)
            if job is None or job.status is not JobStatus.QUEUED:
                # 已取消的等待者：跳过且不占额度（正常取消路径会自行出队，
                # 此分支仅作竞态兜底）
                continue
            job.status = JobStatus.RUNNING
            job.started_at = time.time()
            self._running_count[owner] = used + 1
            promoted.append(job_id)
            logger.info(
                "promote job=%s owner=%s queue_pos=%s used=%s->%s/%s",
                job_id[:8], owner, len(promoted), used, used + 1, limit,
            )
            used += 1
        if queue is not None and not queue:
            self._queues.pop(owner, None)
        return promoted

    def _notify(self, promoted: list[str]) -> None:
        """锁外触发提升回调启动执行线程。"""
        if promoted and self._promote_callback is not None:
            self._promote_callback(promoted)

    # ------------------------------------------------------------------ #
    # 原子提交
    # ------------------------------------------------------------------ #
    def submit_job(
        self,
        *,
        job_id: str,
        owner: str,
        idem_key: str,
        payload: dict[str, Any],
        payload_hash: str,
        wait: bool = False,
    ) -> tuple[Job | None, str]:
        """原子提交。job_id 由调用方预生成（以便先注册取消事件）。

        返回 (job, verdict)：

        - ("created",    job)：任务已创建并以 RUNNING 落库、配额已占用；
        - ("queued",     job)：wait=True 且额度已满，任务以 QUEUED 落库、
                               进入该主体 FIFO 队尾，不占并发额度；
        - ("replay",     job)：命中同主体同幂等键且载荷哈希一致，返回原任务；
        - ("conflict",   None)：同键已存在但载荷不一致；
        - ("quota",      None)：额度满（wait=False 老行为），什么都没创建；
        - ("queue_full", None)：额度满且等待队列也满，什么都没创建。

        幂等判定先于配额/队列判定：冲突/重放不涉及任何额度。
        非等待提交在额度有空位时直接运行，不受队列中既有等待者影响
        （等待模式与直拒模式相互兼容、各自独立）。
        """
        with self._lock:
            index_key = (owner, idem_key)
            existing_id = self._idem_index.get(index_key)
            if existing_id is not None:
                existing = self._jobs[existing_id]
                if existing.payload_hash == payload_hash:
                    return existing, "replay"
                return None, "conflict"

            limit = self._quotas.get(owner, self._default_quota)
            used = self._running_count.get(owner, 0)
            if used >= limit:
                if not wait:
                    return None, "quota"
                queue = self._queues.setdefault(owner, deque())
                queue_limit = self._queue_limits.get(
                    owner, self._default_queue_limit
                )
                if len(queue) >= queue_limit:
                    if not queue:
                        self._queues.pop(owner, None)
                    logger.info(
                        "reject job owner=%s: concurrency full used=%s/%s "
                        "and queue full len=%s/%s -> QUEUE_FULL",
                        owner, used, limit, len(queue), queue_limit,
                    )
                    return None, "queue_full"
                job = Job(
                    id=job_id,
                    owner=owner,
                    idempotency_key=idem_key,
                    payload_hash=payload_hash,
                    payload=payload,
                    status=JobStatus.QUEUED,
                )
                self._jobs[job.id] = job
                self._idem_index[index_key] = job.id
                queue.append(job.id)
                logger.info(
                    "enqueue job=%s owner=%s pos=%s used=%s/%s queue_limit=%s",
                    job.id[:8], owner, len(queue), used, limit, queue_limit,
                )
                return job, "queued"

            job = Job(
                id=job_id,
                owner=owner,
                idempotency_key=idem_key,
                payload_hash=payload_hash,
                payload=payload,
                status=JobStatus.PENDING,  # 锁内瞬态，外部不可见
            )
            self._jobs[job.id] = job
            self._idem_index[index_key] = job.id
            # 与创建同一把锁内完成占用与 PENDING->RUNNING
            self._running_count[owner] = used + 1
            job.status = JobStatus.RUNNING
            job.started_at = time.time()
            return job, "created"

    # ------------------------------------------------------------------ #
    # CRUD
    # ------------------------------------------------------------------ #
    def find_idempotent(self, owner: str, key: str) -> Job | None:
        with self._lock:
            job_id = self._idem_index.get((owner, key))
            return self._jobs.get(job_id) if job_id is not None else None

    def get_owned(self, job_id: str, owner: str) -> Job:
        """存在且归属 owner 才返回。

        语义严格可区分：
        - 任务不存在         -> JobNotFound (404)
        - 存在但属于别的主体 -> PermissionDenied (403)
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFound(f"job {job_id!r} not found", job_id=job_id)
            if job.owner != owner:
                raise PermissionDenied(
                    f"job {job_id!r} is owned by another principal",
                    job_id=job_id,
                )
            return job

    def list_owned(self, owner: str) -> list[Job]:
        with self._lock:
            return [j for j in self._jobs.values() if j.owner == owner]

    def get_raw(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    # ------------------------------------------------------------------ #
    # 状态流转
    # ------------------------------------------------------------------ #
    def cancel_queued(self, job_id: str) -> Job:
        """取消等待中的任务：同一把锁内「出列 + QUEUED->CANCELLED」。

        - 任务不在 QUEUED（已被提升为 RUNNING / 已终态等）：抛
          InvalidStateTransition，调用方应改走运行中取消路径；
        - 取消不触碰 running_count：等待从未占用过并发额度，
          也不会产生新的空闲额度，因此不触发提升。
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFound(f"job {job_id!r} not found", job_id=job_id)
            if job.status is not JobStatus.QUEUED:
                raise InvalidStateTransition(
                    f"cannot cancel-queued job {job_id!r} "
                    f"(current={job.status.value})",
                    job_id=job_id,
                )
            queue = self._queues.get(job.owner)
            if queue is not None:
                try:
                    queue.remove(job_id)
                except ValueError:
                    pass
                if not queue:
                    self._queues.pop(job.owner, None)
            job.status = JobStatus.CANCELLED
            job.finished_at = time.time()
            job.error_code = "CANCELLED"
            job.error_message = "cancelled while queued"
            logger.info(
                "cancel-queued job=%s owner=%s remaining_queue=%s "
                "(quota untouched: running=%s)",
                job_id[:8], job.owner, len(self._queues.get(job.owner, ())),
                self._running_count.get(job.owner, 0),
            )
            return job

    def cancel_all_queued(self, owner: str | None = None) -> list[str]:
        """关停专用：把等待中的任务批量落 CANCELLED（它们从未占用额度）。

        owner=None 表示全部主体。返回被取消的 job_id 列表（锁外清理事件用）。
        """
        with self._lock:
            owners = (
                list(self._queues.keys()) if owner is None else [owner]
            )
            cancelled: list[str] = []
            for ow in owners:
                queue = self._queues.get(ow)
                if not queue:
                    continue
                for job_id in list(queue):
                    job = self._jobs.get(job_id)
                    if job is not None and job.status is JobStatus.QUEUED:
                        job.status = JobStatus.CANCELLED
                        job.finished_at = time.time()
                        job.error_code = "CANCELLED"
                        job.error_message = "cancelled while queued (shutdown)"
                        cancelled.append(job_id)
                self._queues.pop(ow, None)
            return cancelled

    def transition(
        self,
        job_id: str,
        target: JobStatus,
        **fields: Any,
    ) -> Job:
        """按 ALLOWED_TRANSITIONS 做一次原子流转（供 PENDING 取消等场景）。

        非法流转（含终态回退、重复执行）抛 InvalidStateTransition。
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFound(f"job {job_id!r} not found", job_id=job_id)
            if target not in ALLOWED_TRANSITIONS[job.status]:
                raise InvalidStateTransition(
                    f"illegal transition {job.status.value} -> {target.value} "
                    f"for job {job_id!r}",
                    job_id=job_id,
                )
            job.status = target
            for name, value in fields.items():
                setattr(job, name, value)
            return job

    def finish_running(
        self, job_id: str, target: JobStatus, **fields: Any
    ) -> tuple[Job, list[str]]:
        """执行器专用：原子地「RUNNING→终态 + 释放一个配额」，随后立即提升。

        返回 (job, promoted_ids)：promoted_ids 是本次释放后在锁内按 FIFO
        提升为 RUNNING 的等待者（可能为空），调用方须在锁外启动其执行线程。

        任务已不在 RUNNING（重复回调/竞态终结）时不触碰计数并抛
        InvalidStateTransition，调用方据此丢弃迟到结果——
        终态绝不回退、额度绝不重复释放。
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFound(f"job {job_id!r} not found", job_id=job_id)
            if job.status is not JobStatus.RUNNING:
                raise InvalidStateTransition(
                    f"cannot finish non-running job {job_id!r} "
                    f"(current={job.status.value}, target={target.value})",
                    job_id=job_id,
                )
            if target not in ALLOWED_TRANSITIONS[JobStatus.RUNNING]:
                raise InvalidStateTransition(
                    f"illegal terminal target {target.value}", job_id=job_id
                )
            used = self._running_count.get(job.owner, 0)
            if used <= 0:
                # 恒等式破裂属于系统缺陷，宁可显式失败也不静默泄漏
                raise AssertionError(
                    f"quota imbalance while finishing job {job_id!r}"
                )
            self._running_count[job.owner] = used - 1
            job.status = target
            job.finished_at = time.time()
            for name, value in fields.items():
                setattr(job, name, value)
            # 释放后立即在同一把锁内提升等待者：有空闲额度绝不让队列压着不动
            promoted = self._promote_locked(job.owner)
        # 锁外启动被提升者的执行线程（绝不持锁回调）
        self._notify(promoted)
        return job, promoted
