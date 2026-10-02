"""线程安全的内存任务存储（归属校验、幂等索引、配额计数、FIFO 队列与原子提交）。

一致性约定：
- 任务表、幂等索引、配额计数、等待队列四类可变状态只在 self._lock 下读写。
- 提交（submit_job）在同一把锁内原子完成
  「幂等查重 -> 配额/队列检查 -> （计数+1 并以 RUNNING 落库 | 以 QUEUED 落库排队）
  -> 登记幂等键」：任何一步不满足（键冲突 / 超额且不允许排队 / 队列满），
  任务不会被创建、计数不增加、队列位置不占用，
  因此被拒的提交不占用任何额度；竞态下也不可能实际并发超出上限。
- 终结（finish_running）在同一把锁内原子完成
  「RUNNING→终态 + 计数-1 + 按 FIFO 准入队首（QUEUED→RUNNING + 计数+1）」，
  配合 running_count(owner) == RUNNING 任务数 这条恒等式，
  杜绝额度泄漏、重复释放，以及「有空闲额度却压着等待任务不动」。
- 排队取消（cancel_queued）在同一把锁内原子完成
  「QUEUED→CANCELLED + 移出队列」，排队不占用额度，取消也不触碰计数；
  被取消的等待任务永远不会被准入执行。
- 锁外不暴露可变 Job：查询统一返回 snapshot() 字典，避免读撕裂。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from typing import Any

from .errors import (
    InvalidStateTransition,
    JobNotFound,
    PermissionDenied,
)
from .models import ALLOWED_TRANSITIONS, Job, JobStatus

_DEFAULT_QUOTA = 1
_DEFAULT_QUEUE_CAPACITY = 100


class JobStore:
    def __init__(
        self,
        default_quota: int = _DEFAULT_QUOTA,
        default_queue_capacity: int = _DEFAULT_QUEUE_CAPACITY,
    ) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        # (owner, key) -> job_id
        self._idem_index: dict[tuple[str, str], str] = {}
        # owner -> 当前已占用配额数（恒等于该 owner 处于 RUNNING 的任务数）
        self._running_count: dict[str, int] = {}
        self._quotas: dict[str, int] = {}
        self._default_quota = default_quota
        # owner -> 等待中的任务 ID（FIFO，严格按提交先后，不许插队）
        self._queues: dict[str, deque[str]] = {}
        self._queue_caps: dict[str, int] = {}
        self._default_queue_capacity = default_queue_capacity

    # ------------------------------------------------------------------ #
    # 配额
    # ------------------------------------------------------------------ #
    def set_quota(self, owner: str, limit: int) -> None:
        if limit < 1:
            raise ValueError("quota limit must be >= 1")
        with self._lock:
            self._quotas[owner] = limit

    def get_quota(self, owner: str) -> int:
        with self._lock:
            return self._quotas.get(owner, self._default_quota)

    def running_count(self, owner: str) -> int:
        with self._lock:
            return self._running_count.get(owner, 0)

    # ------------------------------------------------------------------ #
    # 排队容量
    # ------------------------------------------------------------------ #
    def set_queue_capacity(self, owner: str, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("queue capacity must be >= 1")
        with self._lock:
            self._queue_caps[owner] = capacity

    def get_queue_capacity(self, owner: str) -> int:
        with self._lock:
            return self._queue_caps.get(owner, self._default_queue_capacity)

    def queued_count(self, owner: str) -> int:
        with self._lock:
            return len(self._queues.get(owner, ()))

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
        timeout: float | None = None,
        queue: bool = False,
    ) -> tuple[Job | None, str]:
        """原子提交。job_id 由调用方预生成（以便先注册取消事件）。

        返回 (job, verdict)：

        - ("created",   job) ：任务已创建并以 RUNNING 落库、配额已占用；
        - ("queued",    job) ：queue=True 且并发已满，任务以 QUEUED 落库排队，
                               不占用任何额度；
        - ("replay",    job) ：命中同主体同幂等键且载荷哈希一致，返回原任务；
        - ("conflict",  None)：同键已存在但载荷不一致；
        - ("quota",     None)：queue=False 且超出并发上限，什么都没创建；
        - ("queue_full",None)：queue=True 但排队容量已满，什么都没创建。

        幂等判定先于配额/排队判定：冲突/重放不涉及任何额度与队列位置。
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
                if not queue:
                    return None, "quota"
                waiting = self._queues.setdefault(owner, deque())
                cap = self._queue_caps.get(owner, self._default_queue_capacity)
                if len(waiting) >= cap:
                    # 队列满：拒绝，不创建任务、不占额度、不占队列位置
                    return None, "queue_full"
                job = Job(
                    id=job_id,
                    owner=owner,
                    idempotency_key=idem_key,
                    payload_hash=payload_hash,
                    payload=payload,
                    timeout=timeout,
                    status=JobStatus.QUEUED,
                )
                self._jobs[job.id] = job
                self._idem_index[index_key] = job.id
                waiting.append(job.id)
                return job, "queued"

            job = Job(
                id=job_id,
                owner=owner,
                idempotency_key=idem_key,
                payload_hash=payload_hash,
                payload=payload,
                timeout=timeout,
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
    # 排队准入与排队取消
    # ------------------------------------------------------------------ #
    def admit_ready(self, owner: str) -> list[Job]:
        """把该主体队列头部的等待任务按 FIFO 准入为 RUNNING。

        在同一把锁内完成「QUEUED→RUNNING + 计数+1 + started_at 起算」，
        因此超时从真正开始执行时起算，等待时长不计入执行超时。
        返回被准入的任务列表（调用方负责在锁外启动执行线程）。
        不变式：准入后 running_count 仍 <= 上限；只要队列非空且有空闲
        额度就必须准入，杜绝「有空闲额度却压着任务不动」。
        """
        admitted: list[Job] = []
        with self._lock:
            limit = self._quotas.get(owner, self._default_quota)
            waiting = self._queues.get(owner)
            if not waiting:
                return admitted
            used = self._running_count.get(owner, 0)
            while waiting and used < limit:
                job_id = waiting.popleft()
                job = self._jobs[job_id]
                if job.status is not JobStatus.QUEUED:
                    # 防御：队列中不应出现非 QUEUED 项（取消时已同步移除）
                    continue
                job.status = JobStatus.RUNNING
                job.started_at = time.time()
                used += 1
                self._running_count[owner] = used
                admitted.append(job)
        return admitted

    def cancel_queued(self, job_id: str) -> Job:
        """原子地「QUEUED→CANCELLED + 移出队列」。

        排队任务不占用额度，取消也不触碰计数；取消后立刻退出等待、
        永远不会被准入执行。任务不在 QUEUED（已准入/已终态）时抛
        InvalidStateTransition，调用方据此走运行中取消或拒绝。
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise JobNotFound(f"job {job_id!r} not found", job_id=job_id)
            if job.status is not JobStatus.QUEUED:
                raise InvalidStateTransition(
                    f"job {job_id!r} is not queued "
                    f"(current={job.status.value})",
                    job_id=job_id,
                )
            waiting = self._queues.get(job.owner)
            if waiting is not None:
                try:
                    waiting.remove(job_id)
                except ValueError:
                    pass
            job.status = JobStatus.CANCELLED
            job.finished_at = time.time()
            job.error_code = "CANCELLED"
            job.error_message = "cancelled while queued"
            return job

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
    ) -> tuple[Job, list[Job]]:
        """执行器专用：原子地「RUNNING→终态 + 释放一个配额 + 准入队首任务」。

        返回 (已终结任务, 本次释放额度后新准入的 QUEUED→RUNNING 任务列表)。
        终结与准入在同一把锁内完成：计数先 -1 再按 FIFO 补位，
        任意瞬间 running_count 都不会超过上限，额度不泄漏也不重复释放；
        只要队列里有等待任务且有空闲额度，补位必然发生。

        任务已不在 RUNNING（重复回调/竞态终结）时不触碰计数、不做准入并抛
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
            admitted = self.admit_ready(job.owner)
            return job, admitted
