"""线程安全的内存任务存储（归属校验、幂等索引、配额计数与原子提交）。

一致性约定：
- 任务表、幂等索引、配额计数三类可变状态只在 self._lock 下读写。
- 提交（submit_job）在同一把锁内原子完成
  「幂等查重 -> 配额检查 -> 计数+1 -> 任务以 RUNNING 落库 + 登记幂等键」：
  任何一步不满足（键冲突 / 超额），任务不会被创建、计数不增加，
  因此超额/冲突的提交不占用任何额度；竞态下也不可能实际并发超出上限。
- 终结（finish_running）在同一把锁内原子完成「RUNNING→终态 + 计数-1」，
  配合 running_count(owner) == RUNNING 任务数 这条恒等式，
  杜绝额度泄漏与重复释放。
- 锁外不暴露可变 Job：查询统一返回 snapshot() 字典，避免读撕裂。
"""

from __future__ import annotations

import threading
import time
from typing import Any

from .errors import (
    InvalidStateTransition,
    JobNotFound,
    PermissionDenied,
)
from .models import ALLOWED_TRANSITIONS, Job, JobStatus

_DEFAULT_QUOTA = 1


class JobStore:
    def __init__(self, default_quota: int = _DEFAULT_QUOTA) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        # (owner, key) -> job_id
        self._idem_index: dict[tuple[str, str], str] = {}
        # owner -> 当前已占用配额数（恒等于该 owner 处于 RUNNING 的任务数）
        self._running_count: dict[str, int] = {}
        self._quotas: dict[str, int] = {}
        self._default_quota = default_quota

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
    ) -> tuple[Job | None, str]:
        """原子提交。job_id 由调用方预生成（以便先注册取消事件）。

        返回 (job, verdict)：

        - ("created", job) ：任务已创建并以 RUNNING 落库、配额已占用；
        - ("replay",  job) ：命中同主体同幂等键且载荷哈希一致，返回原任务；
        - ("conflict", None)：同键已存在但载荷不一致；
        - ("quota",    None)：超出该主体并发上限，什么都没创建。

        幂等判定先于配额判定：冲突/重放不涉及任何额度。
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
                return None, "quota"

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
    ) -> Job:
        """执行器专用：原子地「RUNNING→终态 + 释放一个配额」。

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
            return job
