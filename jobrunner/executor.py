"""任务执行器：为已占用配额的 RUNNING 任务启动执行线程，并驱动 FIFO 提升。

并发模型（仅依赖标准库 threading，无外部服务）：

1. 提交是原子的：service 通过 store.submit_job() 在一把锁内完成
   「幂等查重 -> 配额检查 -> 计数+1 -> 任务以 RUNNING 落库」。
   因此超额提交在任务被创建之前就被拒绝，绝不占用额度；
   running_count(owner) 恒等于该 owner 的 RUNNING 任务数。
2. wait=True 的排队提交在额度已满时以 QUEUED 落库（不占额度），
   其 (timeout, event) 暂存在执行器；任务被 store 按 FIFO 提升为
   RUNNING 时，store 的提升回调把 job_id 列表交回本执行器锁外启动线程。
3. 执行器为每个 RUNNING 任务启动一个 daemon 线程——线程数天然受配额约束。
4. 取消事件在任务对其它线程可见之前由 service 预注册，消除注册窗口期竞态。
5. 任务结束走 store.finish_running()，在同一把锁内完成
   「RUNNING→终态 + 配额-1 + 提升队首」，返回被提升的 job_id；
   本执行器在存储锁外接住并启动它们，保证无泄漏、无重复释放、无空转。
6. 取消是协作式的：仅置位 event。等待中取消由 store 原子出列，
   执行器只需丢弃其暂存元数据；运行中取消则由处理器在检查点观测后
   抛 TaskCancelled，本次执行结束时任务确实进入 CANCELLED。
7. 超时只对 RUNNING 时长计时：等待期间从不启动执行线程，
   join(timeout) 从任务真正开始执行时起算。
8. daemon 线程无法强杀：超时后同时置位 event，协作良好的处理器会尽快停止。
"""

from __future__ import annotations

import logging
import threading
from typing import Any

from .errors import JobExecutionError
from .handlers import TaskCancelled, get_handler
from .models import ErrorCategory, Job, JobStatus
from .store import JobStore

logger = logging.getLogger("jobrunner.executor")


class JobExecutor:
    def __init__(self, store: JobStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        # job_id -> 取消信号（service 在任务落库前预注册）
        self._cancel_events: dict[str, threading.Event] = {}
        # QUEUED 任务暂存：job_id -> (timeout, event)；提升启动后即移除
        self._queued: dict[str, tuple[float | None, threading.Event]] = {}
        self._handler_threads: set[threading.Thread] = set()
        self._shutdown = False
        # 提升回调注入 store：锁内只判定，回调在 store 锁外触发
        store._promote_callback = self._on_promoted

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def register_event(self, job_id: str) -> threading.Event:
        """在任务落库前预注册取消事件，避免取消早于注册而丢失。"""
        event = threading.Event()
        with self._lock:
            if self._shutdown:
                raise RuntimeError("executor is shutting down")
            self._cancel_events[job_id] = event
        return event

    def discard_event(self, job_id: str) -> None:
        """任务最终未能创建（如配额/队列拒绝）时回收预注册事件。"""
        with self._lock:
            self._cancel_events.pop(job_id, None)
            self._queued.pop(job_id, None)

    def mark_queued(
        self,
        job_id: str,
        timeout: float | None,
        event: threading.Event,
    ) -> None:
        """登记 QUEUED 任务的执行参数，等待被提升后启动（此刻不计时）。"""
        with self._lock:
            self._queued[job_id] = (timeout, event)

    def _on_promoted(self, job_ids: list[str]) -> None:
        """store 提升回调（已在存储锁外）：取出暂存参数并启动执行线程。"""
        resolved: list[tuple[str, float | None, threading.Event]] = []
        with self._lock:
            shutting = self._shutdown
            for job_id in job_ids:
                entry = self._queued.pop(job_id, None)
                if entry is None:
                    # 无暂存参数属于内部接线缺陷：不静默吞掉
                    logger.error("promoted job %s missing queued entry", job_id)
                    continue
                timeout, event = entry
                if shutting:
                    # 关停期被提升：直接终结为失败并释放配额，避免计数泄漏
                    self._finish_failure(
                        job_id,
                        ErrorCategory.SYSTEM,
                        "EXECUTOR_SHUTTING_DOWN",
                        "executor is shutting down",
                        event=event,
                    )
                    continue
                resolved.append((job_id, timeout, event))
        for job_id, timeout, event in resolved:
            job = self._store.get_raw(job_id)
            if job is None or job.status is not JobStatus.RUNNING:
                continue
            self._start_thread(job, timeout, event)

    def launch(
        self, job: Job, timeout: float | None, event: threading.Event
    ) -> None:
        """直接 RUNNING 落库的提交：立即启动执行线程。"""
        with self._lock:
            if self._shutdown:
                # 关停期：直接终结为失败并释放配额，避免计数泄漏
                self._finish_failure(
                    job.id,
                    ErrorCategory.SYSTEM,
                    "EXECUTOR_SHUTTING_DOWN",
                    "executor is shutting down",
                    event=event,
                )
                return
        self._start_thread(job, timeout, event)

    def _start_thread(
        self,
        job: Job,
        timeout: float | None,
        event: threading.Event,
    ) -> None:
        """创建并登记执行线程。调用前任务必须已 RUNNING、额度已占用。"""
        thread = threading.Thread(
            target=self._run,
            args=(job.id, timeout, event),
            name=f"job-{job.id[:8]}",
            daemon=True,
        )
        with self._lock:
            if self._shutdown:
                self._finish_failure(
                    job.id,
                    ErrorCategory.SYSTEM,
                    "EXECUTOR_SHUTTING_DOWN",
                    "executor is shutting down",
                    event=event,
                )
                return
            self._handler_threads.add(thread)
        logger.info(
            "launch job=%s owner=%s timeout=%s queued_wait=%.3fs",
            job.id[:8],
            job.owner,
            timeout,
            (job.started_at - job.created_at) if job.started_at else 0.0,
        )
        thread.start()

    def request_cancel(self, job_id: str) -> bool:
        """置位取消信号，可重复调用；事件已不存在（任务已终结）返回 False。"""
        with self._lock:
            event = self._cancel_events.get(job_id)
        if event is None:
            return False
        event.set()
        return True

    def discard_queued(self, job_id: str) -> None:
        """等待中任务已由 store 原子取消：清理其暂存参数（不触碰额度）。"""
        with self._lock:
            self._queued.pop(job_id, None)

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        with self._lock:
            self._shutdown = True
            queued_ids = list(self._queued.keys())
        # 所有仍在等待的任务立即落 CANCELLED（从未占用额度，无需释放）
        cancelled = self._store.cancel_all_queued()
        for job_id in set(queued_ids) | set(cancelled):
            event = self._cancel_events.get(job_id)
            if event is not None:
                event.set()
            self.discard_queued(job_id)
        if wait:
            with self._lock:
                threads = list(self._handler_threads)
            for t in threads:
                t.join(timeout)

    # ------------------------------------------------------------------ #
    # 执行
    # ------------------------------------------------------------------ #
    def _run(
        self,
        job_id: str,
        timeout: float | None,
        event: threading.Event,
    ) -> None:
        current = threading.current_thread()
        try:
            self._execute(job_id, timeout, event)
        finally:
            with self._lock:
                self._handler_threads.discard(current)
                self._cancel_events.pop(job_id, None)

    def _execute(
        self,
        job_id: str,
        timeout: float | None,
        event: threading.Event,
    ) -> None:
        job = self._store.get_raw(job_id)
        if job is None or job.status != JobStatus.RUNNING:
            # 竞态兜底：被重复执行或任务消失（正常路径不会发生）
            return

        handler_name = job.payload.get("handler", "")
        handler = get_handler(handler_name)
        if handler is None:
            self._finish_failure(
                job_id,
                ErrorCategory.BUSINESS,
                "HANDLER_NOT_FOUND",
                f"handler {handler_name!r} is not registered",
                event=event,
            )
            return

        outcome_box: dict[str, Any] = {}

        def _target() -> None:
            try:
                outcome_box["result"] = handler(
                    job.payload.get("arguments", {}), event
                )
            except BaseException as exc:  # 处理器的任何失败都必须被归类
                outcome_box["error"] = exc

        worker = threading.Thread(
            target=_target, name=f"job-worker-{job_id[:8]}", daemon=True
        )
        worker.start()
        worker.join(timeout)  # timeout=None 时表示等待到自然结束

        if worker.is_alive():
            # ---- 超时：立即终态 + 释放额度；请求处理器协作停止 ----
            event.set()
            self._finish_failure(
                job_id,
                ErrorCategory.TIMEOUT,
                "JOB_TIMEOUT",
                f"job exceeded timeout of {timeout}s",
                event=event,
            )
            return

        if "error" in outcome_box:
            exc = outcome_box["error"]
            if isinstance(exc, TaskCancelled):
                self._finish_cancelled(
                    job_id, str(exc) or "cancelled", event=event
                )
            elif isinstance(exc, JobExecutionError):
                # 处理器显式声明的业务错误
                self._finish_failure(
                    job_id,
                    exc.category,
                    exc.code,
                    exc.message,
                    event=event,
                )
            else:
                # 未受控异常一律归为系统性错误，绝不与业务错误混淆
                self._finish_failure(
                    job_id,
                    ErrorCategory.SYSTEM,
                    "SYSTEM_UNCAUGHT_EXCEPTION",
                    f"{type(exc).__name__}: {exc}",
                    event=event,
                )
            return

        # 成功返回。终态优先级确定性：执行异常 > 取消 > 成功。
        # 若取消信号在终态提交前置位，则本次执行按取消收口
        # （取消在本次执行结束时确实生效）。
        if event.is_set():
            self._finish_cancelled(
                job_id, "cancelled near completion", event=event
            )
            return
        self._finish_success(job_id, outcome_box.get("result"), event=event)

    # ------------------------------------------------------------------ #
    # 终态提交（全部走 store 的原子转换；终态不可回退、不重复执行）
    # ------------------------------------------------------------------ #
    def _finish_success(
        self, job_id: str, result: Any, *, event: threading.Event
    ) -> None:
        try:
            self._store.finish_running(job_id, JobStatus.SUCCEEDED, result=result)
        except Exception:
            # 已被终结（重复回调/竞态）：放弃结果，禁止回退
            pass

    def _finish_failure(
        self,
        job_id: str,
        category: ErrorCategory,
        code: str,
        message: str,
        *,
        event: threading.Event,
    ) -> None:
        try:
            self._store.finish_running(
                job_id,
                JobStatus.FAILED,
                error_category=category,
                error_code=code,
                error_message=message,
            )
        except Exception:
            pass

    def _finish_cancelled(
        self, job_id: str, message: str, *, event: threading.Event
    ) -> None:
        try:
            self._store.finish_running(
                job_id,
                JobStatus.CANCELLED,
                error_code="CANCELLED",
                error_message=message,
            )
        except Exception:
            pass
