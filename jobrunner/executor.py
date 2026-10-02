"""任务执行器：为已占用配额的 RUNNING 任务启动执行线程。

并发模型（仅依赖标准库 threading，无外部服务）：

1. 提交是原子的：service 通过 store.submit_job() 在一把锁内完成
   「幂等查重 -> 配额检查 -> 计数+1 -> 任务以 RUNNING 落库」。
   因此超额提交在任务被创建之前就被拒绝，绝不占用额度；
   running_count(owner) 恒等于该 owner 的 RUNNING 任务数。
2. 执行器为每个 RUNNING 任务启动一个 daemon 线程——线程数天然受配额约束。
3. 取消事件在任务对其它线程可见之前由 service 预注册，消除注册窗口期竞态。
4. 任务结束走 store.finish_running()，在同一把锁内完成
   「RUNNING→终态 + 配额-1」，保证无泄漏、无重复释放；迟到结果被丢弃。
5. 取消是协作式的：仅置位 event，处理器在检查点观测后抛 TaskCancelled，
   本次执行结束时任务确实进入 CANCELLED。
6. 超时：worker.join(timeout) 后任务立即落 FAILED/TIMEOUT 并释放配额；
   daemon 线程无法强杀，但同时置位 event，协作良好的处理器会尽快停止。
"""

from __future__ import annotations

import threading
from typing import Any

from .errors import JobExecutionError
from .handlers import TaskCancelled, get_handler
from .models import ErrorCategory, Job, JobStatus
from .store import JobStore


class JobExecutor:
    def __init__(self, store: JobStore) -> None:
        self._store = store
        self._lock = threading.Lock()
        # job_id -> 取消信号（service 在任务落库前预注册）
        self._cancel_events: dict[str, threading.Event] = {}
        self._handler_threads: set[threading.Thread] = set()
        self._shutdown = False

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
        """任务最终未能创建（如配额拒绝）时回收预注册事件。"""
        with self._lock:
            self._cancel_events.pop(job_id, None)

    def launch(
        self, job: Job, timeout: float | None, event: threading.Event
    ) -> None:
        thread = threading.Thread(
            target=self._run,
            args=(job.id, timeout, event),
            name=f"job-{job.id[:8]}",
            daemon=True,
        )
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
            self._handler_threads.add(thread)
        thread.start()

    def launch_admitted(self, jobs: list[Job]) -> None:
        """为准准入的排队任务启动执行线程。

        取消事件在提交时已预注册并一直保留，这里按 job_id 取回；
        超时使用任务创建时保存的 timeout，从准入（RUNNING）时刻起算。
        """
        for job in jobs:
            with self._lock:
                event = self._cancel_events.get(job.id)
            if event is None:
                # 事件缺失属于系统缺陷：终结为失败并释放刚占用的配额
                self._finish_failure(
                    job.id,
                    ErrorCategory.SYSTEM,
                    "MISSING_CANCEL_EVENT",
                    "cancel event missing for admitted job",
                    event=threading.Event(),
                )
                continue
            self.launch(job, job.timeout, event)

    def request_cancel(self, job_id: str) -> bool:
        """置位取消信号，可重复调用；事件已不存在（任务已终结）返回 False。"""
        with self._lock:
            event = self._cancel_events.get(job_id)
        if event is None:
            return False
        event.set()
        return True

    def shutdown(self, wait: bool = True, timeout: float | None = None) -> None:
        with self._lock:
            self._shutdown = True
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
    # 终态提交（全部走 store 的原子转换；终态不可回退、不重复执行。
    # 每次终结都会带回新准入的排队任务，由执行器立即启动，保证
    # 「额度一腾出、队首就开工」，且同时在跑的任务数绝不超过上限。）
    # ------------------------------------------------------------------ #
    def _finish_success(
        self, job_id: str, result: Any, *, event: threading.Event
    ) -> None:
        try:
            _, admitted = self._store.finish_running(
                job_id, JobStatus.SUCCEEDED, result=result
            )
        except Exception:
            # 已被终结（重复回调/竞态）：放弃结果，禁止回退
            return
        self.launch_admitted(admitted)

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
            _, admitted = self._store.finish_running(
                job_id,
                JobStatus.FAILED,
                error_category=category,
                error_code=code,
                error_message=message,
            )
        except Exception:
            return
        self.launch_admitted(admitted)

    def _finish_cancelled(
        self, job_id: str, message: str, *, event: threading.Event
    ) -> None:
        try:
            _, admitted = self._store.finish_running(
                job_id,
                JobStatus.CANCELLED,
                error_code="CANCELLED",
                error_message=message,
            )
        except Exception:
            return
        self.launch_admitted(admitted)
