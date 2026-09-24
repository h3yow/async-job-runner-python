"""领域模型与状态定义。"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """任务状态机：

    PENDING -> RUNNING -> SUCCEEDED
                       \\-> FAILED
              \\-> CANCELLED   （排队中被取消）
    RUNNING  -> CANCELLED      （运行中请求取消，在本次执行结束后落定）

    SUCCEEDED / FAILED / CANCELLED 为终态，不可回退、不可重复执行。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)


class FailReason(str, enum.Enum):
    """失败原因的可区分分类。"""

    BUSINESS = "business"      # 业务性错误：处理器抛 JobError
    CRASH = "crash"            # 系统性错误：处理器抛非预期异常
    TIMEOUT = "timeout"        # 系统性错误：执行超时


@dataclass
class Job:
    """一个后台任务的全部可观察状态。"""

    owner: str
    job_type: str
    payload: dict[str, Any]
    idempotency_key: str | None
    quota: int
    timeout: float
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: JobStatus = JobStatus.PENDING
    fail_reason: FailReason | None = None
    error_code: str | None = None
    error_message: str | None = None
    result: Any = None
    created_at: float = field(default_factory=time.monotonic)
    started_at: float | None = None
    finished_at: float | None = None
    cancel_requested: bool = False
    attempt: int = 0

    def to_view(self) -> dict[str, Any]:
        """对外可观察的状态快照（不暴露内部锁/协程等对象）。"""
        return {
            "id": self.id,
            "owner": self.owner,
            "type": self.job_type,
            "payload": self.payload,
            "idempotency_key": self.idempotency_key,
            "status": self.status.value,
            "fail_reason": self.fail_reason.value if self.fail_reason else None,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "result": self.result,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "cancel_requested": self.cancel_requested,
            "attempt": self.attempt,
        }
