"""任务领域模型与状态机定义。"""

from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


class JobStatus(str, enum.Enum):
    """任务状态枚举。终态：SUCCEEDED / FAILED / CANCELLED。"""

    PENDING = "PENDING"        # 已提交，等待调度（可能在等待配额）
    RUNNING = "RUNNING"        # 已占用配额，正在执行
    SUCCEEDED = "SUCCEEDED"    # 终态：成功
    FAILED = "FAILED"          # 终态：失败（含超时）
    CANCELLED = "CANCELLED"    # 终态：已取消


TERMINAL_STATUSES = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
)

# 合法状态流转（骨架：仅定义，不实现校验逻辑）
ALLOWED_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.PENDING: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}
    ),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.FAILED: frozenset(),
    JobStatus.CANCELLED: frozenset(),
}


class ErrorCategory(str, enum.Enum):
    """错误分类骨架。"""

    BUSINESS = "BUSINESS"   # 业务性错误（任务载荷/处理器自身抛出）
    SYSTEM = "SYSTEM"       # 系统性错误（基础设施、处理器崩溃）
    TIMEOUT = "TIMEOUT"     # 执行超时


@dataclass
class Job:
    """一个后台任务的完整快照。"""

    id: str
    owner: str
    idempotency_key: str
    payload_hash: str
    payload: dict[str, Any]
    status: JobStatus = JobStatus.PENDING
    result: Any = None
    error_category: ErrorCategory | None = None
    error_code: str | None = None
    error_message: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex

    def snapshot(self) -> dict[str, Any]:
        """线程安全地导出不可变快照（标量字段均为不可变值/引用副本）。"""
        return {
            "id": self.id,
            "owner": self.owner,
            "idempotency_key": self.idempotency_key,
            "payload_hash": self.payload_hash,
            "payload": dict(self.payload),
            "status": self.status.value,
            "result": self.result,
            "error_category": (
                self.error_category.value if self.error_category else None
            ),
            "error_code": self.error_code,
            "error_message": self.error_message,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
