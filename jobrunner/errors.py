"""错误类型定义。

错误分两大类：
- 业务性错误（JobError）：调用方可修正的请求问题，HTTP 层映射为 4xx；
- 系统性错误（JobSystemError）：执行器/超时等运行环境问题，HTTP 层映射为 5xx。
"""

from __future__ import annotations


class JobError(Exception):
    """业务性错误基类（不代表系统故障）。"""

    code: str = "job_error"
    http_status: int = 400

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        if code is not None:
            self.code = code


class JobNotFoundError(JobError):
    """任务不存在（与无权限严格区分）。"""

    code = "job_not_found"
    http_status = 404


class JobPermissionDeniedError(JobError):
    """任务存在但不属于当前调用主体。"""

    code = "permission_denied"
    http_status = 403


class IdempotencyConflictError(JobError):
    """相同幂等键但载荷不一致。"""

    code = "idempotency_conflict"
    http_status = 409


class QuotaExceededError(JobError):
    """超过该主体的并发运行额度。"""

    code = "quota_exceeded"
    http_status = 429


class JobTerminalStateError(JobError):
    """对终态任务执行非法操作（如取消已完成任务）。"""

    code = "terminal_state"
    http_status = 409


class InvalidJobRequestError(JobError):
    """请求本身非法（未知任务类型、缺字段等）。"""

    code = "invalid_request"
    http_status = 400


class JobSystemError(Exception):
    """系统性错误基类（执行环境问题，非业务问题）。"""

    code: str = "system_error"


class JobTimeoutError(JobSystemError):
    """任务执行超过允许时长。"""

    code = "timeout"


class JobExecutionCrashed(JobSystemError):
    """任务处理器抛出非业务异常（如 TypeError / KeyError）。"""

    code = "execution_crash"
