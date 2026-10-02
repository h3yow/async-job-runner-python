"""统一错误类型骨架。"""

from __future__ import annotations

from .models import ErrorCategory


class JobRunnerError(Exception):
    """所有任务系统错误的基类。"""

    http_status: int = 400
    error_code: str = "JOB_RUNNER_ERROR"

    def __init__(self, message: str = "", *, job_id: str | None = None):
        super().__init__(message)
        self.message = message
        self.job_id = job_id


class JobNotFound(JobRunnerError):
    """任务不存在（404），与无权限严格区分。"""

    http_status = 404
    error_code = "JOB_NOT_FOUND"


class PermissionDenied(JobRunnerError):
    """任务存在但不属于调用主体（403）。"""

    http_status = 403
    error_code = "PERMISSION_DENIED"


class QuotaExceeded(JobRunnerError):
    """超出主体并发上限；提交被拒绝，不占用额度。"""

    http_status = 429
    error_code = "QUOTA_EXCEEDED"


class QueueFull(JobRunnerError):
    """排队容量已满；排队提交被拒绝，不创建任务、不占用任何额度。"""

    http_status = 429
    error_code = "QUEUE_FULL"


class IdempotencyConflict(JobRunnerError):
    """相同幂等键但载荷不一致。"""

    http_status = 409
    error_code = "IDEMPOTENCY_CONFLICT"


class InvalidStateTransition(JobRunnerError):
    """终态任务被回退/重复执行，或非法状态流转。"""

    http_status = 409
    error_code = "INVALID_STATE_TRANSITION"


class JobExecutionError(Exception):
    """任务处理器抛出的执行错误（内部使用）。"""

    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        code: str = "EXECUTION_ERROR",
    ):
        super().__init__(message)
        self.message = message
        self.category = category
        self.code = code


class InvalidRequest(JobRunnerError):
    """提交参数不合法（未知处理器等），属业务类拒绝。"""

    http_status = 422
    error_code = "INVALID_REQUEST"


class MissingPrincipal(JobRunnerError):
    """缺少调用主体标识（X-Owner-Id）。"""

    http_status = 401
    error_code = "MISSING_PRINCIPAL"
