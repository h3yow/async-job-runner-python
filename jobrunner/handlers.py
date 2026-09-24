"""任务处理器：从载荷到结果的可调用对象。

约定：
- 处理器通过名称注册在 HANDLER_REGISTRY 中；未注册名称属于提交期业务错误。
- 处理器主动抛 JobExecutionError 表示可分类的业务失败（BUSINESS）。
- 处理器抛出其它任何异常都被执行器归类为系统性错误（SYSTEM）。
- cancel_event 被设置后，处理器应在协作检查点尽快抛出 TaskCancelled，
  从而让本次执行真正停止（而不是等到自然结束）。
"""

from __future__ import annotations

import threading
from typing import Any, Callable

# (payload, cancel_event) -> result
TaskHandler = Callable[[dict[str, Any], threading.Event], Any]


class TaskCancelled(Exception):
    """处理器在协作式检查点检测到取消。"""


HANDLER_REGISTRY: dict[str, TaskHandler] = {}


def register_handler(name: str, fn: TaskHandler) -> None:
    if not name:
        raise ValueError("handler name must be non-empty")
    HANDLER_REGISTRY[name] = fn


def get_handler(name: str) -> TaskHandler | None:
    return HANDLER_REGISTRY.get(name)
