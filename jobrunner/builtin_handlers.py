"""内置示例任务处理器。

- echo      ：原样返回载荷中的 message，用于快速成功路径。
- sleep     ：睡眠 seconds 秒，每 0.02s 检查一次取消；
              fail=True 时在醒来后抛业务错误；用于取消/超时测试。
- boom      ：无条件抛出非 JobExecutionError 的普通异常，
              用于验证「未知异常 -> 系统性错误 SYSTEM」的归类。
- fail_biz  ：抛 JobExecutionError(BUSINESS)，用于验证业务错误归类。
"""

from __future__ import annotations

import threading
from typing import Any

from .errors import JobExecutionError
from .handlers import TaskCancelled, register_handler
from .models import ErrorCategory

_TICK = 0.02


def echo(payload: dict[str, Any], cancel_event: threading.Event) -> dict[str, Any]:
    return {"echo": payload.get("message", "")}


def sleep_task(
    payload: dict[str, Any], cancel_event: threading.Event
) -> dict[str, Any]:
    seconds = float(payload.get("seconds", 0.1))
    deadline = None
    import time

    deadline = time.monotonic() + seconds
    while True:
        if cancel_event.is_set():
            raise TaskCancelled("cancelled during sleep")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        cancel_event.wait(min(_TICK, remaining))
    if payload.get("fail"):
        raise JobExecutionError(
            "sleep_task configured to fail",
            category=ErrorCategory.BUSINESS,
            code="SLEEP_TASK_BUSINESS_FAILURE",
        )
    return {"slept": seconds}


def fail_biz(
    payload: dict[str, Any], cancel_event: threading.Event
) -> dict[str, Any]:
    raise JobExecutionError(
        str(payload.get("message", "intentional business failure")),
        category=ErrorCategory.BUSINESS,
        code="BUSINESS_FAILURE",
    )


def boom(payload: dict[str, Any], cancel_event: threading.Event) -> Any:
    raise RuntimeError("intentional system crash")


def register_builtins() -> None:
    """幂等地注册全部内置处理器（重复调用安全）。"""
    register_handler("echo", echo)
    register_handler("sleep", sleep_task)
    register_handler("fail_biz", fail_biz)
    register_handler("boom", boom)
