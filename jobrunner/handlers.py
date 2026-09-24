"""任务处理器注册表。

任务类型 -> async handler(payload, token) -> result。
内置 sleep / fail / crash / succeed 四种类型，覆盖超时、业务失败、
系统崩溃与正常成功等测试场景；自定义类型可通过 register 注册。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .errors import InvalidJobRequestError, JobError

JobHandler = Callable[[dict[str, Any], "CancelToken"], Awaitable[Any]]

_REGISTRY: dict[str, JobHandler] = {}


class CancelledByUser(Exception):
    """协作式取消信号：任务在本次执行中被用户请求取消。"""


class CancelToken:
    """协作式取消令牌：运行中的任务通过 raise_if_cancelled 主动检查。"""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def request(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    async def raise_if_cancelled(self) -> None:
        if self._event.is_set():
            raise CancelledByUser()


def register(name: str, handler: JobHandler) -> None:
    if not name or not isinstance(name, str):
        raise ValueError("handler name must be a non-empty string")
    _REGISTRY[name] = handler


def get_handler(name: str) -> JobHandler:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise InvalidJobRequestError(
            f"unknown job type: {name!r}; available: {sorted(_REGISTRY)}"
        ) from None


# ---------------- 内置任务类型 ----------------


async def _sleep(payload: dict[str, Any], token: CancelToken) -> Any:
    """按秒休眠，可中途响应取消；用于配额/取消/超时测试。"""
    seconds = float(payload.get("seconds", 0.0))
    if seconds < 0:
        raise JobError("seconds must be >= 0", code="invalid_payload")
    deadline = asyncio.get_running_loop().time() + seconds
    while True:
        await token.raise_if_cancelled()
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return {"slept": seconds}
        await asyncio.sleep(min(remaining, 0.02))


async def _fail(payload: dict[str, Any], token: CancelToken) -> Any:
    """确定性业务失败。"""
    raise JobError(payload.get("message", "business failure"), code="business_failure")


async def _crash(payload: dict[str, Any], token: CancelToken) -> Any:
    """确定性系统崩溃（非业务异常）。"""
    raise RuntimeError(payload.get("message", "unexpected crash"))


async def _succeed(payload: dict[str, Any], token: CancelToken) -> Any:
    """立即成功，返回 payload 中的 value。"""
    return {"value": payload.get("value")}


register("sleep", _sleep)
register("fail", _fail)
register("crash", _crash)
register("succeed", _succeed)
