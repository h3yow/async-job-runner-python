"""测试公共工具：轮询等待与关键输入/判定日志。"""

from __future__ import annotations

import asyncio
import logging
import time
import unittest

log = logging.getLogger("jobrunner.tests")


class LoggedTestCase(unittest.IsolatedAsyncioTestCase):
    def log_input(self, label: str, **fields: object) -> None:
        log.info("INPUT  [%s.%s] %s %s", type(self).__name__, label, label, fields)

    def log_assert(self, label: str, condition: bool, **fields: object) -> None:
        log.info(
            "ASSERT [%s.%s] condition=%s %s", type(self).__name__, label, condition, fields
        )
        self.assertTrue(condition, f"{label}: {fields}")

    async def wait_for(
        self, manager, job, *statuses: str, timeout: float = 5.0, interval: float = 0.005
    ):
        deadline = time.monotonic() + timeout
        names = set(statuses)
        latest = None
        while time.monotonic() < deadline:
            latest = await manager.get(owner=job.owner, job_id=job.id)
            if latest.status.value in names:
                return latest
            await asyncio.sleep(interval)
        raise AssertionError(
            f"job {job.id} never reached {names}; latest={latest and latest.status.value}"
        )
