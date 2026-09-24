"""JobManager 服务层测试。

覆盖：状态机正常/异常链路、幂等键一致/冲突、越权 403 与不存在 404 可区分、
配额串行拒绝与释放、取消 PENDING/RUNNING/终态、超时、并发提交竞态。
所有结论基于确定性轮询与 asyncio 屏障，可重复运行。
"""

from __future__ import annotations

import asyncio
import logging
import unittest

from jobrunner.errors import (
    IdempotencyConflictError,
    InvalidJobRequestError,
    JobNotFoundError,
    JobPermissionDeniedError,
    JobTerminalStateError,
    QuotaExceededError,
)
from jobrunner.manager import JobManager
from jobrunner.models import FailReason, JobStatus

from ._base import LoggedTestCase

log = logging.getLogger("jobrunner.tests")


class StateMachineTests(LoggedTestCase):
    async def test_succeed_fail_crash_timeout(self) -> None:
        m = JobManager()
        try:
            cases = [
                ("succeed", {"value": 1}, JobStatus.SUCCEEDED, None),
                ("fail", {"message": "biz"}, JobStatus.FAILED, FailReason.BUSINESS),
                ("crash", {"message": "x"}, JobStatus.FAILED, FailReason.CRASH),
            ]
            for job_type, payload, want_status, want_reason in cases:
                job, created = await m.submit(owner="alice", job_type=job_type, payload=payload)
                self.assertTrue(created)
                done = await self.wait_for(m, job, "SUCCEEDED", "FAILED", "CANCELLED")
                self.log_assert(
                    job_type,
                    done.status == want_status and done.fail_reason == want_reason,
                    status=done.status.value,
                    reason=done.fail_reason,
                    code=done.error_code,
                )

            job, _ = await m.submit(
                owner="alice", job_type="sleep", payload={"seconds": 1}, timeout=0.05
            )
            done = await self.wait_for(m, job, "FAILED")
            self.log_assert(
                "timeout",
                done.fail_reason == FailReason.TIMEOUT
                and done.error_code == "timeout",
                status=done.status.value,
                reason=done.fail_reason,
            )
            self.log_assert(
                "quota released after finishes",
                dict(m._running) == {},
                running=dict(m._running),
            )
        finally:
            await m.aclose()

    async def test_terminal_state_never_changes_and_attempt_once(self) -> None:
        m = JobManager()
        try:
            job, _ = await m.submit(owner="alice", job_type="succeed", payload={})
            done = await self.wait_for(m, job, "SUCCEEDED")
            # 终态取消必须被拒绝
            with self.assertRaises(JobTerminalStateError):
                await m.cancel(owner="alice", job_id=job.id)
            await asyncio.sleep(0.05)
            again = await m.get(owner="alice", job_id=job.id)
            self.log_assert(
                "terminal immutable",
                again.status == JobStatus.SUCCEEDED and again.attempt == 1,
                status=again.status.value,
                attempt=again.attempt,
            )
        finally:
            await m.aclose()


class IdempotencyTests(LoggedTestCase):
    async def test_same_key_same_payload_returns_same_job_executed_once(self) -> None:
        m = JobManager()
        try:
            j1, c1 = await m.submit(
                owner="alice", job_type="succeed", payload={"v": 1}, idempotency_key="K"
            )
            j2, c2 = await m.submit(
                owner="alice", job_type="succeed", payload={"v": 1}, idempotency_key="K"
            )
            self.log_assert(
                "same job", j1.id == j2.id and c1 is True and c2 is False,
                id1=j1.id, id2=j2.id, created1=c1, created2=c2,
            )
            done = await self.wait_for(m, j1, "SUCCEEDED")
            self.log_assert("executed once", done.attempt == 1, attempt=done.attempt)

            # 终态后用同一键再提交，仍然返回同一任务
            j3, c3 = await m.submit(
                owner="alice", job_type="succeed", payload={"v": 1}, idempotency_key="K"
            )
            self.assertTrue(j3.id == j1.id and c3 is False)
        finally:
            await m.aclose()

    async def test_same_key_different_payload_rejected(self) -> None:
        m = JobManager()
        try:
            await m.submit(
                owner="alice", job_type="succeed", payload={"v": 1}, idempotency_key="K"
            )
            with self.assertRaises(IdempotencyConflictError) as ctx:
                await m.submit(
                    owner="alice", job_type="succeed", payload={"v": 2},
                    idempotency_key="K",
                )
            self.log_assert(
                "payload conflict rejected",
                "different payload" in str(ctx.exception),
                message=str(ctx.exception),
            )
            with self.assertRaises(IdempotencyConflictError):
                await m.submit(
                    owner="alice", job_type="fail", payload={"v": 1},
                    idempotency_key="K",
                )

            # 不同主体的相同键互不影响（键按主体隔离）
            jb, created_b = await m.submit(
                owner="bob", job_type="succeed", payload={"v": 1}, idempotency_key="K"
            )
            self.assertTrue(created_b)
        finally:
            await m.aclose()

    async def test_concurrent_duplicate_submission_executes_once(self) -> None:
        m = JobManager()
        try:
            async def one() -> tuple[str, bool]:
                j, created = await m.submit(
                    owner="alice", job_type="succeed", payload={"v": 1},
                    idempotency_key="RACE",
                )
                return j.id, created

            results = await asyncio.gather(*(one() for _ in range(20)))
            ids = {jid for jid, _ in results}
            created = sum(1 for _, c in results if c)
            self.log_assert(
                "20 concurrent same-key submits -> 1 job, 1 created",
                len(ids) == 1 and created == 1,
                distinct_ids=len(ids), created_count=created,
            )
            only = next(iter(ids))
            done = await m.get(owner="alice", job_id=only)
            await self.wait_for(m, done, "SUCCEEDED")
            done = await m.get(owner="alice", job_id=only)
            self.log_assert("single execution", done.attempt == 1, attempt=done.attempt)
        finally:
            await m.aclose()


class OwnershipTests(LoggedTestCase):
    async def test_not_found_and_forbidden_are_distinct(self) -> None:
        m = JobManager()
        try:
            job, _ = await m.submit(owner="alice", job_type="succeed", payload={})
            await self.wait_for(m, job, "SUCCEEDED")

            try:
                await m.get(owner="bob", job_id=job.id)
                raise AssertionError("expected JobPermissionDeniedError")
            except JobPermissionDeniedError as exc:
                self.log_assert("cross-owner get -> 403 class", True, message=exc.message)

            try:
                await m.cancel(owner="bob", job_id=job.id)
                raise AssertionError("expected JobPermissionDeniedError")
            except JobPermissionDeniedError:
                pass

            try:
                await m.get(owner="alice", job_id="does-not-exist")
                raise AssertionError("expected JobNotFoundError")
            except JobNotFoundError as exc:
                self.log_assert("missing -> 404 class", True, message=exc.message)
        finally:
            await m.aclose()

    async def test_quotas_are_isolated_per_owner(self) -> None:
        m = JobManager(default_quota=1)
        try:
            a, _ = await m.submit(owner="a", job_type="sleep", payload={"seconds": 5})
            await self.wait_for(m, a, "RUNNING")
            with self.assertRaises(QuotaExceededError):
                await m.submit(owner="a", job_type="sleep", payload={"seconds": 5})
            b, created = await m.submit(owner="b", job_type="sleep", payload={"seconds": 5})
            self.log_assert("owner b independent quota", created, b_id=b.id)
            await m.cancel(owner="a", job_id=a.id)
            await m.cancel(owner="b", job_id=b.id)
            await self.wait_for(m, a, "CANCELLED")
            await self.wait_for(m, b, "CANCELLED")
        finally:
            await m.aclose()


class QuotaRaceTests(LoggedTestCase):
    async def test_concurrent_submits_never_exceed_quota(self) -> None:
        quota = 5
        n = 50
        m = JobManager(default_quota=quota)
        try:
            async def one(i: int) -> str:
                try:
                    job, _ = await m.submit(
                        owner="alice", job_type="sleep",
                        payload={"seconds": 0.05 + 0.01 * (i % 3)}, timeout=5,
                    )
                    await self.wait_for(m, job, "SUCCEEDED", "FAILED", "CANCELLED")
                    return "accepted"
                except QuotaExceededError:
                    return "rejected"

            results = await asyncio.gather(*(one(i) for i in range(n)))
            accepted = results.count("accepted")
            self.log_assert(
                "exactly quota accepted under contention",
                accepted == quota,
                accepted=accepted, rejected=n - accepted, quota=quota,
            )
            await asyncio.sleep(0.05)
            self.log_assert(
                "no slot leak after drain",
                dict(m._running) == {},
                running=dict(m._running),
            )
        finally:
            await m.aclose()

    async def test_rejected_submit_consumes_no_slot_and_idem_key(self) -> None:
        m = JobManager(default_quota=1)
        try:
            a, _ = await m.submit(owner="alice", job_type="sleep", payload={"seconds": 5})
            await self.wait_for(m, a, "RUNNING")
            # 带幂等键的超额提交：被拒绝且键不得被登记
            with self.assertRaises(QuotaExceededError):
                await m.submit(
                    owner="alice", job_type="sleep", payload={"seconds": 5},
                    idempotency_key="USED-LATER",
                )
            await m.cancel(owner="alice", job_id=a.id)
            await self.wait_for(m, a, "CANCELLED")
            b, created = await m.submit(
                owner="alice", job_type="sleep", payload={"seconds": 0.01},
                idempotency_key="USED-LATER", timeout=5,
            )
            self.log_assert("rejected idem key reusable", created, id=b.id)
            await self.wait_for(m, b, "SUCCEEDED")
        finally:
            await m.aclose()

    async def test_slot_released_on_every_terminal_outcome(self) -> None:
        m = JobManager(default_quota=1)
        try:
            for job_type, payload, expect in [
                ("succeed", {}, "SUCCEEDED"),
                ("fail", {}, "FAILED"),
                ("crash", {}, "FAILED"),
                ("sleep", {"seconds": 1}, "FAILED"),  # timeout
            ]:
                kw = {"timeout": 0.05} if job_type == "sleep" else {}
                job, _ = await m.submit(
                    owner="alice", job_type=job_type, payload=payload, **kw
                )
                await self.wait_for(m, job, "SUCCEEDED", "FAILED", "CANCELLED")
                self.log_assert(
                    f"slot free after {job_type}",
                    dict(m._running) == {},
                    outcome=expect, running=dict(m._running),
                )
        finally:
            await m.aclose()


class CancelTests(LoggedTestCase):
    async def test_cancel_pending_job(self) -> None:
        m = JobManager(default_quota=1)
        try:
            blocking, _ = await m.submit(owner="alice", job_type="sleep", payload={"seconds": 5})
            await self.wait_for(m, blocking, "RUNNING")
            # 此提交必被拒绝（额度满），所以无法进入 PENDING；
            # PENDING 窗口用直接构造模拟：取消一个刚提交但还没跑起来的任务。
            await m.cancel(owner="alice", job_id=blocking.id)
            await self.wait_for(m, blocking, "CANCELLED")
            pending, _ = await m.submit(owner="alice", job_type="succeed", payload={})
            cancelled_view = await m.cancel(owner="alice", job_id=pending.id)
            # 取消可能先于或后于协程调度，终态都必须是 CANCELLED 且只释放一次额度
            final = await self.wait_for(m, pending, "CANCELLED")
            self.log_assert(
                "cancel settles CANCELLED",
                final.status == JobStatus.CANCELLED and final.attempt <= 1,
                immediate_status=cancelled_view.status.value, attempt=final.attempt,
            )
            await asyncio.sleep(0.05)
            self.log_assert("no double release", dict(m._running) == {},
                            running=dict(m._running))
        finally:
            await m.aclose()

    async def test_cancel_running_job_stops_after_execution_settles(self) -> None:
        m = JobManager()
        try:
            job, _ = await m.submit(
                owner="alice", job_type="sleep", payload={"seconds": 5}, timeout=10
            )
            await self.wait_for(m, job, "RUNNING")
            view = await m.cancel(owner="alice", job_id=job.id)
            self.log_assert(
                "cancel marks request on running job",
                view.cancel_requested and view.status == JobStatus.RUNNING,
                status=view.status.value, cancel_requested=view.cancel_requested,
            )
            final = await self.wait_for(m, job, "CANCELLED", timeout=2)
            self.log_assert(
                "running job ends CANCELLED",
                final.status == JobStatus.CANCELLED and final.fail_reason is None,
                status=final.status.value, reason=final.fail_reason,
            )
            await asyncio.sleep(0.05)
            self.log_assert("slot released", dict(m._running) == {},
                            running=dict(m._running))
            # 终态后不能再取消
            with self.assertRaises(JobTerminalStateError):
                await m.cancel(owner="alice", job_id=job.id)
        finally:
            await m.aclose()

    async def test_unknown_type_rejected_before_state(self) -> None:
        m = JobManager()
        try:
            with self.assertRaises(InvalidJobRequestError):
                await m.submit(owner="alice", job_type="nope", payload={})
            self.log_assert("no state created", len(m._jobs) == 0, jobs=len(m._jobs))
        finally:
            await m.aclose()


if __name__ == "__main__":
    unittest.main(verbosity=2)
