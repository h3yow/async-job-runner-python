"""排队等待能力测试（标准库 unittest，无需额外依赖；结论可重复）。

运行：
    .venv/bin/python -m unittest tests.test_queue -v

覆盖：
- 排队准入严格按提交先后（FIFO），不插队；
- 等待中取消立即出列、永不执行、不占额度；终态任务取消仍被拒；
- 等待队列满 -> 429 QUEUE_FULL，不占额度也不占队列位；
- 超时只从真正开始执行起算，等待时长不计入；
- 超时/取消与准入交叠时并发峰值绝不超过上限、额度不泄漏不空转；
- wait=False 直拒老行为与排队模式并存，互不影响。

每个用例打印关键输入（主体/上限/队列容量/提交顺序）与判定依据。
"""

from __future__ import annotations

import threading
import time
import unittest

from jobrunner.builtin_handlers import register_builtins
from jobrunner.errors import InvalidStateTransition, QueueFull, QuotaExceeded
from jobrunner.models import JobStatus
from jobrunner.service import JobService
from jobrunner.store import JobStore

register_builtins()

_TERM = {
    JobStatus.SUCCEEDED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELLED.value,
}


def wait_for(
    svc: JobService,
    owner: str,
    job_id: str,
    *statuses: str,
    timeout: float = 5.0,
) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        last = svc.get(owner, job_id)
        if not statuses or last["status"] in statuses:
            if last["status"] in _TERM or (
                statuses and last["status"] in statuses
            ):
                return last
        time.sleep(0.005)
    raise AssertionError(f"job {job_id} not in {statuses} within {timeout}s: {last}")


def wait_terminal(svc: JobService, owner: str, job_id: str, timeout: float = 5.0) -> dict:
    return wait_for(svc, owner, job_id, *_TERM, timeout=timeout)


# 记录处理器真正开始执行的时刻与并发数，供「等待时长不计入超时」与峰值观测使用
_start_lock = threading.Lock()
_start_times: dict[str, float] = {}
_running_now: dict[str, int] = {}
_peak: dict[str, int] = {}


def _instrumented_sleep(payload: dict, cancel_event: threading.Event) -> dict:
    name = payload["name"]
    with _start_lock:
        _start_times[name] = time.monotonic()
        n = _running_now.get(name, 0) + 1
        _running_now[name] = n
        _peak[name] = max(_peak.get(name, 0), n)
    try:
        deadline = time.monotonic() + payload.get("seconds", 0.1)
        while True:
            if cancel_event.is_set():
                from jobrunner.handlers import TaskCancelled

                raise TaskCancelled("cancelled during sleep")
            remain = deadline - time.monotonic()
            if remain <= 0:
                break
            cancel_event.wait(min(0.01, remain))
        return {"ran": name}
    finally:
        with _start_lock:
            _running_now[name] = _running_now.get(name, 1) - 1


from jobrunner.handlers import register_handler

register_handler("instrumented_sleep", _instrumented_sleep)


class QueueTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = JobStore(default_quota=1)
        self.svc = JobService(store=self.store)

    def tearDown(self) -> None:
        self.svc.shutdown()


class TestQueueAdmission(QueueTestBase):
    def test_queued_when_full_and_fifo_order(self) -> None:
        self.svc.set_quota("alice", 2)
        self.svc.set_queue_limit("alice", 4)
        t0 = time.monotonic()
        # 先占满 2 个并发（长任务）
        runners = [
            self.svc.submit(
                "alice",
                "instrumented_sleep",
                {"name": f"run-{i}", "seconds": 0.6},
                idempotency_key=f"r{i}",
                wait=True,
            )[0]
            for i in range(2)
        ]
        # 再按严格顺序排 3 个
        queued = [
            self.svc.submit(
                "alice",
                "instrumented_sleep",
                {"name": f"q{i}", "seconds": 0.05},
                idempotency_key=f"q{i}",
                wait=True,
            )[0]
            for i in range(3)
        ]
        print(
            f"[fifo] 输入: 上限=2, 先提交 r0,r1 占满, 再按序排队 q0,q1,q2; "
            f"排队态观测={[self.svc.get('alice', j.id)['status'] for j in queued]}"
        )
        self.assertEqual(
            [self.svc.get("alice", j.id)["status"] for j in queued],
            ["QUEUED"] * 3,
        )
        self.assertEqual(self.store.running_count("alice"), 2)

        for j in runners + queued:
            wait_terminal(self.svc, "alice", j.id)
        # 判定依据直接取处理器内部记录的真实进入时刻（不依赖观察线程的调度）
        started_at = {n: _start_times[n] for n in ("q0", "q1", "q2") if n in _start_times}
        start_order = sorted(started_at, key=lambda n: started_at[n])
        runner_starts = [_start_times.get(f"run-{i}") for i in range(2)]
        print(
            f"[fifo] 判定依据: 真实进入处理器顺序={start_order} "
            f"(必须恰为提交顺序 ['q0','q1','q2']); "
            f"q0 进入时刻晚于占位任务={all(runner_starts)}"
        )
        self.assertEqual(start_order, ["q0", "q1", "q2"], "等待者必须按提交先后起、不许插队")
        self.assertTrue(all(t is not None for t in runner_starts))
        self.assertGreaterEqual(started_at["q0"], min(t for t in runner_starts if t))
        for j in queued:
            self.assertEqual(
                self.svc.get("alice", j.id)["status"],
                JobStatus.SUCCEEDED.value,
            )
        self.assertEqual(self.store.running_count("alice"), 0)
        self.assertEqual(self.store.queued_count("alice"), 0)

    def test_direct_reject_still_works_alongside_queue(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 3)
        r, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.4}, idempotency_key="d0"
        )
        # wait=True -> 排队
        q, _ = self.svc.submit(
            "alice", "echo", {}, idempotency_key="d1", wait=True
        )
        self.assertEqual(self.svc.get("alice", q.id)["status"], "QUEUED")
        # wait=False 老行为：即使队列里有人，仍按「额度满」立即拒绝
        with self.assertRaises(QuotaExceeded) as ctx:
            self.svc.submit("alice", "echo", {}, idempotency_key="d2")
        print(
            f"[compat] 排队任务存在时 wait=False 仍 -> {ctx.exception.error_code}; "
            f"队列保持 {self.store.queued_count('alice')} 个等待者不被影响"
        )
        self.assertEqual(self.store.queued_count("alice"), 1)
        wait_terminal(self.svc, "alice", r.id)
        wait_terminal(self.svc, "alice", q.id)
        self.assertEqual(self.store.running_count("alice"), 0)


class TestQueueCancel(QueueTestBase):
    def test_cancel_queued_exits_immediately_and_never_runs(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 3)
        r, _ = self.svc.submit(
            "alice",
            "instrumented_sleep",
            {"name": "cq-run", "seconds": 0.5},
            idempotency_key="c0",
        )
        q, _ = self.svc.submit(
            "alice",
            "instrumented_sleep",
            {"name": "cq-queued", "seconds": 0.1},
            idempotency_key="c1",
            wait=True,
        )
        # 取消立刻返回终态（不必轮询）
        snap = self.svc.cancel("alice", q.id)
        print(
            f"[cancel-queued] 输入: 任务 {q.id[:8]} 在 QUEUED; "
            f"取消返回状态={snap['status']} code={snap['error_code']}"
        )
        self.assertEqual(snap["status"], JobStatus.CANCELLED.value)
        self.assertEqual(self.store.queued_count("alice"), 0)
        self.assertEqual(self.store.running_count("alice"), 1, "等待取消不触碰额度")
        wait_terminal(self.svc, "alice", r.id)
        time.sleep(0.15) # 等待者若被错误执行此刻应已跑完
        print(
            "[cancel-queued] 判定依据: 处理器执行记录="
            f"{_start_times.get('cq-queued') is not None}, "
            f"最终状态={self.svc.get('alice', q.id)['status']}, "
            f"running={self.store.running_count('alice')}"
        )
        self.assertNotIn("cq-queued", _start_times, "取消后的等待任务永远不许执行")
        self.assertEqual(
            self.svc.get("alice", q.id)["status"], JobStatus.CANCELLED.value
        )
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_cancel_terminal_queued_job_still_rejected(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 2)
        r, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.4}, idempotency_key="t0"
        )
        q, _ = self.svc.submit(
            "alice", "echo", {}, idempotency_key="t1", wait=True
        )
        self.svc.cancel("alice", q.id)  # 等待中取消 -> CANCELLED 终态
        with self.assertRaises(InvalidStateTransition) as ctx:
            self.svc.cancel("alice", q.id)
        print(
            f"[cancel-queued-terminal] QUEUED->CANCELLED 后再取消 -> "
            f"{ctx.exception.error_code}: {ctx.exception.message}"
        )
        self.assertEqual(ctx.exception.http_status, 409)
        self.assertEqual(
            self.svc.get("alice", q.id)["status"], JobStatus.CANCELLED.value
        )
        wait_terminal(self.svc, "alice", r.id)

    def test_cancel_head_promotes_next_without_quota_change(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 4)
        r, _ = self.svc.submit(
            "alice",
            "instrumented_sleep",
            {"name": "head-run", "seconds": 0.5},
            idempotency_key="h0",
        )
        qs = [
            self.svc.submit(
                "alice",
                "instrumented_sleep",
                {"name": f"head-{i}", "seconds": 0.05},
                idempotency_key=f"h{i+1}",
                wait=True,
            )[0]
            for i in range(3)
        ]
        # 取消队首 q0：q1 应在 r 结束后先于 q2 启动（取消不额外释放额度）
        self.svc.cancel("alice", qs[0].id)
        wait_terminal(self.svc, "alice", r.id)
        wait_terminal(self.svc, "alice", qs[1].id)
        wait_terminal(self.svc, "alice", qs[2].id)
        started_at = {n: _start_times[n] for n in ("head-1", "head-2") if n in _start_times}
        order = sorted(started_at, key=lambda n: started_at[n])
        print(
            f"[cancel-head] 取消队首后真实提升顺序={order}（应 ['head-1','head-2']）; "
            f"q0 未执行={'head-0' not in _start_times}"
        )
        self.assertEqual(order, ["head-1", "head-2"])
        self.assertNotIn("head-0", _start_times)
        self.assertEqual(self.store.running_count("alice"), 0)


class TestQueueFull(QueueTestBase):
    def test_queue_full_rejected_without_consuming(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 2)
        r, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.5}, idempotency_key="f0"
        )
        qs = [
            self.svc.submit(
                "alice", "echo", {}, idempotency_key=f"f{i+1}", wait=True
            )[0]
            for i in range(2)
        ]
        with self.assertRaises(QueueFull) as ctx:
            self.svc.submit("alice", "echo", {}, idempotency_key="f3", wait=True)
        print(
            f"[queue-full] 输入: 上限=1 队列容量=2, 1 跑 2 排; 第三个排队提交 -> "
            f"{ctx.exception.error_code} ({ctx.exception.http_status})"
        )
        self.assertEqual(ctx.exception.error_code, "QUEUE_FULL")
        # 被拒不占额度也不占队列位
        self.assertEqual(self.store.running_count("alice"), 1)
        self.assertEqual(self.store.queued_count("alice"), 2)
        # 任务列表里只有 3 个任务（1 RUNNING + 2 QUEUED）
        jobs = self.svc.list_jobs("alice")
        self.assertEqual(len(jobs), 3)
        # 被拒的幂等键没有被登记：稍后用同键仍可在有额度时提交
        wait_terminal(self.svc, "alice", r.id)
        for q in qs:
            wait_terminal(self.svc, "alice", q.id)
        again, replayed = self.svc.submit(
            "alice", "echo", {}, idempotency_key="f3"
        )
        print(
            "[queue-full] 判定依据: 队列消化后用被拒时的同键重提 -> "
            f"新任务={again.id[:8]} replayed={replayed}（必须不是重放）"
        )
        self.assertFalse(replayed)
        wait_terminal(self.svc, "alice", again.id)
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_queue_limit_zero_disables_queue(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 0)
        self.svc.submit("alice", "sleep", {"seconds": 0.3}, idempotency_key="z0")
        with self.assertRaises(QueueFull):
            self.svc.submit("alice", "echo", {}, idempotency_key="z1", wait=True)
        with self.assertRaises(QuotaExceeded):
            self.svc.submit("alice", "echo", {}, idempotency_key="z2")
        print("[queue-zero] 队列容量=0: wait=True -> QUEUE_FULL, wait=False -> QUOTA_EXCEEDED")


class TestTimeoutFromStart(QueueTestBase):
    def test_waiting_time_not_counted_in_timeout(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 2)
        t_begin = time.monotonic()
        r, _ = self.svc.submit(
            "alice",
            "instrumented_sleep",
            {"name": "to-run", "seconds": 0.5},
            idempotency_key="w0",
        )
        # 排在后面：需等待 ~0.5s，执行本身只需 0.15s，timeout 只给 0.25s。
        # 若等待时长被错误计入，必然超时；正确语义下应成功。
        q, _ = self.svc.submit(
            "alice",
            "instrumented_sleep",
            {"name": "to-queued", "seconds": 0.15},
            idempotency_key="w1",
            wait=True,
            timeout=0.25,
        )
        final = wait_terminal(self.svc, "alice", q.id, timeout=3)
        started = _start_times.get("to-queued")
        wait_before_start = (started - t_begin) if started else -1
        print(
            f"[timeout-start] 输入: 排队等待约 {wait_before_start:.2f}s, "
            f"执行需 0.15s, timeout=0.25s; 判定依据: status={final['status']} "
            f"category={final['error_category']}（必须 SUCCEEDED）"
        )
        self.assertEqual(final["status"], JobStatus.SUCCEEDED.value)
        self.assertGreater(wait_before_start, 0.3, "任务确实先经历了等待")
        wait_terminal(self.svc, "alice", r.id)
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_timeout_after_start_still_fires(self) -> None:
        # 另一个方向：起执行之后超时仍然要触发
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_limit("alice", 1)
        r, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.3}, idempotency_key="x0"
        )
        q, _ = self.svc.submit(
            "alice",
            "sleep",
            {"seconds": 2},
            idempotency_key="x1",
            wait=True,
            timeout=0.15,
        )
        final = wait_terminal(self.svc, "alice", q.id, timeout=3)
        print(
            f"[timeout-fire] 起执行后 timeout=0.15s, 任务需 2s -> "
            f"status={final['status']} category={final['error_category']}"
        )
        self.assertEqual(final["status"], JobStatus.FAILED.value)
        self.assertEqual(final["error_category"], "TIMEOUT")
        wait_terminal(self.svc, "alice", r.id)
        self.assertEqual(self.store.running_count("alice"), 0)


class TestOverlapPeak(QueueTestBase):
    def test_timeout_cancel_admission_overlap_peak_never_exceeds(self) -> None:
        limit = 2
        n_wait = 8
        self.svc.set_quota("dave", limit)
        self.svc.set_queue_limit("dave", n_wait + 4)

        stop = threading.Event()
        peak_lock = threading.Lock()
        peak = 0

        def monitor() -> None:
            nonlocal peak
            while not stop.is_set():
                used = self.store.running_count("dave")
                with peak_lock:
                    peak = max(peak, used)
                time.sleep(0.001)

        mon = threading.Thread(target=monitor)
        mon.start()
        try:
            # 2 个短超时占位（会很快 TIMEOUT 释放额度），8 个排队任务里
            # 再从外部取消其中 3 个，制造「超时/取消/准入」密集交叠。
            holders = [
                self.svc.submit(
                    "dave",
                    "sleep",
                    {"seconds": 5},
                    idempotency_key=f"hold-{i}",
                    timeout=0.12 + 0.05 * i,
                )[0]
                for i in range(limit)
            ]
            queued = [
                self.svc.submit(
                    "dave",
                    "instrumented_sleep",
                    {"name": f"ov-{i}", "seconds": 0.12},
                    idempotency_key=f"ov-{i}",
                    wait=True,
                )[0]
                for i in range(n_wait)
            ]
            print(
                f"[overlap] 输入: 上限={limit}, {limit} 个超时占位 + "
                f"{n_wait} 个排队; 排队后 running={self.store.running_count('dave')} "
                f"queued={self.store.queued_count('dave')}"
            )
            # 稍等，让部分排队任务开始被提升，再取消其中 3 个（含等待中/运行中）
            time.sleep(0.25)
            cancelled_ids = {queued[1].id, queued[4].id, queued[6].id}
            for jid in cancelled_ids:
                snap = self.svc.cancel("dave", jid)
                print(f"[overlap] 取消 {jid[:8]} -> 返回态 {snap['status']}")

            finals: dict[str, str] = {}
            for j in holders + queued:
                f = wait_terminal(self.svc, "dave", j.id, timeout=6)
                finals[j.id] = f["status"]

            started_names = {
                f"ov-{i}" for i, j in enumerate(queued) if f"ov-{i}" in _start_times
            }
            print(
                "[overlap] 判定依据: "
                f"被取消 3 个最终={[finals[i] for i in cancelled_ids]}; "
                f"超时占位最终={[finals[h.id] for h in holders]}; "
                f"排队任务实际启动数={len(started_names)}/8; 观测峰值并发={peak}"
            )
            # 取消的 3 个必须 CANCELLED；超时占位必须 FAILED
            for jid in cancelled_ids:
                self.assertEqual(finals[jid], JobStatus.CANCELLED.value)
            for h in holders:
                self.assertEqual(finals[h.id], JobStatus.FAILED.value)
            # 未取消的 5 个排队任务必须都成功（若某个被取消任务错误执行，
            # started 计数也会暴露异常）
            for j in queued:
                if j.id not in cancelled_ids:
                    self.assertEqual(finals[j.id], JobStatus.SUCCEEDED.value)
            self.assertLessEqual(peak, limit, "交叠期实际并发绝不能超过上限")
            self.assertEqual(self.store.running_count("dave"), 0, "额度不得泄漏")
            self.assertEqual(self.store.queued_count("dave"), 0)
        finally:
            stop.set()
            mon.join()

    def test_burst_settle_no_idle_quota_while_queue_nonempty(self) -> None:
        # 有空闲额度时队列不得压着不动：30 个排队任务最终全部被消化，
        # 且全过程峰值不超过上限。
        limit = 3
        n = 30
        self.svc.set_quota("erin", limit)
        self.svc.set_queue_limit("erin", n)
        peak_lock = threading.Lock()
        peak = 0
        stop = threading.Event()

        def monitor() -> None:
            nonlocal peak
            while not stop.is_set():
                used = self.store.running_count("erin")
                with peak_lock:
                    peak = max(peak, used)
                time.sleep(0.001)

        mon = threading.Thread(target=monitor)
        mon.start()
        try:
            jobs = [
                self.svc.submit(
                    "erin",
                    "instrumented_sleep",
                    {"name": f"burst-{i}", "seconds": 0.05},
                    idempotency_key=f"b{i}",
                    wait=True,
                )[0]
                for i in range(n)
            ]
            print(
                f"[burst] 输入: {n} 个 wait 提交, 上限={limit}; "
                f"首批 running={self.store.running_count('erin')} "
                f"queued={self.store.queued_count('erin')}"
            )
            for j in jobs:
                wait_terminal(self.svc, "erin", j.id, timeout=8)
            print(
                f"[burst] 判定依据: {n} 个全部 SUCCEEDED; 观测峰值={peak} "
                f"(必须 == {limit}，说明有空位立即提升) ; running=0 queued=0"
            )
            self.assertEqual(peak, limit, "有空闲额度却压着队列不提升")
            for j in jobs:
                self.assertEqual(
                    self.svc.get("erin", j.id)["status"],
                    JobStatus.SUCCEEDED.value,
                )
            self.assertEqual(self.store.running_count("erin"), 0)
        finally:
            stop.set()
            mon.join()


if __name__ == "__main__":
    unittest.main(verbosity=2)
