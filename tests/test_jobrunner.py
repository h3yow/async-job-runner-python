"""任务系统测试（标准库 unittest，无需额外依赖；结论可重复）。

运行：
    .venv/bin/python -m unittest tests.test_jobrunner -v

每个用例都会打印关键输入（主体/配额/幂等键/载荷）与判定依据，
便于复盘并发竞态下的状态一致性。
"""

from __future__ import annotations

import threading
import time
import unittest

from jobrunner.builtin_handlers import register_builtins
from jobrunner.errors import (
    IdempotencyConflict,
    InvalidStateTransition,
    JobNotFound,
    PermissionDenied,
    QueueFull,
    QuotaExceeded,
)
from jobrunner.handlers import register_handler
from jobrunner.models import JobStatus
from jobrunner.service import JobService
from jobrunner.store import JobStore

register_builtins()

_TERM = {
    JobStatus.SUCCEEDED.value,
    JobStatus.FAILED.value,
    JobStatus.CANCELLED.value,
}


def wait_terminal(
    svc: JobService, owner: str, job_id: str, timeout: float = 5.0
) -> dict:
    deadline = time.monotonic() + timeout
    last: dict = {}
    while time.monotonic() < deadline:
        last = svc.get(owner, job_id)
        if last["status"] in _TERM:
            return last
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not finish in {timeout}s: {last}")


# 用于测试的自定义处理器 -------------------------------------------------- #
_counter_lock = threading.Lock()
_counter: dict[str, int] = {}


def _counting_sleep(payload: dict, cancel_event: threading.Event) -> dict:
    name = payload["name"]
    with _counter_lock:
        _counter[name] = _counter.get(name, 0) + 1
    deadline = time.monotonic() + payload.get("seconds", 0.1)
    while True:
        if cancel_event.is_set():
            from jobrunner.handlers import TaskCancelled

            raise TaskCancelled()
        remain = deadline - time.monotonic()
        if remain <= 0:
            break
        cancel_event.wait(min(0.02, remain))
    return {"ran": name}


register_handler("counting_sleep", _counting_sleep)


# 记录「真正开始执行」先后顺序的处理器，用于验证 FIFO 准入不许插队
_order_lock = threading.Lock()
_start_order: list[str] = []


def _recording_sleep(payload: dict, cancel_event: threading.Event) -> dict:
    name = payload["name"]
    with _order_lock:
        _start_order.append(name)
    deadline = time.monotonic() + payload.get("seconds", 0.1)
    while True:
        if cancel_event.is_set():
            from jobrunner.handlers import TaskCancelled

            raise TaskCancelled()
        remain = deadline - time.monotonic()
        if remain <= 0:
            break
        cancel_event.wait(min(0.02, remain))
    return {"ran": name}


register_handler("recording_sleep", _recording_sleep)


class JobRunnerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = JobStore(default_quota=1)
        self.svc = JobService(store=self.store)

    def tearDown(self) -> None:
        self.svc.shutdown()


class TestSubmitAndStatus(JobRunnerTestBase):
    def test_success_path(self) -> None:
        self.svc.set_quota("alice", 4)
        job, replayed = self.svc.submit(
            "alice", "echo", {"message": "hello"}, idempotency_key="k-1"
        )
        print(
            f"[success] owner=alice key=k-1 -> job={job.id[:8]} replayed={replayed}"
        )
        self.assertFalse(replayed)
        snap = wait_terminal(self.svc, "alice", job.id)
        print(f"[success] 判定依据: status={snap['status']} result={snap['result']}")
        self.assertEqual(snap["status"], JobStatus.SUCCEEDED.value)
        self.assertEqual(snap["result"], {"echo": "hello"})
        self.assertEqual(self.store.running_count("alice"), 0)


class TestIdempotency(JobRunnerTestBase):
    def test_replay_same_job_executes_once(self) -> None:
        self.svc.set_quota("alice", 4)
        job1, r1 = self.svc.submit(
            "alice",
            "counting_sleep",
            {"name": "once", "seconds": 0.2},
            idempotency_key="idem-once",
        )
        job2, r2 = self.svc.submit(
            "alice",
            "counting_sleep",
            {"name": "once", "seconds": 0.2},
            idempotency_key="idem-once",
        )
        print(
            f"[idem-replay] 两次提交相同键/载荷: ids_equal={job1.id == job2.id} "
            f"replays=({r1},{r2})"
        )
        self.assertEqual(job1.id, job2.id)
        self.assertFalse(r1)
        self.assertTrue(r2)
        snap = wait_terminal(self.svc, "alice", job1.id)
        print(
            f"[idem-replay] 判定依据: status={snap['status']} "
            f"处理器实际执行次数={_counter.get('once')}"
        )
        self.assertEqual(snap["status"], JobStatus.SUCCEEDED.value)
        self.assertEqual(_counter.get("once"), 1)

    def test_conflict_different_payload_rejected(self) -> None:
        self.svc.set_quota("alice", 4)
        self.svc.submit(
            "alice", "echo", {"message": "a"}, idempotency_key="idem-x"
        )
        with self.assertRaises(IdempotencyConflict) as ctx:
            self.svc.submit(
                "alice", "echo", {"message": "b"}, idempotency_key="idem-x"
            )
        print(f"[idem-conflict] 同键不同载荷被拒绝: {ctx.exception.message}")
        self.assertEqual(ctx.exception.error_code, "IDEMPOTENCY_CONFLICT")
        # 被拒绝的提交没有产生第二个任务（第一个任务结束后列表仍只有 1 个）
        jobs = self.svc.list_jobs("alice")
        self.assertEqual(len(jobs), 1)
        first = wait_terminal(self.svc, "alice", jobs[0]["id"])
        self.assertEqual(first["status"], JobStatus.SUCCEEDED.value)
        # 原任务正常结束后计数归零；冲突提交从未占用过额度
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_concurrent_same_key_single_execution(self) -> None:
        self.svc.set_quota("alice", 8)
        name = "race-key"
        outcomes: list[tuple[str, bool]] = []
        barrier = threading.Barrier(20)

        def worker() -> None:
            barrier.wait()
            job, replayed = self.svc.submit(
                "alice",
                "counting_sleep",
                {"name": name, "seconds": 0.2},
                idempotency_key="concurrent-key",
            )
            outcomes.append((job.id, replayed))

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        unique_ids = {jid for jid, _ in outcomes}
        created = sum(1 for _, r in outcomes if not r)
        print(
            f"[idem-race] 20 并发同键: 唯一任务数={len(unique_ids)} "
            f"真正创建次数={created} 重放次数={20 - created}"
        )
        self.assertEqual(len(unique_ids), 1)
        self.assertEqual(created, 1)
        only_id = next(iter(unique_ids))
        snap = wait_terminal(self.svc, "alice", only_id)
        print(
            f"[idem-race] 判定依据: status={snap['status']} "
            f"处理器执行次数={_counter.get(name)}"
        )
        self.assertEqual(snap["status"], JobStatus.SUCCEEDED.value)
        self.assertEqual(_counter.get(name), 1)


class TestQuota(JobRunnerTestBase):
    def test_over_quota_rejected_without_consuming(self) -> None:
        self.svc.set_quota("alice", 2)
        a, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.5}, idempotency_key="qa"
        )
        b, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.5}, idempotency_key="qb"
        )
        with self.assertRaises(QuotaExceeded) as ctx:
            self.svc.submit("alice", "echo", {}, idempotency_key="qc")
        print(
            f"[quota] 上限=2, 第三个提交被拒绝 code={ctx.exception.error_code}; "
            f"msg={ctx.exception.message}"
        )
        self.assertEqual(self.store.running_count("alice"), 2)
        wait_terminal(self.svc, "alice", a.id)
        wait_terminal(self.svc, "alice", b.id)
        print("[quota] 判定依据: 两个任务结束后 running 计数归零，额度已释放")
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_quota_race_never_exceeds_limit(self) -> None:
        limit = 3
        n = 30
        self.svc.set_quota("bob", limit)
        accepted: list[str] = []
        rejected = 0
        barrier = threading.Barrier(n)
        lock = threading.Lock()
        max_seen = 0
        stop = threading.Event()

        def monitor() -> None:
            nonlocal max_seen
            while not stop.is_set():
                used = self.store.running_count("bob")
                with lock:
                    max_seen = max(max_seen, used)
                time.sleep(0.002)

        def worker(i: int) -> None:
            nonlocal rejected
            barrier.wait()
            try:
                job, _ = self.svc.submit(
                    "bob",
                    "sleep",
                    {"seconds": 0.25},
                    idempotency_key=f"race-{i}",
                )
                with lock:
                    accepted.append(job.id)
            except QuotaExceeded:
                with lock:
                    rejected += 1

        mon = threading.Thread(target=monitor)
        mon.start()
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        for jid in accepted:
            wait_terminal(self.svc, "bob", jid)
        stop.set()
        mon.join()
        print(
            f"[quota-race] 输入: {n} 并发提交, 上限={limit}; "
            f"接受={len(accepted)} 拒绝={rejected} 观测峰值并发={max_seen}"
        )
        self.assertEqual(len(accepted) + rejected, n)
        self.assertLessEqual(max_seen, limit, "实际并发绝不能超过上限")
        self.assertGreater(rejected, 0)
        self.assertEqual(self.store.running_count("bob"), 0)
        # 额度全部释放后，新的提交必须能成功
        after, _ = self.svc.submit("bob", "echo", {}, idempotency_key="after")
        snap = wait_terminal(self.svc, "bob", after.id)
        self.assertEqual(snap["status"], JobStatus.SUCCEEDED.value)

    def test_failure_and_cancel_release_quota(self) -> None:
        self.svc.set_quota("carol", 2)
        f, _ = self.svc.submit(
            "carol", "fail_biz", {"message": "x"}, idempotency_key="f"
        )
        c, _ = self.svc.submit(
            "carol", "sleep", {"seconds": 2}, idempotency_key="c"
        )
        wait_terminal(self.svc, "carol", f.id)
        self.assertEqual(self.store.running_count("carol"), 1, "失败应释放额度")
        self.svc.cancel("carol", c.id)
        wait_terminal(self.svc, "carol", c.id)
        print("[quota-release] 失败任务 + 取消任务结束后 running 计数应为 0")
        self.assertEqual(self.store.running_count("carol"), 0)


class TestOwnership(JobRunnerTestBase):
    def test_not_found_vs_permission_denied(self) -> None:
        self.svc.set_quota("alice", 2)
        job, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.5}, idempotency_key="mine"
        )
        # 他人任务：必须是 403，而不是 404
        with self.assertRaises(PermissionDenied) as pe:
            self.svc.get("bob", job.id)
        # 不存在的 ID：必须是 404
        with self.assertRaises(JobNotFound) as nf:
            self.svc.get("bob", "deadbeef")
        print(
            f"[ownership] 他人任务 -> {pe.exception.error_code}("
            f"{pe.exception.http_status}); 不存在 -> {nf.exception.error_code}("
            f"{nf.exception.http_status})"
        )
        self.assertEqual(pe.exception.http_status, 403)
        self.assertEqual(nf.exception.http_status, 404)

    def test_cross_owner_cancel_denied_and_key_scoped(self) -> None:
        self.svc.set_quota("alice", 2)
        self.svc.set_quota("bob", 2)
        job, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.6}, idempotency_key="shared-key"
        )
        with self.assertRaises(PermissionDenied):
            self.svc.cancel("bob", job.id)
        snap_alice = self.svc.get("alice", job.id)
        self.assertEqual(snap_alice["status"], JobStatus.RUNNING.value)
        # 幂等键按主体隔离：bob 使用同一个键不算冲突
        bjob, replayed = self.svc.submit(
            "bob", "echo", {}, idempotency_key="shared-key"
        )
        print(
            f"[ownership] bob 越权取消被拒；bob 复用 alice 的幂等键 -> 新任务="
            f"{bjob.id[:8]}, replayed={replayed}"
        )
        self.assertFalse(replayed)
        self.assertNotEqual(bjob.id, job.id)
        wait_terminal(self.svc, "alice", job.id)
        wait_terminal(self.svc, "bob", bjob.id)


class TestCancel(JobRunnerTestBase):
    def test_cancel_running_job(self) -> None:
        self.svc.set_quota("alice", 2)
        job, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 5}, idempotency_key="long"
        )
        time.sleep(0.1)
        snap = self.svc.cancel("alice", job.id)
        print(f"[cancel] 取消请求返回时观察状态={snap['status']}")
        final = wait_terminal(self.svc, "alice", job.id, timeout=2.0)
        print(
            f"[cancel] 判定依据: final={final['status']} "
            f"error_code={final['error_code']} message={final['error_message']}"
        )
        self.assertEqual(final["status"], JobStatus.CANCELLED.value)
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_cancel_terminal_rejected(self) -> None:
        self.svc.set_quota("alice", 2)
        job, _ = self.svc.submit(
            "alice", "echo", {"message": "x"}, idempotency_key="done"
        )
        final = wait_terminal(self.svc, "alice", job.id)
        with self.assertRaises(InvalidStateTransition) as ctx:
            self.svc.cancel("alice", job.id)
        print(
            f"[cancel-terminal] 终态={final['status']} 再取消 -> "
            f"{ctx.exception.error_code}: {ctx.exception.message}"
        )
        self.assertEqual(ctx.exception.http_status, 409)
        # 终态保持不变
        self.assertEqual(
            self.svc.get("alice", job.id)["status"],
            JobStatus.SUCCEEDED.value,
        )


class TestTimeoutAndErrors(JobRunnerTestBase):
    def test_timeout_enters_failed_with_timeout_category(self) -> None:
        self.svc.set_quota("alice", 2)
        job, _ = self.svc.submit(
            "alice",
            "sleep",
            {"seconds": 5},
            idempotency_key="timeout",
            timeout=0.2,
        )
        final = wait_terminal(self.svc, "alice", job.id, timeout=2.0)
        print(
            f"[timeout] 输入 timeout=0.2s, 任务需 5s -> "
            f"status={final['status']} category={final['error_category']} "
            f"code={final['error_code']}"
        )
        self.assertEqual(final["status"], JobStatus.FAILED.value)
        self.assertEqual(final["error_category"], "TIMEOUT")
        self.assertEqual(final["error_code"], "JOB_TIMEOUT")
        # 超时同样释放额度
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_business_vs_system_error_separated(self) -> None:
        self.svc.set_quota("alice", 4)
        biz, _ = self.svc.submit(
            "alice", "fail_biz", {"message": "bad input"}, idempotency_key="biz"
        )
        sysj, _ = self.svc.submit("alice", "boom", {}, idempotency_key="sys")
        fb = wait_terminal(self.svc, "alice", biz.id)
        fs = wait_terminal(self.svc, "alice", sysj.id)
        print(
            f"[errors] 业务错误 -> category={fb['error_category']} "
            f"code={fb['error_code']}; 系统错误 -> category={fs['error_category']} "
            f"code={fs['error_code']}"
        )
        self.assertEqual(fb["status"], JobStatus.FAILED.value)
        self.assertEqual(fb["error_category"], "BUSINESS")
        self.assertEqual(fs["error_category"], "SYSTEM")
        self.assertEqual(fs["error_code"], "SYSTEM_UNCAUGHT_EXCEPTION")
        self.assertNotEqual(
            fb["error_category"], fs["error_category"], "两类错误不得混同"
        )
        self.assertEqual(self.store.running_count("alice"), 0)


class TestQueueing(JobRunnerTestBase):
    """排队等待模式：queue=True 时并发占满不拒绝，而是 FIFO 排队等待准入。"""

    def test_fifo_admission_order_no_overtake(self) -> None:
        self.svc.set_quota("alice", 1)
        blocker, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.3}, idempotency_key="blocker"
        )
        queued = []
        for i in range(3):
            job, _ = self.svc.submit(
                "alice",
                "recording_sleep",
                {"name": f"q{i}", "seconds": 0.1},
                idempotency_key=f"q{i}",
                queue=True,
            )
            queued.append(job)
        # 等待中的任务可查询、状态明确为 QUEUED，且不占运行额度
        for j in queued:
            snap = self.svc.get("alice", j.id)
            self.assertEqual(snap["status"], JobStatus.QUEUED.value)
        print(
            f"[fifo] 输入: 上限=1, 1 个在跑 + 3 个排队(q0,q1,q2); "
            f"提交时 running={self.store.running_count('alice')} "
            f"queued={self.store.queued_count('alice')}"
        )
        self.assertEqual(self.store.running_count("alice"), 1)
        self.assertEqual(self.store.queued_count("alice"), 3)

        wait_terminal(self.svc, "alice", blocker.id)
        for j in queued:
            snap = wait_terminal(self.svc, "alice", j.id)
            self.assertEqual(snap["status"], JobStatus.SUCCEEDED.value)
        with _order_lock:
            order = [n for n in _start_order if n.startswith("q")]
        print(
            f"[fifo] 判定依据: 实际执行先后顺序={order}，"
            f"必须严格等于提交先后 [q0, q1, q2]，不许插队"
        )
        self.assertEqual(order, ["q0", "q1", "q2"])
        self.assertEqual(self.store.running_count("alice"), 0)
        self.assertEqual(self.store.queued_count("alice"), 0)

    def test_cancel_queued_never_runs_and_consumes_nothing(self) -> None:
        self.svc.set_quota("alice", 1)
        running, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.4}, idempotency_key="run"
        )
        queued, _ = self.svc.submit(
            "alice",
            "counting_sleep",
            {"name": "never-ran", "seconds": 0.1},
            idempotency_key="q1",
            queue=True,
        )
        self.assertEqual(
            self.svc.get("alice", queued.id)["status"], JobStatus.QUEUED.value
        )
        snap = self.svc.cancel("alice", queued.id)
        print(
            f"[cancel-queued] 排队中取消: 返回状态={snap['status']} "
            f"running={self.store.running_count('alice')} "
            f"queued={self.store.queued_count('alice')}"
        )
        # 立即退出等待并落终态；排队本就不占额度，取消也不触碰计数
        self.assertEqual(snap["status"], JobStatus.CANCELLED.value)
        self.assertEqual(self.store.running_count("alice"), 1)
        self.assertEqual(self.store.queued_count("alice"), 0)
        wait_terminal(self.svc, "alice", running.id)
        # 给准入/执行留出时间窗，确认被取消的排队任务永远没有被执行
        time.sleep(0.3)
        final = self.svc.get("alice", queued.id)
        print(
            f"[cancel-queued] 判定依据: final={final['status']} "
            f"started_at={final['started_at']} "
            f"处理器执行次数={_counter.get('never-ran', 0)}（必须为 0）"
        )
        self.assertEqual(final["status"], JobStatus.CANCELLED.value)
        self.assertIsNone(final["started_at"])
        self.assertEqual(_counter.get("never-ran", 0), 0)
        self.assertEqual(self.store.running_count("alice"), 0)
        # 已出最终结果的任务再取消，仍按老规则拒绝
        with self.assertRaises(InvalidStateTransition):
            self.svc.cancel("alice", queued.id)

    def test_queue_full_rejected_without_consuming(self) -> None:
        self.svc.set_quota("alice", 1)
        self.svc.set_queue_capacity("alice", 1)
        running, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.4}, idempotency_key="run"
        )
        q1, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.1}, idempotency_key="q1", queue=True
        )
        with self.assertRaises(QueueFull) as ctx:
            self.svc.submit(
                "alice", "echo", {}, idempotency_key="q2", queue=True
            )
        print(
            f"[queue-full] 容量=1 且已有 1 个排队: 新排队提交被拒 "
            f"code={ctx.exception.error_code}; msg={ctx.exception.message}"
        )
        self.assertEqual(ctx.exception.http_status, 429)
        # 被拒的提交不创建任务、不占额度、不占队列位置
        self.assertEqual(self.store.running_count("alice"), 1)
        self.assertEqual(self.store.queued_count("alice"), 1)
        self.assertEqual(len(self.svc.list_jobs("alice")), 2)
        # 老行为保留：queue=False 超额依旧直接拒绝
        with self.assertRaises(QuotaExceeded):
            self.svc.submit("alice", "echo", {}, idempotency_key="q3")
        print("[queue-full] 老行为判定: queue=False 超额仍抛 QUOTA_EXCEEDED")
        wait_terminal(self.svc, "alice", running.id)
        wait_terminal(self.svc, "alice", q1.id)
        self.assertEqual(self.store.running_count("alice"), 0)

    def test_queued_wait_time_not_counted_in_timeout(self) -> None:
        self.svc.set_quota("alice", 1)
        blocker, _ = self.svc.submit(
            "alice", "sleep", {"seconds": 0.5}, idempotency_key="blocker"
        )
        # 排队等待 0.5s，远超自身 timeout=0.2s；若等待计入超时必失败
        queued, _ = self.svc.submit(
            "alice",
            "sleep",
            {"seconds": 0.2},
            idempotency_key="q-timeout",
            timeout=0.4,
            queue=True,
        )
        self.assertEqual(
            self.svc.get("alice", queued.id)["status"], JobStatus.QUEUED.value
        )
        wait_terminal(self.svc, "alice", blocker.id)
        final = wait_terminal(self.svc, "alice", queued.id, timeout=3.0)
        waited = final["started_at"] - final["created_at"]
        print(
            f"[queued-timeout] 输入: 排队等待={waited:.2f}s, timeout=0.4s; "
            f"判定依据: status={final['status']}（超时须从 RUNNING 起算，"
            f"等待时长不计入，故应 SUCCEEDED）"
        )
        self.assertEqual(final["status"], JobStatus.SUCCEEDED.value)
        self.assertGreater(waited, 0.3, "确实经历了排队等待")

    def test_peak_concurrency_with_timeout_cancel_and_admission(self) -> None:
        limit = 2
        self.svc.set_quota("dave", limit)
        self.svc.set_queue_capacity("dave", 16)
        max_seen = 0
        stop = threading.Event()
        lock = threading.Lock()

        def monitor() -> None:
            nonlocal max_seen
            while not stop.is_set():
                used = self.store.running_count("dave")
                with lock:
                    max_seen = max(max_seen, used)
                time.sleep(0.001)

        mon = threading.Thread(target=monitor)
        mon.start()
        # 一个必超时（timeout=0.2 但需睡 5s），一个将被取消
        t_job, _ = self.svc.submit(
            "dave", "sleep", {"seconds": 5}, idempotency_key="t", timeout=0.2
        )
        c_job, _ = self.svc.submit(
            "dave", "sleep", {"seconds": 5}, idempotency_key="c"
        )
        queued_ids = []
        for i in range(6):
            job, _ = self.svc.submit(
                "dave",
                "sleep",
                {"seconds": 0.1},
                idempotency_key=f"q{i}",
                queue=True,
            )
            queued_ids.append(job.id)
        self.assertEqual(self.store.queued_count("dave"), 6)
        # 取消一个运行中任务：与超时释放、准入补位密集交叠
        self.svc.cancel("dave", c_job.id)
        finals = [wait_terminal(self.svc, "dave", jid) for jid in queued_ids]
        tf = wait_terminal(self.svc, "dave", t_job.id)
        cf = wait_terminal(self.svc, "dave", c_job.id)
        stop.set()
        mon.join()
        statuses = [f["status"] for f in finals]
        print(
            f"[peak] 输入: 上限={limit}, 1 超时 + 1 取消 + 6 排队; "
            f"观测峰值并发={max_seen}, 排队任务终态={statuses}, "
            f"超时任务={tf['status']}/{tf['error_category']}, "
            f"取消任务={cf['status']}"
        )
        self.assertLessEqual(max_seen, limit, "并发峰值绝不能超过上限")
        self.assertEqual(tf["status"], JobStatus.FAILED.value)
        self.assertEqual(tf["error_category"], "TIMEOUT")
        self.assertEqual(cf["status"], JobStatus.CANCELLED.value)
        # 额度腾出后排队任务必须全部起来并成功，不许压着不动
        self.assertEqual(statuses, [JobStatus.SUCCEEDED.value] * 6)
        self.assertEqual(self.store.running_count("dave"), 0)
        self.assertEqual(self.store.queued_count("dave"), 0)


class TestHTTPAPI(unittest.TestCase):
    """通过 ASGI 测试客户端验证 HTTP 语义（404/403/409/429 可区分）。"""

    def setUp(self) -> None:
        from fastapi.testclient import TestClient

        from jobrunner.api import create_app

        self.svc = JobService(store=JobStore(default_quota=1))
        self.client = TestClient(create_app(self.svc))

    def tearDown(self) -> None:
        self.svc.shutdown()

    def test_http_flow_and_distinguishable_errors(self) -> None:
        h = {"X-Owner-Id": "alice"}
        r = self.client.post(
            "/jobs",
            headers=h,
            json={
                "handler": "sleep",
                "payload": {"seconds": 0.5},
                "idempotency_key": "http-1",
            },
        )
        self.assertEqual(r.status_code, 201, r.text)
        job_id = r.json()["id"]
        print(f"[http] POST /jobs -> 201 job={job_id[:8]}")

        # 重放返回同一任务
        r2 = self.client.post(
            "/jobs",
            headers=h,
            json={
                "handler": "sleep",
                "payload": {"seconds": 0.5},
                "idempotency_key": "http-1",
            },
        )
        self.assertEqual(r2.json()["id"], job_id)
        self.assertTrue(r2.json()["idempotent_replay"])

        # 配额：429
        rq = self.client.post(
            "/jobs",
            headers=h,
            json={"handler": "echo", "payload": {}, "idempotency_key": "http-2"},
        )
        print(f"[http] 超额提交 -> {rq.status_code} {rq.json()['error']['code']}")
        self.assertEqual(rq.status_code, 429)

        # 越权：403
        rb = self.client.get(f"/jobs/{job_id}", headers={"X-Owner-Id": "bob"})
        self.assertEqual(rb.status_code, 403)
        # 不存在：404
        rn = self.client.get("/jobs/nope", headers={"X-Owner-Id": "bob"})
        print(
            f"[http] 越权={rb.status_code}/{rb.json()['error']['code']} "
            f"不存在={rn.status_code}/{rn.json()['error']['code']}"
        )
        self.assertEqual(rn.status_code, 404)

        # 幂等冲突：409
        rc = self.client.post(
            "/jobs",
            headers=h,
            json={
                "handler": "sleep",
                "payload": {"seconds": 9},
                "idempotency_key": "http-1",
            },
        )
        self.assertEqual(rc.status_code, 409)
        print(f"[http] 幂等冲突 -> {rc.status_code} {rc.json()['error']['message']}")

        # 取消
        rcx = self.client.post(f"/jobs/{job_id}/cancel", headers=h)
        self.assertIn(rcx.status_code, (200,))
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            g = self.client.get(f"/jobs/{job_id}", headers=h).json()
            if g["status"] in _TERM:
                break
            time.sleep(0.02)
        print(f"[http] 取消后终态={g['status']}")
        self.assertEqual(g["status"], "CANCELLED")
        # 终态再取消：409
        rag = self.client.post(f"/jobs/{job_id}/cancel", headers=h)
        self.assertEqual(rag.status_code, 409)

    def test_http_queue_mode(self) -> None:
        h = {"X-Owner-Id": "alice"}
        self.svc.set_queue_capacity("alice", 1)
        # 默认配额 1：第一个直接运行
        r1 = self.client.post(
            "/jobs",
            headers=h,
            json={
                "handler": "sleep",
                "payload": {"seconds": 0.4},
                "idempotency_key": "hq-1",
            },
        )
        self.assertEqual(r1.status_code, 201, r1.text)
        # queue=true：占满后进入 QUEUED 而不是 429
        r2 = self.client.post(
            "/jobs",
            headers=h,
            json={
                "handler": "sleep",
                "payload": {"seconds": 0.1},
                "idempotency_key": "hq-2",
                "queue": True,
            },
        )
        self.assertEqual(r2.status_code, 201, r2.text)
        queued_id = r2.json()["id"]
        g = self.client.get(f"/jobs/{queued_id}", headers=h).json()
        print(f"[http-queue] queue=true 提交后状态={g['status']}（应为 QUEUED）")
        self.assertEqual(g["status"], "QUEUED")
        # 队列容量=1 已满：再排队 -> 429 QUEUE_FULL
        r3 = self.client.post(
            "/jobs",
            headers=h,
            json={"handler": "echo", "payload": {}, "idempotency_key": "hq-3",
                  "queue": True},
        )
        print(
            f"[http-queue] 队列满 -> {r3.status_code} "
            f"{r3.json()['error']['code']}"
        )
        self.assertEqual(r3.status_code, 429)
        self.assertEqual(r3.json()["error"]["code"], "QUEUE_FULL")
        # 等待中的任务可取消，立即落 CANCELLED
        rc = self.client.post(f"/jobs/{queued_id}/cancel", headers=h)
        self.assertEqual(rc.status_code, 200)
        self.assertEqual(rc.json()["status"], "CANCELLED")
        print(f"[http-queue] 排队取消 -> {rc.json()['status']}")
        # 第一个任务正常结束
        deadline = time.monotonic() + 3
        first_id = r1.json()["id"]
        while time.monotonic() < deadline:
            g1 = self.client.get(f"/jobs/{first_id}", headers=h).json()
            if g1["status"] in _TERM:
                break
            time.sleep(0.02)
        self.assertEqual(g1["status"], "SUCCEEDED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
