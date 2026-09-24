"""HTTP API 层测试。

使用 FastAPI TestClient + 线程池模拟多主体并发 HTTP 提交；
每个用例打印关键请求输入与判定依据，结论可重复。
"""

from __future__ import annotations

import logging
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from jobrunner.api import create_app
from jobrunner.manager import JobManager

log = logging.getLogger("jobrunner.tests")


def poll(client: TestClient, jid: str, owner: str, *statuses: str,
         timeout: float = 5.0, interval: float = 0.01) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/jobs/{jid}", headers={"X-Principal": owner})
        body = r.json()["job"]
        if body["status"] in statuses:
            return body
        time.sleep(interval)
    raise AssertionError(f"{jid} never reached {statuses}; last={body['status']}")


class ApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = JobManager()
        # 作为上下文管理器使用，保证后台任务跨请求存活
        self._ctx = TestClient(create_app(self.manager))
        self.client = self._ctx.__enter__()

    def tearDown(self) -> None:
        self._ctx.__exit__(None, None, None)


class AuthAndRoutingTests(ApiTestCase):
    def test_missing_principal_is_401(self) -> None:
        r = self.client.post("/jobs", json={"type": "succeed", "payload": {}})
        log.info("INPUT no-header -> ASSERT status==401, got %s", r.status_code)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["error"]["code"], "unauthenticated")

    def test_404_vs_403_distinct(self) -> None:
        r = self.client.post(
            "/jobs", json={"type": "succeed", "payload": {}},
            headers={"X-Principal": "alice"},
        )
        jid = r.json()["job"]["id"]
        poll(self.client, jid, "alice", "SUCCEEDED")

        r403 = self.client.get(f"/jobs/{jid}", headers={"X-Principal": "bob"})
        r404 = self.client.get("/jobs/ghost-id", headers={"X-Principal": "alice"})
        log.info("ASSERT forbidden=%s notfound=%s", r403.status_code, r404.status_code)
        self.assertEqual(r403.status_code, 403)
        self.assertEqual(r403.json()["error"]["code"], "permission_denied")
        self.assertEqual(r404.status_code, 404)
        self.assertEqual(r404.json()["error"]["code"], "job_not_found")

    def test_cross_owner_cancel_denied(self) -> None:
        r = self.client.post(
            "/jobs", json={"type": "sleep", "payload": {"seconds": 5}, "timeout": 10},
            headers={"X-Principal": "alice"},
        )
        jid = r.json()["job"]["id"]
        poll(self.client, jid, "alice", "RUNNING")
        r = self.client.post(f"/jobs/{jid}/cancel", headers={"X-Principal": "bob"})
        self.assertEqual(r.status_code, 403)
        # 取消被拒绝后任务仍正常运行，最后由属主取消
        body = self.client.get(f"/jobs/{jid}", headers={"X-Principal": "alice"}).json()["job"]
        self.assertEqual(body["status"], "RUNNING")
        self.client.post(f"/jobs/{jid}/cancel", headers={"X-Principal": "alice"})
        final = poll(self.client, jid, "alice", "CANCELLED")
        log.info("ASSERT cross-owner cancel rejected, final=%s", final["status"])


class IdempotencyApiTests(ApiTestCase):
    def test_replay_200_and_conflict_409_and_header_key(self) -> None:
        payload = {"type": "succeed", "payload": {"v": 9}, "idempotency_key": "K1"}
        r1 = self.client.post("/jobs", json=payload, headers={"X-Principal": "alice"})
        jid = r1.json()["job"]["id"]
        r2 = self.client.post("/jobs", json=payload, headers={"X-Principal": "alice"})
        log.info(
            "INPUT same key/payload twice -> ASSERT 201 then 200 replayed; got %s/%s ids_equal=%s",
            r1.status_code, r2.status_code,
            r1.json()["job"]["id"] == r2.json()["job"]["id"],
        )
        self.assertEqual((r1.status_code, r2.status_code), (201, 200))
        self.assertTrue(r2.json()["replayed"])
        self.assertEqual(r2.json()["job"]["id"], jid)

        bad = {**payload, "payload": {"v": 10}}
        r3 = self.client.post("/jobs", json=bad, headers={"X-Principal": "alice"})
        self.assertEqual(r3.status_code, 409)

        # 通过 Idempotency-Key 请求头提交同样生效
        r4 = self.client.post(
            "/jobs", json={"type": "succeed", "payload": {"v": 1}},
            headers={"X-Principal": "alice", "Idempotency-Key": "HDR-K"},
        )
        r5 = self.client.post(
            "/jobs", json={"type": "succeed", "payload": {"v": 1}},
            headers={"X-Principal": "alice", "Idempotency-Key": "HDR-K"},
        )
        self.assertEqual(r4.status_code, 201)
        self.assertEqual(r5.status_code, 200)
        self.assertEqual(r4.json()["job"]["id"], r5.json()["job"]["id"])
        poll(self.client, r4.json()["job"]["id"], "alice", "SUCCEEDED")


class FailureClassificationApiTests(ApiTestCase):
    def test_business_crash_timeout_classified(self) -> None:
        def submit(kind, payload, timeout=None):
            body = {"type": kind, "payload": payload}
            if timeout is not None:
                body["timeout"] = timeout
            return self.client.post("/jobs", json=body, headers={"X-Principal": "alice"})

        r_biz = submit("fail", {"message": "bad-input"})
        biz = poll(self.client, r_biz.json()["job"]["id"], "alice", "FAILED")
        log.info("ASSERT business: status=%s reason=%s code=%s",
                 biz["status"], biz["fail_reason"], biz["error_code"])
        self.assertEqual((biz["status"], biz["fail_reason"], biz["error_code"]),
                         ("FAILED", "business", "business_failure"))

        r_crash = submit("crash", {"message": "boom"})
        crash = poll(self.client, r_crash.json()["job"]["id"], "alice", "FAILED")
        log.info("ASSERT crash: reason=%s code=%s msg=%s",
                 crash["fail_reason"], crash["error_code"], crash["error_message"])
        self.assertEqual((crash["fail_reason"], crash["error_code"]),
                         ("crash", "RuntimeError"))

        r_to = submit("sleep", {"seconds": 1}, timeout=0.1)
        timed_out = poll(self.client, r_to.json()["job"]["id"], "alice", "FAILED")
        log.info("ASSERT timeout: reason=%s code=%s",
                 timed_out["fail_reason"], timed_out["error_code"])
        self.assertEqual(timed_out["fail_reason"], "timeout")


class ConcurrentApiTests(ApiTestCase):
    def test_concurrent_same_key_only_one_job(self) -> None:
        def submit(_i: int):
            return self.client.post(
                "/jobs",
                json={"type": "succeed", "payload": {"v": 1}, "idempotency_key": "DUP"},
                headers={"X-Principal": "alice"},
            )

        with ThreadPoolExecutor(max_workers=16) as pool:
            responses = list(pool.map(submit, range(32)))
        statuses = sorted(r.status_code for r in responses)
        ids = {r.json()["job"]["id"] for r in responses}
        log.info(
            "INPUT 32 concurrent same-key POSTs -> ASSERT one job id; created(201)=%s",
            statuses.count(201),
        )
        self.assertEqual(len(ids), 1)
        self.assertEqual(statuses.count(201), 1)
        self.assertEqual(statuses.count(200), 31)
        final = poll(self.client, next(iter(ids)), "alice", "SUCCEEDED")
        self.assertEqual(final["attempt"], 1)

    def test_concurrent_submits_respect_quota_and_release(self) -> None:
        quota, n = 3, 30

        def submit(i: int):
            return self.client.post(
                "/jobs",
                json={"type": "sleep", "payload": {"seconds": 0.05 + 0.01 * (i % 4)},
                      "timeout": 5, "quota": quota},
                headers={"X-Principal": "carol"},
            )

        with ThreadPoolExecutor(max_workers=16) as pool:
            responses = list(pool.map(submit, range(n)))
        created = sum(1 for r in responses if r.status_code == 201)
        rejected = sum(1 for r in responses if r.status_code == 429)
        log.info(
            "INPUT %s concurrent POSTs quota=%s -> ASSERT accepted==quota; got accepted=%s rejected=%s",
            n, quota, created, rejected,
        )
        self.assertEqual(created, quota)
        self.assertEqual(rejected, n - quota)
        # 被拒绝的响应必须明确是配额原因，且所有任务终态后额度归零
        for r in responses:
            if r.status_code == 429:
                self.assertEqual(r.json()["error"]["code"], "quota_exceeded")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.manager._running.get("carol", 0) == 0:
                break
            time.sleep(0.01)
        log.info("ASSERT slots drained, running=%s", dict(self.manager._running))
        self.assertEqual(self.manager._running.get("carol", 0), 0)

    def test_multi_owner_concurrent_isolation(self) -> None:
        def submit(task: tuple[str, str]):
            owner, key = task
            return self.client.post(
                "/jobs",
                json={"type": "succeed", "payload": {"k": key}, "idempotency_key": key},
                headers={"X-Principal": owner},
            )

        tasks = [("alice", "shared-key"), ("bob", "shared-key")] * 10
        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(submit, tasks))
        by_owner: dict[str, set[str]] = {}
        for r in responses:
            body = r.json()["job"]
            by_owner.setdefault(body["owner"], set()).add(body["id"])
        log.info("ASSERT per-owner idempotent isolation: %s",
                 {k: len(v) for k, v in by_owner.items()})
        self.assertEqual({k: len(v) for k, v in by_owner.items()},
                         {"alice": 1, "bob": 1})


class CancelApiTests(ApiTestCase):
    def test_cancel_running_and_cancel_terminal_409(self) -> None:
        r = self.client.post(
            "/jobs", json={"type": "sleep", "payload": {"seconds": 5}, "timeout": 10},
            headers={"X-Principal": "alice"},
        )
        jid = r.json()["job"]["id"]
        poll(self.client, jid, "alice", "RUNNING")
        r = self.client.post(f"/jobs/{jid}/cancel", headers={"X-Principal": "alice"})
        self.assertIn(r.json()["job"]["status"], ("RUNNING", "CANCELLED"))
        final = poll(self.client, jid, "alice", "CANCELLED", timeout=2)
        log.info("ASSERT running cancel settles: status=%s", final["status"])
        # 终态再取消：409
        r2 = self.client.post(f"/jobs/{jid}/cancel", headers={"X-Principal": "alice"})
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(r2.json()["error"]["code"], "terminal_state")
        log.info("ASSERT cancel terminal -> 409 %s", r2.json()["error"]["code"])

    def test_invalid_type_400_and_validation_422(self) -> None:
        r = self.client.post(
            "/jobs", json={"type": "ghost", "payload": {}},
            headers={"X-Principal": "alice"},
        )
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.json()["error"]["code"], "invalid_request")
        r2 = self.client.post("/jobs", json={"payload": {}}, headers={"X-Principal": "alice"})
        self.assertEqual(r2.status_code, 422)  # pydantic 请求体校验
        log.info("ASSERT invalid type=400, missing field=422")


if __name__ == "__main__":
    unittest.main(verbosity=2)
