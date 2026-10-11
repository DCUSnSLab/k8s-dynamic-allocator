"""요청 파드 경로가 쓰는 큐 조회(파드 없는 claim, 생성 예약).

Redis 서버 없이 가짜 클라이언트로 확인한다.
"""

import importlib.util
import sys
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

CONTROLLER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CONTROLLER / "rest_api"))
QUEUE_DIR = CONTROLLER / "services" / "queue"
PACKAGE_NAME = "direct_queue_test_package"
queue_package = types.ModuleType(PACKAGE_NAME)
queue_package.__path__ = [str(QUEUE_DIR)]
sys.modules[PACKAGE_NAME] = queue_package
_spec = importlib.util.spec_from_file_location(
    f"{PACKAGE_NAME}.compute_queues", QUEUE_DIR / "compute_queues.py"
)
queue_module = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = queue_module
_spec.loader.exec_module(queue_module)


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.ops = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def hgetall(self, key):
        self.ops.append(("hgetall", key))

    def hmget(self, key, *fields):
        self.ops.append(("hmget", key, fields))

    def hget(self, key, field):
        self.ops.append(("hget", key, field))

    def zadd(self, key, mapping, xx=False):
        self.ops.append(("zadd", key, mapping, xx))

    def zscore(self, key, member):
        self.ops.append(("zscore", key, member))

    def zremrangebyscore(self, key, low, high):
        self.ops.append(("zremrangebyscore", key, high))

    def zrange(self, key, start, end):
        self.ops.append(("zrange", key))

    def execute(self):
        self.client.round_trips += 1
        out = []
        for op in self.ops:
            data = self.client.hashes.get(op[1], {})
            if op[0] == "zadd":
                zset = self.client.zsets.setdefault(op[1], {})
                changed = 0
                for member, score in op[2].items():
                    if op[3] and member not in zset:
                        continue
                    changed += int(zset.get(member) != score)
                    zset[member] = score
                out.append(changed)
            elif op[0] == "zremrangebyscore":
                zset = self.client.zsets.setdefault(op[1], {})
                gone = [m for m, score in zset.items() if score <= op[2]]
                for m in gone:
                    del zset[m]
                out.append(len(gone))
            elif op[0] == "zrange":
                zset = self.client.zsets.get(op[1], {})
                out.append(sorted(zset, key=lambda m: zset[m]))
            elif op[0] == "zscore":
                out.append(self.client.zsets.get(op[1], {}).get(op[2]))
            elif op[0] == "hgetall":
                out.append(dict(data))
            elif op[0] == "hget":
                out.append(data.get(op[2]))
            else:
                out.append([data.get(f) for f in op[2]])
        return out


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.queue = {}
        self.active = set()
        self.round_trips = 0
        self.zrank_calls = 0
        self.zsets = {}
        self.transactions = []

    def zrange(self, _key, _start, _end):
        return sorted(self.queue, key=lambda ticket_id: self.queue[ticket_id])

    def zrank(self, _key, _member):
        self.zrank_calls += 1
        return None

    def smembers(self, _key):
        return set(self.active)

    def pipeline(self, transaction=False):
        self.transactions.append(transaction)
        return FakePipeline(self)


def make_queue():
    queue = queue_module.ComputeQueues(prefix="direct-test")
    client = FakeRedis()
    queue._redis_client = lambda: client
    return queue, client


def put(queue, client, ticket_id, *, queued=False, **fields):
    client.hashes[queue._ticket_key(ticket_id)] = {k: str(v) for k, v in fields.items()}
    client.active.add(ticket_id)
    if queued:
        client.queue[ticket_id] = len(client.queue)


class DirectQueueTests(unittest.TestCase):
    def test_allocating_missing_pods_reads_once_and_skips_positions(self):
        queue, client = make_queue()
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
        put(queue, client, "no-name", status="allocating", compute_pod="", allocation_deadline=future)
        put(queue, client, "listed", status="allocating", compute_pod="p-listed", allocation_deadline=future)
        put(queue, client, "unlisted", status="allocating", compute_pod="p-new", allocation_deadline=future)
        put(queue, client, "stale", status="allocating", compute_pod="", allocation_deadline=past)
        for index in range(20):
            put(queue, client, f"q{index}", queued=True, status="queued")
        missing = queue.allocating_missing_pods({"p-listed"}, "general")
        self.assertEqual(sorted(missing), ["", "p-new"])
        self.assertEqual(client.round_trips, 1)
        # 대기 티켓의 큐 위치를 읽지 않는다(포화에서 대기 수만큼 왕복이 늘던 것).
        self.assertEqual(client.zrank_calls, 0)


class PendingCreateRefreshTests(unittest.TestCase):
    def test_refresh_keeps_only_reservations_that_still_exist(self):
        queue, client = make_queue()
        key = queue._pending_create_key("general", "request")
        client.zsets[key] = {"pod-a": 100.0}
        with patch.object(time, "time", return_value=200.0):
            alive = queue.refresh_pending_creates("general", ["pod-a", "pod-gone"], "request")
        self.assertEqual(alive, {"pod-a"})
        self.assertEqual(client.zsets[key], {"pod-a": 200.0})  # 지워진 것을 되살리지 않는다
        self.assertEqual(client.round_trips, 1)
        self.assertEqual(client.transactions, [True])

    def test_pending_creates_returns_kind_and_drops_expired(self):
        # 이 arm 은 request 만 올리지만, 다른 arm 이 남긴 idle 예약도 만료 전까지 센다.
        queue, client = make_queue()
        client.zsets[queue._pending_create_key("general", "idle")] = {"pod-a": 950.0, "pod-old": 100.0}
        client.zsets[queue._pending_create_key("general", "request")] = {"pod-b": 990.0}
        with patch.object(time, "time", return_value=1000.0):
            pending = queue.pending_creates("general", 60.0)
        self.assertEqual(pending, {"pod-a": "idle", "pod-b": "request"})
        self.assertNotIn("pod-old", client.zsets[queue._pending_create_key("general", "idle")])
        self.assertEqual(client.round_trips, 1)

    def test_same_score_refresh_still_counts_as_alive(self):
        """ZADD XX CH 는 점수가 같으면 0 을 돌려준다. 그것을 예약 상실로 보면 안 된다."""
        queue, client = make_queue()
        key = queue._pending_create_key("general", "request")
        client.zsets[key] = {"pod-a": 300.0}
        with patch.object(time, "time", return_value=300.0):
            alive = queue.refresh_pending_creates("general", ["pod-a"], "request")
        self.assertEqual(alive, {"pod-a"})


if __name__ == "__main__":
    unittest.main()
