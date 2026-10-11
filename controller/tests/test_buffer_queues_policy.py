"""버퍼 직접 관리가 새로 쓰는 큐 조회와 정책(BufferPolicy) 계산.

큐는 Redis 서버 없이 가짜 클라이언트로, 정책은 모듈만 따로 읽어 확인한다.
"""

import importlib.util
import sys
import time
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
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


def load_policy_class():
    config = types.ModuleType("config")
    config.settings = SimpleNamespace(
        BUFFER_SCALE_DOWN_COOLDOWN_SAMPLES=20,
        BUFFER_SCALE_DOWN_COOLDOWN_MIN_SAMPLES=3,
    )
    spec = importlib.util.spec_from_file_location(
        "buffer_policy_under_test", CONTROLLER / "services" / "compute" / "buffer_policy.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"config": config}):
        spec.loader.exec_module(module)
    return module.BufferPolicy


BufferPolicy = load_policy_class()


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
    def test_claimable_count_skips_silent_clients_and_non_queued(self):
        queue, client = make_queue()
        now_ms = int(time.time() * 1000)
        silent = (queue.client_timeout_seconds + 30) * 1000
        put(queue, client, "a", queued=True, status="queued", last_poll_ms=now_ms)
        put(queue, client, "b", queued=True, status="queued", last_poll_ms=now_ms - silent)
        put(queue, client, "c", queued=True, status="allocating", last_poll_ms=now_ms)
        self.assertEqual(queue.queued_count("general"), 2)
        self.assertEqual(queue.claimable_queued_count("general"), 1)
        self.assertEqual(client.round_trips, 2)

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
        key = queue._pending_create_key("general")
        client.zsets[key] = {"warm-a": 100.0}
        with patch.object(time, "time", return_value=200.0):
            alive = queue.refresh_pending_creates("general", ["warm-a", "warm-gone"])
        self.assertEqual(alive, {"warm-a"})
        self.assertEqual(client.zsets[key], {"warm-a": 200.0})  # 지워진 것을 되살리지 않는다
        self.assertEqual(client.round_trips, 1)
        self.assertEqual(client.transactions, [True])

    def test_same_score_refresh_still_counts_as_alive(self):
        """ZADD XX CH 는 점수가 같으면 0 을 돌려준다. 그것을 예약 상실로 보면 안 된다."""
        queue, client = make_queue()
        key = queue._pending_create_key("general")
        client.zsets[key] = {"warm-a": 300.0}
        with patch.object(time, "time", return_value=300.0):
            alive = queue.refresh_pending_creates("general", ["warm-a"])
        self.assertEqual(alive, {"warm-a"})


class BufferPolicyTests(unittest.TestCase):
    def make(self):
        queues = SimpleNamespace(
            churn_samples=lambda *a: (_ for _ in ()).throw(RuntimeError("no shared")),
            record_churn_sample=lambda *a, **k: None,
        )
        return BufferPolicy(queues)

    def test_reserve_is_the_floor_and_demand_raises_it(self):
        self.assertEqual(BufferPolicy.desired_detail(2, 40, 10, 0), {"desired": 2, "bound_by": "reserve"})
        self.assertEqual(BufferPolicy.desired_detail(2, 40, 10, 7), {"desired": 7, "bound_by": "demand"})

    def test_room_bounds_the_target(self):
        self.assertEqual(BufferPolicy.desired_detail(2, 40, 38, 7), {"desired": 2, "bound_by": "room"})
        self.assertEqual(BufferPolicy.desired_detail(2, 40, 40, 7), {"desired": 0, "bound_by": "room"})
        self.assertEqual(BufferPolicy.desired_detail(2, 40, 45, 7), {"desired": 0, "bound_by": "room"})

    def test_window_holds_the_peak_for_the_cooldown(self):
        policy = self.make()
        with patch.object(time, "monotonic", return_value=100.0):
            policy.note_target("general", 8, 30.0)
        with patch.object(time, "monotonic", return_value=110.0):
            policy.note_target("general", 2, 30.0)
            self.assertEqual(policy.window_max("general"), 8)
        # 창(30초)이 지나면 그 최댓값을 놓는다.
        with patch.object(time, "monotonic", return_value=131.0):
            policy.note_target("general", 2, 30.0)
            self.assertEqual(policy.window_max("general"), 2)

    def test_scale_down_waits_while_a_higher_target_is_in_the_window(self):
        policy = self.make()
        with patch.object(time, "monotonic", return_value=1000.0):
            policy.note_target("general", 8, 30.0)
        with patch.object(time, "monotonic", return_value=1040.0):
            policy.note_target("general", 2, 30.0)
            # 막 보기 시작한 것은 아니다(40초 관측). 창(30초) 안의 더 높은 목표는 없다.
            self.assertIsNone(policy.cooldown_blocks_scale_down("general", 2, 30.0))
        with patch.object(time, "monotonic", return_value=1045.0):
            policy.note_target("general", 9, 30.0)
            policy.note_target("general", 2, 30.0)
            hold = policy.cooldown_blocks_scale_down("general", 2, 30.0)
        self.assertIsNotNone(hold)
        self.assertAlmostEqual(hold, 30.0, delta=0.01)

    def test_fresh_observer_waits_one_window_before_scaling_down(self):
        policy = self.make()
        with patch.object(time, "monotonic", return_value=500.0):
            policy.note_target("general", 2, 30.0)
        with patch.object(time, "monotonic", return_value=510.0):
            self.assertAlmostEqual(policy.cooldown_blocks_scale_down("general", 2, 30.0), 20.0, delta=0.01)

    def test_dips_and_create_times_set_the_cooldown(self):
        policy = self.make()
        self.assertEqual(policy.cooldown_seconds("general"), 0.0)
        created = datetime(2026, 10, 11, tzinfo=timezone.utc)
        for index, seconds in enumerate((5.0, 6.0, 7.0, 20.0)):
            policy.note_create_time("general", f"uid-{index}", created, created + timedelta(seconds=seconds))
        # 같은 파드의 Ready 이벤트는 한 번만 센다.
        policy.note_create_time("general", "uid-0", created, created + timedelta(seconds=99))
        self.assertEqual(policy.create_time_seconds("general"), 20.0)
        self.assertEqual(policy.cooldown_seconds("general"), 20.0)


if __name__ == "__main__":
    unittest.main()
