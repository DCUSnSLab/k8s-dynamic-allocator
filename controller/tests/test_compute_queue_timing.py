"""Focused checks for queue timing telemetry without a Redis server."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from time import time
from unittest.mock import patch


CONTROLLER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CONTROLLER / "rest_api"))
QUEUE_DIR = CONTROLLER / "services" / "queue"
# services/__init__.py imports the full runtime. Load this queue package alone
# so the timing tests work with the standard library and no cluster dependencies.
PACKAGE_NAME = "queue_timing_test_package"
queue_package = types.ModuleType(PACKAGE_NAME)
queue_package.__path__ = [str(QUEUE_DIR)]
sys.modules[PACKAGE_NAME] = queue_package
spec = importlib.util.spec_from_file_location(
    f"{PACKAGE_NAME}.compute_queues", QUEUE_DIR / "compute_queues.py"
)
queue_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = queue_module
spec.loader.exec_module(queue_module)


class FakeRedis:
    def __init__(self):
        self.active = set()
        self.hashes = {}
        self.queue = {}

    def smembers(self, _key):
        return set(self.active)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def zscore(self, _key, ticket_id):
        return self.queue.get(ticket_id)

    def zadd(self, _key, scores):
        self.queue.update(scores)

    def zrem(self, _key, ticket_id):
        self.queue.pop(ticket_id, None)

    def zrange(self, _key, _start, _end):
        return sorted(self.queue, key=lambda ticket_id: self.queue[ticket_id])

    def sadd(self, _key, ticket_id):
        self.active.add(ticket_id)

    def srem(self, _key, ticket_id):
        self.active.discard(ticket_id)


class FakeTickets:
    def __init__(self, queue, client):
        self.queue = queue
        self.client = client

    def get_ticket(self, ticket_id):
        return self.client.hgetall(self.queue._ticket_key(ticket_id)) or None

    def _ticket_transition(self, ticket_id, *, expected_statuses, updates, **_kwargs):
        raw = self.client.hashes.get(self.queue._ticket_key(ticket_id))
        if not raw or raw["status"] not in expected_statuses:
            return None
        raw.update(updates)
        return dict(raw)


class CountingPipeline:
    def __init__(self, client):
        self.client = client
        self.reads = []

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def hget(self, key, field):
        self.reads.append((key, field))

    def execute(self):
        self.client.pipeline_reads.append(list(self.reads))
        if self.client.fail_pipeline:
            raise queue_module.RedisError("pipeline unavailable")
        return [self.client.hashes.get(key, {}).get(field) for key, field in self.reads]


class CountingRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.zrange_calls = 0
        self.pipeline_transactions = []
        self.pipeline_reads = []
        self.fail_pipeline = False

    def zrange(self, key, start, end):
        self.zrange_calls += 1
        return super().zrange(key, start, end)

    def pipeline(self, transaction):
        self.pipeline_transactions.append(transaction)
        return CountingPipeline(self)


class QueueTimingTests(unittest.TestCase):
    def make_queue(self):
        queue = queue_module.ComputeQueues(prefix="timing-test")
        client = FakeRedis()
        queue._redis_client = lambda: client
        queue.tickets = FakeTickets(queue, client)
        return queue, client

    def test_claim_keeps_transition_and_logs_only_aggregate_fields(self):
        queue, client = self.make_queue()
        ticket_id = "private-ticket-id"
        client.active.add(ticket_id)
        client.queue[ticket_id] = 1.0
        client.hashes[queue._ticket_key(ticket_id)] = {
            "status": "queued",
            "request_at_ms": "1",
            "last_poll_ms": str(int(time() * 1000)),
        }

        with patch.object(queue_module, "QUEUE_TIMING_WINDOW_SECONDS", 0), patch.object(
            queue_module, "QUEUE_TIMING_SLOW_MS", 0
        ), self.assertLogs(queue_module.logger, level="WARNING") as captured:
            ticket = queue.claim_next_ticket("general", worker_id="worker")

        self.assertEqual(ticket["status"], "allocating")
        self.assertEqual(ticket["claimed_by"], "worker")
        self.assertIn(ticket_id, client.active)
        self.assertIn(ticket_id, client.queue)
        messages = "\n".join(captured.output)
        self.assertIn("operation=repair", messages)
        self.assertIn("operation=claim", messages)
        self.assertIn("repair_ms=", messages)
        self.assertIn("scan_ms=", messages)
        self.assertIn("claimed=1", messages)
        self.assertNotIn(ticket_id, messages)

    def test_missing_ticket_is_still_removed_before_empty_result(self):
        queue, client = self.make_queue()
        client.active.add("missing-ticket")
        client.queue["missing-ticket"] = 1.0

        with patch.object(queue_module, "QUEUE_TIMING_WINDOW_SECONDS", 0), patch.object(
            queue_module, "QUEUE_TIMING_SLOW_MS", 0
        ), self.assertLogs(queue_module.logger, level="WARNING") as captured:
            result = queue.claim_next_ticket("general")

        self.assertIsNone(result)
        self.assertFalse(client.active)
        self.assertFalse(client.queue)
        self.assertIn("empty=1", "\n".join(captured.output))

    def test_window_aggregates_multiple_calls(self):
        queue, _client = self.make_queue()
        with patch.object(queue_module.time, "monotonic", side_effect=[100.0, 131.0]), patch.object(
            queue_module, "QUEUE_TIMING_SLOW_MS", 0
        ), self.assertLogs(queue_module.logger, level="WARNING") as captured:
            queue._record_queue_timing("general", "repair", 1.0, examined=3)
            queue._record_queue_timing("general", "repair", 2.0, examined=4)

        self.assertEqual(len(captured.output), 1)
        self.assertIn("calls=2", captured.output[0])
        self.assertIn("total_ms=3.0", captured.output[0])
        self.assertIn("examined=7", captured.output[0])

    def test_queued_count_pipelines_only_status_in_queue_order(self):
        queue = queue_module.ComputeQueues(prefix="timing-test")
        client = CountingRedis()
        queue._redis_client = lambda: client
        client.queue.update({"queued-late": 3.0, "assigned": 2.0, "queued-early": 1.0, "missing": 4.0})
        client.hashes[queue._ticket_key("queued-late")] = {"status": "QUEUED"}
        client.hashes[queue._ticket_key("assigned")] = {"status": "allocating"}
        client.hashes[queue._ticket_key("queued-early")] = {"status": "queued"}

        self.assertEqual(queue.queued_count("general"), 2)
        self.assertEqual(client.zrange_calls, 1)
        self.assertEqual(client.pipeline_transactions, [False])
        self.assertEqual(
            client.pipeline_reads,
            [[(queue._ticket_key(ticket_id), "status") for ticket_id in
              ("queued-early", "assigned", "queued-late", "missing")]],
        )

    def test_queued_count_empty_queue_skips_pipeline(self):
        queue = queue_module.ComputeQueues(prefix="timing-test")
        client = CountingRedis()
        queue._redis_client = lambda: client

        self.assertEqual(queue.queued_count("general"), 0)
        self.assertEqual(client.zrange_calls, 1)
        self.assertEqual(client.pipeline_transactions, [])

    def test_queued_count_converts_pipeline_redis_failure(self):
        queue = queue_module.ComputeQueues(prefix="timing-test")
        client = CountingRedis()
        client.queue["ticket"] = 1.0
        client.fail_pipeline = True
        queue._redis_client = lambda: client

        with self.assertRaises(queue_module.QueueUnavailableError):
            queue.queued_count("general")


if __name__ == "__main__":
    unittest.main()
