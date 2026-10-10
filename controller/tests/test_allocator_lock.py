"""Allocation/replica correction invariants without a live Kubernetes cluster."""

import ast
import logging
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple


def load_allocator_class():
    # This class has no external service dependency, but its module imports the
    # Kubernetes and Django stacks. Load the production class for these isolated
    # concurrency tests so they also run in a minimal local Python environment.
    path = Path(__file__).resolve().parents[1] / "services/compute/allocator.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.ClassDef) and item.name == "ComputeAllocator"
    )
    namespace = {
        "Callable": Callable,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "Tuple": Tuple,
        "ThreadPoolExecutor": ThreadPoolExecutor,
        "as_completed": as_completed,
        "datetime": datetime,
        "time": time,
        "logger": logging.getLogger("test_allocator_lock"),
        "settings": SimpleNamespace(WAIT_QUEUE_LOCK_RENEW_SECONDS=20),
        "ticket_format": SimpleNamespace(log_queue_event=lambda *args, **kwargs: None),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["ComputeAllocator"]


ComputeAllocator = load_allocator_class()


class FakeQueues:
    worker_identity = "test-worker"
    max_retries = 1

    def __init__(self, ticket_count):
        self.tickets = [
            {"ticket_id": f"ticket-{index}", "claim_token": f"claim-{index}",
             "user_pod": f"user-{index}"}
            for index in range(ticket_count)
        ]
        self.events = []
        self.locked = False
        self.renew_count = 0
        self.lose_on_renew = set()
        self.on_renew = None
        self.release_error = False

    def normalize_compute_type(self, value):
        return value

    def is_buffer_policy_ready(self):
        return True

    def is_scale_down_gated(self, compute_type):
        return False

    def acquire_allocator_lock(self, compute_type):
        assert not self.locked
        self.locked = True
        self.events.append("lock")
        return "lock-token"

    def renew_allocator_lock(self, compute_type, token):
        self.renew_count += 1
        self.events.append("renew")
        if self.on_renew:
            self.on_renew(self.renew_count)
        return self.locked and self.renew_count not in self.lose_on_renew

    def release_allocator_lock(self, compute_type, token):
        self.events.append("unlock")
        self.locked = False
        if self.release_error:
            raise OSError("Redis lock release failed")

    def find_stale_allocating_tickets(self, compute_type):
        return []

    def has_queued_tickets(self, compute_type):
        return bool(self.tickets)

    def get_buffer_policy(self, compute_type):
        return {"N": 10, "deployment_name": "compute-general"}

    def claim_next_ticket(self, compute_type, worker_id):
        return self.tickets.pop(0) if self.tickets else None

    def pop_compute_available_at(self, pod):
        return ""


class FakeProvider:
    def __init__(self, queues, assigned, replicas, available):
        self.queues = queues
        self.assigned = assigned
        self.replicas = replicas
        self.available = [
            {"name": f"pod-{index}", "ip": f"10.0.0.{index + 1}",
             "resource_version": "1", "annotations": {}}
            for index in range(available)
        ]
        self.events = queues.events
        self.patches = []
        self.lower_calls = 0
        self.cas_conflict_release_once = False
        self.snapshot_error_after_assign = False
        self.assignment_count = 0

    def list_buffer_snapshot(self, compute_type):
        self.events.append("snapshot")
        if self.snapshot_error_after_assign and self.assignment_count:
            raise OSError("Pod list failed")
        return {
            "buffer_assigned": self.assigned,
            "available_candidates": list(self.available),
        }

    def assign_pod(self, name, user, **kwargs):
        assert self.queues.locked
        self.events.append("assign")
        self.assigned += 1
        self.assignment_count += 1
        self.available = [pod for pod in self.available if pod["name"] != name]

    def read_deployment_replicas(self, deployment_name):
        self.events.append("read_replicas")
        return self.replicas

    def lower_deployment_replicas(self, deployment_name, expected, desired):
        assert self.queues.locked
        self.events.append("lower")
        self.lower_calls += 1
        assert desired <= 10 - self.assigned
        if self.cas_conflict_release_once:
            self.cas_conflict_release_once = False
            self.assigned -= 1
            return False
        assert self.replicas == expected
        self.replicas = desired
        self.patches.append((expected, desired))
        return True


class FakeTickets:
    def mark_allocating(self, ticket_id, **kwargs):
        return {
            "ticket_id": ticket_id,
            "status": "allocating",
            "claim_token": kwargs["claim_token"],
            "compute_pod": kwargs["compute_pod"],
            "compute_pod_ip": kwargs["compute_pod_ip"],
        }


class AllocatorLockTests(unittest.TestCase):
    def run_batch(self, *, assigned=8, replicas=2, available=1, tickets=1,
                  configure=None):
        queues = FakeQueues(tickets)
        provider = FakeProvider(queues, assigned, replicas, available)
        if configure:
            configure(queues, provider)
        allocator = ComputeAllocator(provider, queues, FakeTickets())
        allocator._compute_wait_queue_batch_plan = lambda: (10, 1)

        def mount(ticket, compute_type):
            self.assertFalse(queues.locked)
            queues.events.append("mount")
            return {"status": "assigned"}

        allocator._safe_execute_allocated_ticket = mount
        result = allocator.drain_wait_queue_for_type("general", lambda ticket: {})
        return result, provider, queues

    def test_claim_correction_is_locked_and_mount_is_unlocked(self):
        result, provider, queues = self.run_batch(available=2, tickets=2)
        self.assertEqual((result["claimed"], result["assigned"]), (2, 2))
        self.assertEqual(provider.patches, [(2, 0)])
        self.assertLess(queues.events.index("lower"), queues.events.index("unlock"))
        self.assertLess(queues.events.index("unlock"), queues.events.index("mount"))

    def test_concurrent_release_expands_room_without_unneeded_trim(self):
        def configure(queues, provider):
            queues.on_renew = lambda count: setattr(
                provider, "assigned", provider.assigned - 1
            ) if count == 1 else None

        result, provider, _ = self.run_batch(configure=configure)
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(provider.assigned, 8)
        self.assertEqual(provider.replicas, 2)
        self.assertEqual(provider.patches, [])

    def test_cas_conflict_refreshes_room_before_retry(self):
        def configure(queues, provider):
            provider.cas_conflict_release_once = True

        result, provider, _ = self.run_batch(configure=configure)
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(provider.lower_calls, 1)
        self.assertEqual(provider.replicas, 2)

    def test_expired_lock_prevents_patch_but_mount_continues(self):
        def configure(queues, provider):
            queues.lose_on_renew.add(2)

        result, provider, _ = self.run_batch(configure=configure)
        self.assertTrue(result["lock_lost"])
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(provider.patches, [])

    def test_pod_list_failure_does_not_strand_reserved_ticket(self):
        def configure(queues, provider):
            provider.snapshot_error_after_assign = True

        result, provider, _ = self.run_batch(configure=configure)
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(provider.patches, [])

    def test_lock_release_failure_does_not_strand_reserved_ticket(self):
        def configure(queues, provider):
            queues.release_error = True

        result, _, queues = self.run_batch(configure=configure)
        self.assertEqual(result["assigned"], 1)
        self.assertIn("mount", queues.events)

    def test_no_trim_when_replicas_already_within_room(self):
        result, provider, _ = self.run_batch(assigned=3, replicas=2)
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(provider.patches, [])


if __name__ == "__main__":
    unittest.main()
