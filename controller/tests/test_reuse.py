"""재사용 arm: 반납된 파드를 정리한 뒤 버퍼 목표 안이면 idle 로 되돌리고, 아니면 지운다.

  - 되돌릴지는 리더의 계획과 같은 식이다(이 파드가 받아 간 요청에서 빠진다). 쿨다운 창의
    목표는 리더가 적어 둔 값을 읽는다.
  - 판정과 되돌리기는 할당 락 아래에서 한다. 락을 못 잡거나, 목표 밖이거나, 옛 템플릿
    세대이거나, 정리에 실패하면 지운다. 늦은 반납(다른 티켓)은 아무것도 바꾸지 않는다.
  - 반납·재할당·리더 조정이 동시에 돌아도 N 을 넘지 않고, 한 파드를 두 요청이 동시에
    쓰지 않는다.
"""

import contextlib
import importlib.util
import random
import sys
import threading
import time
import types
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import patch

from test_direct_buffer import (
    CT,
    ROOT,
    FakeApiException,
    FakeProvider,
    FakeQueues,
    FakeTickets,
    SimAllocator,
    DirectBufferReconciler,
    World,
    no_stale,
    wait_until,
)


# ---------------------------------------------------------------------- 반납 판정


class ReuseQueues(FakeQueues):
    """리더가 적는 버퍼 목표를 담는다(실제로는 만료가 있는 Redis 키)."""

    def __init__(self, world):
        super().__init__(world)
        self.buffer_target: Optional[int] = None

    def publish_buffer_target(self, compute_type, target, ttl_seconds):
        self.buffer_target = int(target)

    def read_buffer_target(self, compute_type):
        return self.buffer_target


class ReuseProvider(FakeProvider):
    def return_pod(self, name, ticket_id, reuse_count, available_since):
        self.calls.append(("return", name, ticket_id))
        with self.world.mu:
            pod = self.world.pods.get(name)
            if (not pod or pod["terminating"] or pod["buffer_status"] != "assigned"
                    or pod["annotations"].get("ticket") != ticket_id):
                raise RuntimeError(f"{name} is no longer assigned to ticket {ticket_id}")
            pod["buffer_status"] = "idle"
            pod["annotations"] = {"reuse": str(reuse_count), "available_since": available_since}
            self.world.bump(pod)


def make_reuse(world: World, create_latency: float = 0.0):
    provider = ReuseProvider(world, create_latency=create_latency)
    queues = ReuseQueues(world)
    tickets = FakeTickets(world)
    queues.tickets = tickets
    allocator = SimAllocator(provider, queues, tickets)
    reconciler = DirectBufferReconciler(provider, queues)
    return provider, queues, tickets, allocator, reconciler


class ReturnDecisionTests(unittest.TestCase):
    def _settled(self, world, count, prefix="busy"):
        for index in range(count):
            world.add_pod(f"{prefix}-{index}", "assigned", {"ticket": f"x{index}"}, ready=True)

    def test_returns_while_buffer_is_below_reserve(self):
        world = World(capacity=10, reserve=2)
        self._settled(world, 1)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        decision = reconciler.return_decision(CT)
        self.assertTrue(decision["keep"])
        self.assertEqual(decision["target"], 2)

    def test_deletes_when_buffer_already_meets_the_target(self):
        world = World(capacity=10, reserve=2)
        self._settled(world, 1)
        world.add_pod("idle-a", "idle", {}, ready=True)
        world.add_pod("idle-b", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        self.assertFalse(reconciler.return_decision(CT)["keep"])

    def test_creating_idle_counts_as_buffer(self):
        world = World(capacity=10, reserve=2)
        self._settled(world, 1)
        world.add_pod("idle-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        queues.add_pending_create(CT, "idle-creating", "idle")
        self.assertFalse(reconciler.return_decision(CT)["keep"])

    def test_published_window_target_keeps_more_than_reserve(self):
        """몰림 뒤 리더의 쿨다운 창이 목표를 붙잡고 있으면 그만큼 되돌린다(지웠다가 다시 만들지 않게)."""
        world = World(capacity=10, reserve=1)
        self._settled(world, 1)
        world.add_pod("idle-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        self.assertFalse(reconciler.return_decision(CT)["keep"])
        queues.buffer_target = 4
        decision = reconciler.return_decision(CT)
        self.assertTrue(decision["keep"])
        self.assertEqual(decision["target"], 4)

    def test_returning_pod_leaves_settled_so_full_capacity_still_returns(self):
        """N 이 꽉 찬 채 대기가 있으면, 반납 파드는 빠지는 자리만큼 목표 안에 든다."""
        world = World(capacity=3, reserve=0)
        self._settled(world, 3)
        world.enqueue(2)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        decision = reconciler.return_decision(CT)
        self.assertTrue(decision["keep"])
        self.assertEqual(decision["queued"], 2)
        self.assertEqual(decision["target"], 1)

    def test_outdated_or_stuck_idle_is_not_counted_as_buffer(self):
        world = World(capacity=10, reserve=1)
        self._settled(world, 1)
        world.add_pod("idle-old", "idle", {}, ready=True, template_hash="gen-0")
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        self.assertTrue(reconciler.return_decision(CT)["keep"])

    def test_leader_publishes_its_target_each_pass(self):
        world = World(capacity=10, reserve=3)
        provider, queues, tickets, allocator, reconciler = make_reuse(world)
        reconciler.reconcile_type(CT)
        self.assertEqual(queues.buffer_target, 3)


# ---------------------------------------------------------------------- releaser 의 되돌리기


class FakeAgentError(Exception):
    pass


class FakeTicketMismatch(FakeAgentError):
    def __init__(self, message, current_ticket_id=""):
        super().__init__(message)
        self.current_ticket_id = current_ticket_id


AGENT = {"scrub": lambda ticket_id: {"status": "success", "scrub_ms": 3}}


class FakeAgent:
    def __init__(self, pod_ip):
        self.pod_ip = pod_ip

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def scrub(self, ticket_id=None):
        return AGENT["scrub"](ticket_id)

    def unmount(self, ticket_id=None):
        return {"status": "success"}


EVENTS: List[Dict] = []


def load_releaser_class():
    modules = {}
    for name in ("controller", "controller.services", "controller.services.compute"):
        package = types.ModuleType(name)
        package.__path__ = []
        modules[name] = package
    settings = types.ModuleType("config.settings")
    settings.RELEASE_LOCK_STALE_SECONDS = 60.0
    settings.request_label_scope = contextlib.nullcontext
    settings.set_request_label = lambda *args, **kwargs: None
    config = types.ModuleType("config")
    config.settings = settings
    ticket_format = types.ModuleType("controller.services.ticket_format")
    ticket_format.set_ticket_context = lambda ticket: None
    ticket_format.log_queue_event = lambda level, event, ticket, **fields: EVENTS.append({"event": event, **fields})
    ticket_format.datetime_to_epoch_ms = lambda value: 0
    modules["controller.services"].ticket_format = ticket_format
    queue = types.ModuleType("controller.services.queue")
    queue.QueueUnavailableError = type("QueueUnavailableError", (Exception,), {})
    queue.safe_int = lambda value, default=0: int(value) if str(value or "").strip().lstrip("-").isdigit() else default
    agent_client = types.ModuleType("controller.services.compute.agent_client")
    agent_client.AgentTicketMismatch = FakeTicketMismatch
    agent_client.ComputeAgent = FakeAgent
    agent_client.ComputeAgentError = FakeAgentError
    kubernetes = types.ModuleType("kubernetes")
    kclient = types.ModuleType("kubernetes.client")
    rest = types.ModuleType("kubernetes.client.rest")
    rest.ApiException = FakeApiException
    kubernetes.client = kclient
    kclient.rest = rest
    modules.update({
        "config": config,
        "config.settings": settings,
        ticket_format.__name__: ticket_format,
        queue.__name__: queue,
        agent_client.__name__: agent_client,
        "kubernetes": kubernetes,
        "kubernetes.client": kclient,
        "kubernetes.client.rest": rest,
    })
    name = "controller.services.compute.releaser"
    with patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location(name, ROOT / "services" / "compute" / "releaser.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module.ComputeReleaser


ComputeReleaser = load_releaser_class()


class PodApi:
    """releaser 가 직접 읽는 파드 한 개."""

    LABEL_STATUS = "compute-status"
    LABEL_COMPUTE_TYPE = "compute-type"
    STATUS_ASSIGNED = "assigned"
    STATUS_IDLE = "idle"
    ANNOTATION_ALLOCATION_TICKET = "k8s-dynamic-allocator/allocation-ticket-id"
    ANNOTATION_ALLOCATION_CLAIM = "k8s-dynamic-allocator/allocation-claim-token"
    ANNOTATION_REUSE_COUNT = "k8s-dynamic-allocator/reuse-count"
    ANNOTATION_AVAILABLE_SINCE = "k8s-dynamic-allocator/available-since"
    ANNOTATION_TEMPLATE_HASH = "k8s-dynamic-allocator/template-hash"
    namespace = "kda-test"
    api_request_timeout = 5

    def __init__(self, queues, ticket_id="t1", template_hash="gen-1", current_hash="gen-1"):
        self.queues = queues
        self.current_hash = current_hash
        self.labels = {"compute-status": "assigned", "compute-type": CT}
        self.annotations = {
            self.ANNOTATION_ALLOCATION_TICKET: ticket_id,
            self.ANNOTATION_ALLOCATION_CLAIM: "claim",
            self.ANNOTATION_TEMPLATE_HASH: template_hash,
        }
        self.deleted = False
        self.calls: List[str] = []
        self.v1 = SimpleNamespace(read_namespaced_pod=self._read)

    def _read(self, name, namespace, _request_timeout=None):
        if self.deleted:
            raise FakeApiException(404, "NotFound")
        return SimpleNamespace(
            metadata=SimpleNamespace(labels=dict(self.labels), annotations=dict(self.annotations),
                                     deletion_timestamp=None),
            status=SimpleNamespace(pod_ip="10.0.0.9"),
        )

    def template_hash(self, compute_type):
        return self.current_hash

    def return_pod(self, name, ticket_id, reuse_count, available_since):
        # 되돌리기는 할당 락 아래에서만 일어나야 한다.
        self.calls.append("return" if self.queues.lock_token else "return_without_lock")
        self.labels["compute-status"] = "idle"
        self.annotations.pop(self.ANNOTATION_ALLOCATION_TICKET, None)
        self.annotations[self.ANNOTATION_REUSE_COUNT] = str(reuse_count)
        self.annotations[self.ANNOTATION_AVAILABLE_SINCE] = available_since

    def release_pod(self, name):
        self.calls.append("delete")
        if self.deleted:
            return False
        self.deleted = True
        return True


class LockQueues:
    def __init__(self):
        self.lock_token: Optional[str] = None
        self.held_by_other = False

    def normalize_compute_type(self, value):
        return (value or CT).strip().lower() or CT

    def acquire_allocator_lock(self, compute_type):
        if self.held_by_other or self.lock_token:
            return None
        self.lock_token = "alloc"
        return self.lock_token

    def release_allocator_lock(self, compute_type, token):
        if self.lock_token == token:
            self.lock_token = None
            return True
        return False


class ReleaseTickets:
    def __init__(self, ticket_id="t1"):
        self.ticket_id = ticket_id
        self.dropped = []

    def get_assigned_request_context(self, compute_pod):
        return {}

    def get_ticket_snapshot(self, ticket_id):
        return {"ticket_id": ticket_id, "compute_pod": "pod-a", "status": "assigned", "compute_type": CT}

    def acquire_release_lock(self, ticket_id):
        return "release-token"

    def drop_release_lock(self, ticket_id, token):
        self.dropped.append(ticket_id)
        return True

    def clear_assigned_request_context_if_owner(self, compute_pod, ticket_id):
        return []


class ReleaserReturnTests(unittest.TestCase):
    def setUp(self):
        EVENTS.clear()
        AGENT["scrub"] = lambda ticket_id: {"status": "success", "scrub_ms": 3}

    def _releaser(self, keep=True, **pod_kwargs):
        queues = LockQueues()
        pod = PodApi(queues, **pod_kwargs)
        releaser = ComputeReleaser(pod, queues, ReleaseTickets())
        decisions = []

        def policy(compute_type):
            # 판정도 할당 락 아래에서만 불린다.
            decisions.append(bool(queues.lock_token))
            return {"keep": keep, "target": 2, "idle_total": 1 if keep else 2}

        releaser.set_return_policy(policy)
        return releaser, pod, queues, decisions

    def _released_outcome(self):
        released = [e for e in EVENTS if e["event"] == "Released"]
        self.assertEqual(len(released), 1, EVENTS)
        return released[0]["outcome"]

    def test_returns_to_idle_under_the_allocator_lock(self):
        releaser, pod, queues, decisions = self._releaser(keep=True)
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "returned")
        self.assertEqual(pod.calls, ["return"])
        self.assertEqual(decisions, [True])
        self.assertIsNone(queues.lock_token)  # 락을 놓았다
        self.assertEqual(pod.labels["compute-status"], "idle")
        self.assertEqual(pod.annotations[pod.ANNOTATION_REUSE_COUNT], "1")

    def test_over_target_is_deleted(self):
        releaser, pod, queues, decisions = self._releaser(keep=False)
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(self._released_outcome(), "deleted_over_target")
        self.assertIsNone(queues.lock_token)

    def test_lock_never_free_deletes_instead_of_returning(self):
        releaser, pod, queues, decisions = self._releaser(keep=True)
        queues.held_by_other = True
        # 반납 잠금이 곧 낡는다고 보게 만든다(기다리는 상한이 곧바로 지난다).
        releaser.release_lock_stale_seconds = releaser._RELABEL_MARGIN_SECONDS + 0.3
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(decisions, [])
        self.assertEqual(self._released_outcome(), "deleted_lock_busy")

    def test_outdated_template_is_deleted_without_taking_the_lock(self):
        releaser, pod, queues, decisions = self._releaser(keep=True, template_hash="gen-0")
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(decisions, [])
        self.assertEqual(self._released_outcome(), "deleted_outdated")

    def test_scrub_failure_is_deleted(self):
        releaser, pod, queues, decisions = self._releaser(keep=True)

        def failed(ticket_id):
            raise FakeAgentError("leftover processes")

        AGENT["scrub"] = failed
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(self._released_outcome(), "deleted_scrub_failed")

    def test_decision_error_deletes(self):
        releaser, pod, queues, decisions = self._releaser(keep=True)

        def broken(compute_type):
            raise OSError("apiserver timeout")

        releaser.set_return_policy(broken)
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertIsNone(queues.lock_token)
        self.assertEqual(self._released_outcome(), "deleted_return_failed")

    def test_late_release_for_another_ticket_changes_nothing(self):
        releaser, pod, queues, decisions = self._releaser(keep=True, ticket_id="t2")
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])
        self.assertEqual(decisions, [])

    def test_no_policy_means_delete(self):
        queues = LockQueues()
        pod = PodApi(queues)
        ComputeReleaser(pod, queues, ReleaseTickets()).return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])


# ---------------------------------------------------------------------- 동시 실행


class ReuseAllocator(SimAllocator):
    """세션이 끝나면 releaser 와 같은 순서로 반납한다: 할당 락 → 판정 → 되돌리기/지우기."""

    def _execute_allocated_ticket(self, ticket):
        world = self.provider.world
        ticket_id = ticket["ticket_id"]
        pod = ticket["compute_pod"]
        with world.mu:
            current = world.tickets[ticket_id]
            owns = current["status"] == "allocating" and current["claim_token"] == ticket["claim_token"]
            if owns:
                holder = world.holders.get(pod)
                if holder is not None:
                    world.double_use.append((pod, holder, ticket_id))
                world.holders[pod] = ticket_id
                current["status"] = "assigned"
                world.assignments.setdefault(pod, []).append(ticket_id)
        if not owns:
            self.provider.release_pod(pod)
            return {"status": "queued"}

        def _session_end():
            with world.mu:
                world.holders.pop(pod, None)
                world.tickets[ticket_id]["status"] = "done"
            token = None
            while token is None:
                token = self.queues.acquire_allocator_lock(CT)
            try:
                keep = world.return_policy(CT)["keep"]
                if keep:
                    try:
                        self.provider.return_pod(pod, ticket_id, 1, datetime.now(timezone.utc).isoformat())
                        with world.mu:
                            world.returned += 1
                        return
                    except RuntimeError:
                        pass
            finally:
                self.queues.release_allocator_lock(CT, token)
            self.provider.release_pod(pod)

        threading.Timer(self.hold_seconds * random.uniform(0.5, 2.0), _session_end).start()
        return {"status": "assigned"}


class ReuseConcurrencyTests(unittest.TestCase):
    def _simulate(self, capacity, reserve, tickets_total=60, seed=7):
        random.seed(seed)
        world = World(capacity=capacity, reserve=reserve, boot_seconds=0.03)
        world.holders = {}
        world.double_use = []
        world.returned = 0
        provider = ReuseProvider(world, create_latency=0.01)
        queues = ReuseQueues(world)
        tickets = FakeTickets(world)
        queues.tickets = tickets
        allocator = ReuseAllocator(provider, queues, tickets)
        allocator2 = ReuseAllocator(provider, queues, tickets)
        reconciler = DirectBufferReconciler(provider, queues)
        world.return_policy = reconciler.return_decision
        stop = threading.Event()
        errors: List[BaseException] = []

        def loop(fn, pause):
            def body():
                while not stop.is_set():
                    try:
                        fn()
                    except BaseException as exc:  # noqa: BLE001 - 테스트가 실패로 보고한다
                        errors.append(exc)
                        return
                    time.sleep(pause)
            thread = threading.Thread(target=body, daemon=True)
            thread.start()
            return thread

        threads = [
            loop(world.tick, 0.002),
            loop(lambda: allocator.drain_wait_queue_for_type(CT, no_stale), 0.003),
            loop(lambda: allocator2.drain_wait_queue_for_type(CT, no_stale), 0.003),
            loop(lambda: reconciler.reconcile_type(CT), 0.004),
        ]
        ids: List[str] = []
        try:
            initial = min(reserve, capacity or 0)
            self.assertTrue(wait_until(
                lambda: sum(1 for p in world.live() if p["buffer_status"] == "idle" and p["ready"]) >= initial,
                timeout=5.0,
            ))
            remaining = tickets_total
            while remaining > 0:
                burst = min(remaining, random.choice([1, 1, 2, 5, 8]))
                ids += world.enqueue(burst)
                remaining -= burst
                if random.random() < 0.3:
                    world.cancel(random.choice(ids))
                time.sleep(random.uniform(0.0, 0.05))

            def settled():
                with world.mu:
                    return all(world.tickets[t]["status"] in ("done", "cancelled") for t in ids)

            self.assertTrue(wait_until(settled, timeout=20.0), "티켓이 끝나지 않았다")
            converged = wait_until(
                lambda: not any(p["buffer_status"] == "assigned" for p in world.live())
                and len([p for p in world.live() if p["buffer_status"] == "idle"]) == min(reserve, capacity),
                timeout=10.0,
            )
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=2)
        self.assertEqual(errors, [])
        self.assertEqual(world.violations, [], f"N={capacity} 을 넘었다: {world.violations[:5]}")
        self.assertEqual(world.double_use, [], "한 파드를 두 요청이 동시에 썼다")
        self.assertTrue(converged, "버퍼가 R 로 돌아오지 않았다")
        return world, ids

    def test_returns_are_reused_without_exceeding_n(self):
        world, ids = self._simulate(capacity=6, reserve=2)
        self.assertGreater(world.returned, 0, "한 번도 되돌리지 않았다")
        reused = sum(1 for owners in world.assignments.values() if len(owners) > 1)
        self.assertGreater(reused, 0, "되돌린 파드를 다시 쓰지 않았다")

    def test_tight_capacity_with_returns(self):
        self._simulate(capacity=2, reserve=2, tickets_total=40, seed=11)

    def test_reserve_zero_with_returns(self):
        self._simulate(capacity=6, reserve=0, seed=3)


if __name__ == "__main__":
    unittest.main()
