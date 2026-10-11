"""재사용 arm: 반납된 파드를 정리한 뒤 idle 로 되돌린다. 목표를 넘는 idle 은 리더가 줄인다.

  - 정리(scrub)에 실패하거나 옛 템플릿 세대면 되돌리지 않고 지운다. 늦은 반납(다른 티켓)과
    중복 반납은 아무것도 바꾸지 않는다. 되돌리기 PATCH 가 실패하면 지금 상태를 다시 읽어
    정한다(아직 우리 것이면 지운다).
  - 정리(cleanup)·일반 반납의 삭제는 지우기 직전에 다시 읽어, 처음 본 상태·티켓 그대로일
    때만 그 버전을 전제로 지운다. 그사이 되돌려져 다음 요청에 할당된 파드는 지우지 않는다.
  - 반납·재할당·리더 조정이 동시에 돌아도 N 을 넘지 않고, 한 파드를 두 요청이 동시에
    쓰지 않으며, 버퍼는 R 로 돌아온다.
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


# ---------------------------------------------------------------------- 가짜 모듈


class FakeAgentError(Exception):
    pass


class FakeTicketMismatch(FakeAgentError):
    def __init__(self, message, current_ticket_id=""):
        super().__init__(message)
        self.current_ticket_id = current_ticket_id


AGENT: Dict = {}


def reset_agent():
    AGENT.clear()
    AGENT["scrub"] = lambda ticket_id: {"status": "success", "scrub_ms": 3}
    AGENT["unmount"] = lambda ticket_id: {"status": "success"}


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
        return AGENT["unmount"](ticket_id)


EVENTS: List[Dict] = []


def _stub_modules():
    modules = {}
    for name in ("controller", "controller.services", "controller.services.compute"):
        package = types.ModuleType(name)
        package.__path__ = []
        modules[name] = package
    settings = types.ModuleType("config.settings")
    settings.RELEASE_LOCK_STALE_SECONDS = 60.0
    settings.COMPUTE_NOT_READY_GRACE_SECONDS = 30.0
    settings.request_label_scope = lambda *args, **kwargs: contextlib.nullcontext()
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
    queue.safe_int = lambda value, default=0: int(value) if str(value or "").strip().isdigit() else default
    queue.release_lock_age_seconds = lambda token, now=None: None
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
    return modules


def load_module(short):
    name = f"controller.services.compute.{short}"
    with patch.dict(sys.modules, _stub_modules()):
        spec = importlib.util.spec_from_file_location(name, ROOT / "services" / "compute" / f"{short}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


ComputeReleaser = load_module("releaser").ComputeReleaser
ComputeCleanup = load_module("cleanup").ComputeCleanup


# ---------------------------------------------------------------------- releaser


class PodApi:
    """releaser 가 직접 읽고 쓰는 파드 한 개(apiserver 대역)."""

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

    def __init__(self, ticket_id="t1", template_hash="gen-1", current_hash="gen-1", status="assigned"):
        self.current_hash = current_hash
        self.labels = {"compute-status": status, "compute-type": CT}
        self.annotations = {self.ANNOTATION_TEMPLATE_HASH: template_hash}
        if ticket_id:
            self.annotations[self.ANNOTATION_ALLOCATION_TICKET] = ticket_id
            self.annotations[self.ANNOTATION_ALLOCATION_CLAIM] = "claim"
        self.uid = "uid-1"
        self.rv = 1
        self.deleted = False
        self.calls: List[str] = []
        self.return_error: Optional[Exception] = None
        self.after_failed_return = None  # 실패한 PATCH 뒤의 실제 상태를 꾸민다
        self.v1 = SimpleNamespace(read_namespaced_pod=self._read)

    def bump(self):
        self.rv += 1

    def _read(self, name, namespace, _request_timeout=None):
        if self.deleted:
            raise FakeApiException(404, "NotFound")
        return SimpleNamespace(
            metadata=SimpleNamespace(labels=dict(self.labels), annotations=dict(self.annotations),
                                     deletion_timestamp=None, uid=self.uid, resource_version=str(self.rv)),
            status=SimpleNamespace(pod_ip="10.0.0.9"),
        )

    def template_hash(self, compute_type):
        return self.current_hash

    def return_pod(self, name, ticket_id, reuse_count, available_since):
        self.calls.append("return")
        if self.return_error is not None:
            if self.after_failed_return:
                self.after_failed_return(self, available_since)
            raise self.return_error
        self.labels["compute-status"] = "idle"
        self.annotations.pop(self.ANNOTATION_ALLOCATION_TICKET, None)
        self.annotations.pop(self.ANNOTATION_ALLOCATION_CLAIM, None)
        self.annotations[self.ANNOTATION_REUSE_COUNT] = str(reuse_count)
        self.annotations[self.ANNOTATION_AVAILABLE_SINCE] = available_since
        self.bump()

    def release_pod(self, name):
        self.calls.append("delete")
        if self.deleted:
            return False
        self.deleted = True
        return True

    def delete_pod_if_unchanged(self, name, uid, rv):
        self.calls.append("delete_if_unchanged")
        if self.deleted or uid != self.uid or rv != str(self.rv):
            return False
        self.deleted = True
        return True


class Queues:
    def normalize_compute_type(self, value):
        return (value or CT).strip().lower() or CT


class ReleaseTickets:
    def __init__(self):
        self.lock_free = True

    def get_assigned_request_context(self, compute_pod):
        return {}

    def get_ticket_snapshot(self, ticket_id):
        return {"ticket_id": ticket_id, "compute_pod": "pod-a", "status": "assigned", "compute_type": CT}

    def acquire_release_lock(self, ticket_id):
        return "release-token" if self.lock_free else None

    def take_over_release_lock(self, ticket_id, stale_token):
        return "taken-over"

    def drop_release_lock(self, ticket_id, token):
        return True

    def clear_assigned_request_context_if_owner(self, compute_pod, ticket_id):
        return []

    def clear_assigned_request_context(self, compute_pod):
        return []


def released_outcomes():
    return [e["outcome"] for e in EVENTS if e["event"] == "Released"]


class ReleaserReturnTests(unittest.TestCase):
    def setUp(self):
        EVENTS.clear()
        reset_agent()

    def _releaser(self, **pod_kwargs):
        pod = PodApi(**pod_kwargs)
        tickets = ReleaseTickets()
        return ComputeReleaser(pod, Queues(), tickets), pod, tickets

    def test_returns_to_idle_after_scrub(self):
        releaser, pod, _ = self._releaser()
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "returned")
        self.assertEqual(pod.calls, ["return"])
        self.assertEqual(pod.labels["compute-status"], "idle")
        self.assertNotIn(pod.ANNOTATION_ALLOCATION_TICKET, pod.annotations)
        self.assertEqual(pod.annotations[pod.ANNOTATION_REUSE_COUNT], "1")

    def test_outdated_template_is_deleted(self):
        releaser, pod, _ = self._releaser(template_hash="gen-0")
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(released_outcomes(), ["deleted_outdated"])

    def test_scrub_failure_is_deleted(self):
        releaser, pod, _ = self._releaser()

        def failed(ticket_id):
            raise FakeAgentError("leftover processes")

        AGENT["scrub"] = failed
        releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(pod.calls, ["delete"])
        self.assertEqual(released_outcomes(), ["deleted_scrub_failed"])

    def test_agent_serving_another_ticket_changes_nothing(self):
        releaser, pod, _ = self._releaser()

        def mismatch(ticket_id):
            raise FakeTicketMismatch("scrub refused", "t2")

        AGENT["scrub"] = mismatch
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])

    def test_late_release_for_another_ticket_changes_nothing(self):
        releaser, pod, _ = self._releaser(ticket_id="t2")
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])

    def test_duplicate_release_changes_nothing(self):
        releaser, pod, tickets = self._releaser()
        tickets.lock_free = False  # 같은 티켓의 다른 반납이 잠금을 쥐고 있다
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])

    def _failed_return(self, after):
        releaser, pod, _ = self._releaser()
        pod.return_error = RuntimeError("patch timed out")
        pod.after_failed_return = after
        result = releaser.return_compute_pod("pod-a", ticket_id="t1")
        return result, pod

    def test_failed_return_still_ours_is_deleted(self):
        result, pod = self._failed_return(None)
        self.assertEqual(pod.calls, ["return", "delete"])
        self.assertEqual(released_outcomes(), ["deleted_return_failed"])

    def test_failed_return_that_landed_counts_as_returned(self):
        def landed(api, available_since):
            api.labels["compute-status"] = "idle"
            api.annotations.pop(api.ANNOTATION_ALLOCATION_TICKET, None)
            api.annotations[api.ANNOTATION_AVAILABLE_SINCE] = available_since

        result, pod = self._failed_return(landed)
        self.assertEqual(pod.calls, ["return"])
        self.assertEqual(released_outcomes(), ["returned"])

    def test_failed_return_after_pod_moved_on_leaves_it(self):
        def moved(api, available_since):
            api.annotations[api.ANNOTATION_ALLOCATION_TICKET] = "t9"

        result, pod = self._failed_return(moved)
        self.assertEqual(pod.calls, ["return"])
        self.assertEqual(released_outcomes(), ["lost_external"])

    def test_failed_return_of_deleted_pod_is_ignored(self):
        def gone(api, available_since):
            api.deleted = True

        result, pod = self._failed_return(gone)
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, ["return"])


class ReleaserDeleteTests(unittest.TestCase):
    """정리(cleanup)·일반 반납의 삭제: 지우기 직전에 다시 읽고 그 버전을 전제로 지운다."""

    def setUp(self):
        EVENTS.clear()
        reset_agent()

    def test_pod_returned_and_reassigned_during_unmount_is_not_deleted(self):
        """검토 D: 늦게 반영된 되돌리기로 idle 이 된 뒤 다음 요청이 가져간 파드."""
        pod = PodApi(ticket_id="t1")

        def reassigned(ticket_id):
            pod.labels["compute-status"] = "idle"
            pod.annotations.pop(pod.ANNOTATION_ALLOCATION_TICKET, None)
            pod.bump()
            pod.labels["compute-status"] = "assigned"
            pod.annotations[pod.ANNOTATION_ALLOCATION_TICKET] = "t2"
            pod.bump()
            return {"status": "success"}

        AGENT["unmount"] = reassigned
        releaser = ComputeReleaser(pod, Queues(), ReleaseTickets())
        result = releaser.release_stale_locked_compute_pod("pod-a", "t1", "stale-token")
        self.assertEqual(result["outcome"], "ignored")
        self.assertFalse(pod.deleted)

    def test_status_bump_alone_is_retried_and_deleted(self):
        pod = PodApi(ticket_id="t1")
        original = pod.delete_pod_if_unchanged
        bumped = {"done": False}

        def kubelet_bump_once(name, uid, rv):
            if not bumped["done"]:
                bumped["done"] = True
                pod.bump()  # 상태 갱신으로 버전만 바뀌었다
            return original(name, uid, rv)

        pod.delete_pod_if_unchanged = kubelet_bump_once
        releaser = ComputeReleaser(pod, Queues(), ReleaseTickets())
        result = releaser.release_compute_pod("pod-a", expected_status="assigned", expected_ticket_id="t1")
        self.assertEqual(result["status"], "success")
        self.assertNotEqual(result.get("outcome"), "ignored")
        self.assertTrue(pod.deleted)

    def test_idle_seen_by_cleanup_but_assigned_now_is_left_alone(self):
        """검토 A: 정리는 NotReady idle 을 봤지만, 그사이 Ready 로 돌아와 할당됐다."""
        pod = PodApi(ticket_id="t2", status="assigned")
        releaser = ComputeReleaser(pod, Queues(), ReleaseTickets())
        expected = ComputeCleanup._snapshot_expectation({"buffer_status": "idle"})
        self.assertEqual(expected, ("idle", ""))
        result = releaser.release_unreachable_compute_pod(
            "pod-a", expected_status=expected[0], expected_ticket_id=expected[1]
        )
        self.assertEqual(result["outcome"], "ignored")
        self.assertFalse(pod.deleted)


# ---------------------------------------------------------------------- 리더가 넘는 idle 을 줄인다


class LeaderTrimTests(unittest.TestCase):
    def test_excess_returned_idle_is_trimmed_newest_first(self):
        world = World(capacity=10, reserve=1)
        world.add_pod("returned-old", "idle", {"reuse": "3"}, ready=True, age_seconds=300)
        world.add_pod("fresh-new", "idle", {}, ready=True, age_seconds=5)
        provider = FakeProvider(world)
        queues = FakeQueues(world)
        queues.tickets = FakeTickets(world)
        reconciler = DirectBufferReconciler(provider, queues)
        reconciler.reconcile_type(CT)
        self.assertFalse(world.pods["returned-old"]["terminating"])
        self.assertTrue(world.pods["fresh-new"]["terminating"])


# ---------------------------------------------------------------------- 동시 실행


class ReuseProvider(FakeProvider):
    def return_pod(self, name, ticket_id, reuse_count, available_since):
        with self.world.mu:
            pod = self.world.pods.get(name)
            if (not pod or pod["terminating"] or pod["buffer_status"] != "assigned"
                    or pod["annotations"].get("ticket") != ticket_id):
                raise RuntimeError(f"{name} is no longer assigned to ticket {ticket_id}")
            pod["buffer_status"] = "idle"
            pod["annotations"] = {"reuse": str(reuse_count), "available_since": available_since}
            self.world.bump(pod)


class ReuseAllocator(SimAllocator):
    """세션이 끝나면 정리 후 되돌린다(락·판정 없음). 목표를 넘으면 리더가 줄인다."""

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
            try:
                self.provider.return_pod(pod, ticket_id, 1, datetime.now(timezone.utc).isoformat())
                with world.mu:
                    world.returned += 1
            except RuntimeError:
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
        queues = FakeQueues(world)
        tickets = FakeTickets(world)
        queues.tickets = tickets
        allocator = ReuseAllocator(provider, queues, tickets)
        allocator2 = ReuseAllocator(provider, queues, tickets)
        reconciler = DirectBufferReconciler(provider, queues)
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
