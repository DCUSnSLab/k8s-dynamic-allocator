"""cold start(요청마다 파드를 만든다)의 불변식.

실제 클러스터 없이 가짜 클러스터·큐 위에서 확인한다.
  - 두 복제본의 할당(drain)과 리더가 동시에 돌아도 살아 있는 파드가 N 을 넘지 않는다.
  - 미리 만든 파드를 쓰지 않는다. 다른 arm 이 남긴 idle 은 내주지 않고 리더가 지운다.
  - 묶는 사이 취소된 티켓 몫으로 파드를 만들지 않는다.
  - 생성 중 목록이 N 계산에 들어가고, 거절(4xx)만 목록에서 뺀다.
"""

import ast
import importlib.util
import itertools
import logging
import random
import sys
import threading
import time
import types
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class FakeApiException(Exception):
    def __init__(self, status=None, reason=""):
        super().__init__(reason)
        self.status = status
        self.reason = reason


class FakeQueueUnavailable(Exception):
    pass


class AllocationClaimLost(RuntimeError):
    pass


SETTINGS = SimpleNamespace(
    # 할당
    WAIT_QUEUE_BATCH_LIMIT=10,
    WAIT_QUEUE_MOUNT_CONCURRENCY=10,
    COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS=10,
    WAIT_QUEUE_ALLOCATING_TTL_SECONDS=60,
    WAIT_QUEUE_WORKER_INTERVAL_SECONDS=1,
    WAIT_QUEUE_LOCK_RENEW_SECONDS=20,
    WARM_BUFFER_PENDING_CREATE_SECONDS=60.0,
    # 리더
    BUFFER_RECONCILE_RESYNC_SECONDS=5,
    WARM_BUFFER_RESYNC_SECONDS=0.2,
    COLD_START_POD_READY_TIMEOUT_SECONDS=300.0,
    COLD_START_POD_READY_POLL_SECONDS=0.01,
)


def _ticket_response(ticket, message=None):
    if not ticket:
        return {"status": "error", "message": message or "Ticket not found"}
    return {"status": str(ticket.get("status") or "queued"), "message": message or "",
            "ticket_id": ticket.get("ticket_id")}


TICKET_FORMAT = SimpleNamespace(
    ticket_response=_ticket_response,
    log_queue_event=lambda *args, **kwargs: None,
    set_ticket_context=lambda *args, **kwargs: None,
    assigned_request_context=lambda ticket: {},
    assignment_timing_fields=lambda ticket: {},
)


def load_allocator_class():
    """allocator 모듈은 Kubernetes·Django 를 읽으므로 클래스만 떼어 실행한다."""
    path = ROOT / "services/compute/allocator.py"
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
        "threading": threading,
        "logger": logging.getLogger("test_direct_buffer"),
        "settings": SETTINGS,
        "ticket_format": TICKET_FORMAT,
        "timing": SimpleNamespace(ENABLED=False),
        "AllocationClaimLost": AllocationClaimLost,
        "QueueUnavailableError": FakeQueueUnavailable,
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["ComputeAllocator"]


def load_direct_reconciler_class():
    package_names = ("controller", "controller.services", "controller.services.compute")
    modules = {}
    for name in package_names:
        package = types.ModuleType(name)
        package.__path__ = []
        modules[name] = package
    config = types.ModuleType("config")
    config.settings = SETTINGS
    queue = types.ModuleType("controller.services.queue")
    queue.QueueUnavailableError = FakeQueueUnavailable
    modules.update({"config": config, queue.__name__: queue})
    name = "controller.services.compute.direct_buffer_reconciler"
    with patch.dict(sys.modules, modules):
        spec = importlib.util.spec_from_file_location(
            name, ROOT / "services" / "compute" / "direct_buffer_reconciler.py"
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module.DirectBufferReconciler


ComputeAllocator = load_allocator_class()
DirectBufferReconciler = load_direct_reconciler_class()

CT = "general"


# ---------------------------------------------------------------------- 가짜 세계


class World:
    """apiserver(파드)와 Redis(티켓·락·생성 중 목록)를 한곳에 둔다.

    파드가 하나 생길 때마다 살아 있는(지우는 중이 아닌) 파드 수가 N 이하인지 본다.
    """

    def __init__(self, capacity: Optional[int], boot_seconds: float = 0.03):
        self.mu = threading.RLock()
        self.capacity = capacity
        self.boot_seconds = boot_seconds
        self.pods: Dict[str, Dict] = {}
        self.rv = itertools.count(1)
        self.uid = itertools.count(1)
        self.tickets: Dict[str, Dict] = {}
        self.queue: List[Tuple[int, str]] = []
        self.seq = itertools.count(1)
        self.pending: Dict[str, float] = {}
        self.pending_kind: Dict[str, str] = {}
        self.lock_token: Optional[str] = None
        self.max_live = 0
        self.violations: List[Tuple[str, int]] = []
        self.created = {"request": 0}
        self.assignments: Dict[str, List[str]] = {}

    # 파드
    def live(self) -> List[Dict]:
        # 다른 스레드가 파드를 넣고 빼므로 잠금 안에서 복사한다.
        with self.mu:
            return [dict(p) for p in self.pods.values() if not p["terminating"]]

    def add_pod(self, name: str, status: str, annotations: Dict, *, ready: bool = False,
                age_seconds: float = 0.0) -> Dict:
        with self.mu:
            if name in self.pods:
                raise FakeApiException(409, "AlreadyExists")
            pod = {
                "name": name,
                "uid": f"uid-{next(self.uid)}",
                "resource_version": str(next(self.rv)),
                "created": datetime.now(timezone.utc) - timedelta(seconds=age_seconds),
                "born": time.monotonic() - age_seconds,
                "annotations": dict(annotations),
                "buffer_status": status,
                "ready": ready,
                "terminating": False,
            }
            self.pods[name] = pod
            live = len(self.live())
            self.max_live = max(self.max_live, live)
            if self.capacity is not None and live > self.capacity:
                self.violations.append((name, live))
            return pod

    def bump(self, pod: Dict) -> None:
        pod["resource_version"] = str(next(self.rv))

    def terminate(self, name: str) -> bool:
        with self.mu:
            pod = self.pods.get(name)
            if not pod or pod["terminating"]:
                return False
            pod["terminating"] = True
            pod["gone_at"] = time.monotonic() + 0.02
            self.bump(pod)
            return True

    def tick(self) -> None:
        """kubelet 대역: 부팅이 끝난 파드를 Ready 로, 지우는 중인 파드를 치운다."""
        now = time.monotonic()
        with self.mu:
            for pod in list(self.pods.values()):
                if pod["terminating"]:
                    if now >= pod["gone_at"]:
                        del self.pods[pod["name"]]
                elif not pod["ready"] and now - pod["born"] >= self.boot_seconds:
                    pod["ready"] = True
                    self.bump(pod)

    # 티켓
    def enqueue(self, count: int) -> List[str]:
        ids = []
        with self.mu:
            for _ in range(count):
                seq = next(self.seq)
                ticket_id = f"t{seq:04d}"
                self.tickets[ticket_id] = {
                    "ticket_id": ticket_id,
                    "ticket_short": ticket_id,
                    "compute_type": CT,
                    "status": "queued",
                    "claim_token": "",
                    "compute_pod": "",
                    "user_pod": f"user-{seq}",
                    "user_pod_ip": "10.1.0.1",
                    "retry_count": 0,
                    "max_retries": 3,
                    "seq": seq,
                }
                self.queue.append((seq, ticket_id))
                ids.append(ticket_id)
            self.queue.sort()
        return ids

    def cancel(self, ticket_id: str) -> None:
        with self.mu:
            ticket = self.tickets[ticket_id]
            if ticket["status"] in ("queued", "allocating"):
                ticket["status"] = "cancelled"
                self.queue = [item for item in self.queue if item[1] != ticket_id]


class FakeProvider:
    allocation_mode = "cold_start"
    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "compute-status"
    STATUS_IDLE = "idle"
    STATUS_ASSIGNED = "assigned"

    def __init__(self, world: World, create_latency: float = 0.0):
        self.world = world
        self.create_latency = create_latency
        self.fail_create_with: Optional[Exception] = None
        self.calls: List[Tuple] = []

    def snapshot(self, compute_type=None) -> Dict:
        with self.world.mu:
            items = [
                {
                    "name": p["name"],
                    "uid": p["uid"],
                    "resource_version": p["resource_version"],
                    "created": p["created"],
                    "annotations": dict(p["annotations"]),
                    "buffer_status": p["buffer_status"],
                    "ready": p["ready"],
                    "ip": "10.0.0.9" if p["ready"] else None,
                    "terminating": p["terminating"],
                }
                for p in self.world.pods.values()
            ]
        live = [p for p in items if not p["terminating"]]
        idle = [p for p in live if p["buffer_status"] == "idle"]
        return {
            "pods": items,
            "active_names": {p["name"] for p in live},
            "idle": idle,
            "idle_ready": [p for p in idle if p["ready"]],
            "assigned": sum(1 for p in live if p["buffer_status"] == "assigned"),
        }

    def read_capacity(self, compute_type=None):
        return self.world.capacity

    def request_pod_name(self, ticket):
        return f"compute-general-{ticket['ticket_id']}-{ticket.get('retry_count') or 0}"

    def _latency(self):
        if self.create_latency:
            time.sleep(random.uniform(0, self.create_latency))

    def create_pod_for_ticket(self, ticket, pod_name=None):
        name = pod_name or self.request_pod_name(ticket)
        self.calls.append(("create_request", name))
        if self.fail_create_with is not None:
            raise self.fail_create_with
        self._latency()
        self.world.add_pod(name, "assigned", {
            "ticket": ticket["ticket_id"], "claim": ticket.get("claim_token") or "",
        })
        with self.world.mu:
            self.world.created["request"] += 1
        return name

    def delete_idle_pod(self, name, uid, rv):
        self.calls.append(("delete_idle", name))
        with self.world.mu:
            pod = self.world.pods.get(name)
            if not pod or pod["uid"] != uid or pod["resource_version"] != rv:
                return False
            return self.world.terminate(name)

    def release_pod(self, name):
        self.calls.append(("release", name))
        return self.world.terminate(name)

    def wait_pod_ready(self, name, keep_claim=None):
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            self.world.tick()
            with self.world.mu:
                pod = self.world.pods.get(name)
                if pod is not None:
                    if pod["terminating"]:
                        raise RuntimeError(f"deleted before Ready: {name}")
                    if pod["ready"]:
                        return SimpleNamespace(status=SimpleNamespace(pod_ip="10.0.0.9"))
            time.sleep(0.002)
        raise TimeoutError(name)

    def get_pod_ip(self, name):
        return "10.0.0.9"

    @staticmethod
    def _pod_ready_at(pod):
        return datetime.now(timezone.utc)

    def drain_buffer_deployments(self):
        return 0


class FakeQueues:
    worker_identity = "worker"
    max_retries = 3
    lock_ttl_seconds = 60

    def __init__(self, world: World):
        self.world = world
        self.tickets = None

    def normalize_compute_type(self, value):
        return (value or CT).strip().lower() or CT

    def known_compute_types(self):
        return [CT]

    # 할당 락: Redis 토큰 락처럼 잡지 못하면 None 이다(잠깐은 기다린다).
    def acquire_allocator_lock(self, compute_type):
        deadline = time.monotonic() + 0.2
        while time.monotonic() < deadline:
            with self.world.mu:
                if self.world.lock_token is None:
                    self.world.lock_token = uuid.uuid4().hex
                    return self.world.lock_token
            time.sleep(0.001)
        return None

    def renew_allocator_lock(self, compute_type, token):
        with self.world.mu:
            return self.world.lock_token == token

    def release_allocator_lock(self, compute_type, token):
        with self.world.mu:
            if self.world.lock_token == token:
                self.world.lock_token = None
                return True
            return False

    def find_stale_allocating_tickets(self, compute_type=None):
        return []

    def has_queued_tickets(self, compute_type):
        with self.world.mu:
            return any(self.world.tickets[tid]["status"] == "queued" for _, tid in self.world.queue)

    def allocating_missing_pods(self, active, compute_type=None):
        known = set(active or ())
        with self.world.mu:
            return [
                t["compute_pod"] or "" for t in self.world.tickets.values()
                if t["status"] == "allocating" and not (t["compute_pod"] and t["compute_pod"] in known)
            ]

    def pending_creates(self, compute_type, max_age_seconds):
        cutoff = time.time() - max_age_seconds
        with self.world.mu:
            for name in [n for n, at in self.world.pending.items() if at < cutoff]:
                del self.world.pending[name]
            return {name: self.world.pending_kind.get(name, "request") for name in self.world.pending}

    def add_pending_create(self, compute_type, name, kind):
        with self.world.mu:
            self.world.pending[name] = time.time()
            self.world.pending_kind[name] = kind

    def refresh_pending_creates(self, compute_type, names, kind):
        # 실제와 같은 계약: 남아 있는 것만 갱신하고, 남아 있던 이름을 돌려준다.
        with self.world.mu:
            alive = {name for name in names if name in self.world.pending}
            for name in alive:
                self.world.pending[name] = time.time()
            return alive

    def is_allocation_stale(self, ticket):
        return bool(ticket.get("stale"))

    def remove_pending_creates(self, compute_type, names):
        with self.world.mu:
            for name in names:
                self.world.pending.pop(name, None)

    def claim_next_ticket(self, compute_type, worker_id=None):
        with self.world.mu:
            while self.world.queue:
                _, ticket_id = self.world.queue.pop(0)
                ticket = self.world.tickets[ticket_id]
                if ticket["status"] != "queued":
                    continue
                ticket["status"] = "allocating"
                ticket["claim_token"] = uuid.uuid4().hex
                ticket["claimed_by"] = worker_id
                return dict(ticket)
            return None


class FakeTickets:
    def __init__(self, world: World):
        self.world = world
        self.requeued: List[str] = []
        self.before_record: Optional[Callable[[str], None]] = None

    def get_ticket(self, ticket_id):
        with self.world.mu:
            ticket = self.world.tickets.get(ticket_id)
            return dict(ticket) if ticket else None

    def _owns(self, ticket, claim_token):
        return ticket["status"] == "allocating" and (not claim_token or ticket["claim_token"] == claim_token)

    def record_allocating_pod(self, ticket_id, compute_pod, claim_token=None):
        if self.before_record is not None:
            self.before_record(ticket_id)
        with self.world.mu:
            ticket = self.world.tickets[ticket_id]
            if self._owns(ticket, claim_token):
                ticket["compute_pod"] = compute_pod
            return dict(ticket)

    def requeue_ticket(self, ticket_id, reason="", increment_retry=True, claim_token=None):
        with self.world.mu:
            ticket = self.world.tickets[ticket_id]
            if ticket["status"] in ("assigned", "failed", "cancelled", "done"):
                return dict(ticket)
            if claim_token is not None and ticket["claim_token"] != claim_token:
                return dict(ticket)
            ticket.update(status="queued", claim_token="", compute_pod="")
            if increment_retry:
                ticket["retry_count"] = int(ticket["retry_count"]) + 1
            if all(tid != ticket_id for _, tid in self.world.queue):
                self.world.queue.append((ticket["seq"], ticket_id))
                self.world.queue.sort()
            self.requeued.append(ticket_id)
            return dict(ticket)

    def mark_allocating(self, ticket_id, compute_pod, compute_pod_ip, claimed_by=None,
                        claim_token=None, compute_ready_at=None, compute_available_at=None):
        with self.world.mu:
            ticket = self.world.tickets[ticket_id]
            if self._owns(ticket, claim_token):
                ticket.update(compute_pod=compute_pod, compute_pod_ip=compute_pod_ip)
            return dict(ticket)

    def extend_allocation_deadline(self, ticket_id, claim_token):
        with self.world.mu:
            return self._owns(self.world.tickets[ticket_id], claim_token)

    def mark_failed(self, ticket_id, reason, claim_token=None):
        with self.world.mu:
            ticket = self.world.tickets[ticket_id]
            ticket["status"] = "failed"
            return dict(ticket)


class SimAllocator(ComputeAllocator):
    """마운트 대신 티켓을 assigned 로 두고, 세션이 끝나면 파드를 반납한다."""

    hold_seconds = 0.02

    def _execute_allocated_ticket(self, ticket):
        world = self.provider.world
        ticket_id = ticket["ticket_id"]
        pod = ticket["compute_pod"]
        with world.mu:
            current = world.tickets[ticket_id]
            owns = current["status"] == "allocating" and current["claim_token"] == ticket["claim_token"]
            if owns:
                current["status"] = "assigned"
                current["final_pod"] = pod
                world.assignments.setdefault(pod, []).append(ticket_id)
        if not owns:
            self.provider.release_pod(pod)
            return {"status": "queued"}

        def _session_end():
            self.provider.release_pod(pod)
            with world.mu:
                world.tickets[ticket_id]["status"] = "done"

        threading.Timer(self.hold_seconds * random.uniform(0.5, 2.0), _session_end).start()
        return {"status": "assigned"}


def make(world: World, create_latency: float = 0.0):
    provider = FakeProvider(world, create_latency=create_latency)
    queues = FakeQueues(world)
    tickets = FakeTickets(world)
    queues.tickets = tickets
    allocator = SimAllocator(provider, queues, tickets)
    reconciler = DirectBufferReconciler(provider, queues)
    return provider, queues, tickets, allocator, reconciler


def no_stale(ticket):
    return {"status": "skipped"}


def run_detached_threads_inline():
    """drain 이 티켓마다 띄우는 스레드를 그 자리에서 실행한다(결정적 단위 테스트용)."""

    class InlineThread:
        def __init__(self, target, args=(), name=None, daemon=None):
            self.target, self.args = target, args

        def start(self):
            self.target(*self.args)

    return patch.dict(ComputeAllocator._drain_wait_queue_for_type_impl.__globals__,
                      {"threading": SimpleNamespace(Thread=InlineThread)})


def wait_until(predicate, timeout=5.0, world: Optional[World] = None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if world is not None:
            world.tick()
        if predicate():
            return True
        time.sleep(0.002)
    return False


def pod_event(status):
    return SimpleNamespace(metadata=SimpleNamespace(labels={"compute-type": CT, "compute-status": status}))


# ---------------------------------------------------------------------- 할당(drain)


class DirectDrainTests(unittest.TestCase):
    def test_each_ticket_gets_its_own_new_pod(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(2)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 2)
        self.assertEqual(world.created["request"], 2)
        for ticket_id in ("t0001", "t0002"):
            self.assertEqual(world.tickets[ticket_id]["status"], "assigned")
        # 예약은 목록에 보일 때까지 남고, 리더가 목록에서 보면 뺀다.
        self.assertEqual(len(world.pending), 2)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.pending, {})

    def test_listed_reservation_does_not_outlive_its_pod(self):
        """세션이 끝나 파드가 사라진 뒤 남은 예약이 자리를 쥐지 않는다(리더가 목록에서 보고 뺐다)."""
        world = World(capacity=1)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        reconciler.reconcile_type(CT)
        pod = world.tickets["t0001"]["compute_pod"]
        world.terminate(pod)
        self.assertTrue(wait_until(lambda: pod not in world.pods, world=world))
        world.enqueue(1)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 1)

    def test_leftover_idle_is_never_handed_out(self):
        """다른 arm 이 남긴 Ready idle 이 있어도 새로 만든다(cold 측정이 warm hit 로 오염되지 않게)."""
        world = World(capacity=5)
        world.add_pod("leftover", "idle", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertNotIn("leftover", world.assignments)
        self.assertEqual(world.pods["leftover"]["buffer_status"], "idle")
        self.assertEqual(world.created["request"], 1)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")

    def test_leftover_idle_still_counts_toward_n(self):
        world = World(capacity=2)
        world.add_pod("leftover", "idle", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(3)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 1)
        self.assertEqual(world.violations, [])

    def test_full_capacity_leaves_tickets_queued_in_order(self):
        world = World(capacity=1)
        world.add_pod("busy", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(2)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 0)
        self.assertEqual(result.get("capacity_blocked"), "capacity")
        self.assertEqual([tid for _, tid in world.queue], ["t0001", "t0002"])

    def test_capacity_counts_pending_creates_and_missing_claims(self):
        world = World(capacity=4)
        world.add_pod("busy", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        # 생성 중인 요청 파드 하나 + 파드가 아직 안 보이는 claim 하나.
        queues.add_pending_create(CT, "pod-pending", "request")
        world.enqueue(1)
        with world.mu:
            world.tickets["t0001"].update(status="allocating", claim_token="c")
            world.queue.clear()
        world.enqueue(5)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        # 4 - 1(busy) - 1(생성 중) - 1(파드 없는 claim) = 1
        self.assertEqual(result["claimed"], 1)
        self.assertEqual(world.created["request"], 1)

    def test_listed_pending_name_is_not_counted_twice(self):
        world = World(capacity=3)
        provider, queues, tickets, allocator, _ = make(world)
        # 생성 중 목록에 남아 있지만 이미 목록에 보이는(지우는 중) 파드.
        world.add_pod("pod-gone", "assigned", {}, ready=True)
        queues.add_pending_create(CT, "pod-gone", "request")
        world.terminate("pod-gone")
        world.enqueue(5)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 3)

    def test_cancel_before_reservation_check_creates_nothing(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        original_add = queues.add_pending_create
        cancelled = []

        def cancel_then_add(compute_type, name, kind):
            world.cancel("t0001")
            cancelled.append(name)
            original_add(compute_type, name, kind)

        queues.add_pending_create = cancel_then_add
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        # 취소가 실제로 일어났고(가짜가 불렸고), 그 몫으로 만들지 않았고, 자리도 돌려줬다.
        self.assertEqual(len(cancelled), 1)
        self.assertEqual(world.tickets["t0001"]["status"], "cancelled")
        self.assertEqual(world.created["request"], 0)
        self.assertEqual(world.pending, {})

    def test_cancel_after_reservation_keeps_the_slot(self):
        """확인 뒤 취소된 claim 의 파드는 그래도 만들어진다 - 그 자리는 생성 중 목록이 쥔다."""
        world = World(capacity=2)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        original_create = provider.create_pod_for_ticket
        seen = {}

        def cancel_then_other_replica_drains(ticket, pod_name=None):
            world.cancel(ticket["ticket_id"])
            world.enqueue(3)
            # 파드가 목록에 뜨기 전에 다른 복제본이 drain 한다(티켓 스레드는 띄우지 않는다).
            silent = SimpleNamespace(Thread=lambda **kwargs: SimpleNamespace(start=lambda: None))
            with patch.dict(ComputeAllocator._drain_wait_queue_for_type_impl.__globals__, {"threading": silent}):
                seen["result"] = SimAllocator(provider, queues, tickets).drain_wait_queue_for_type(CT, no_stale)
            return original_create(ticket, pod_name)

        provider.create_pod_for_ticket = cancel_then_other_replica_drains
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        # N=2 에서 생성 중 이름 하나가 자리를 쥐므로 다른 복제본은 하나만 claim 한다.
        self.assertEqual(seen["result"]["claimed"], 1)
        self.assertEqual(world.violations, [])

    def test_rejected_create_returns_the_slot_but_unanswered_keeps_it(self):
        for error, kept in ((FakeApiException(422, "Invalid"), 0),
                            (FakeApiException(504, "Timeout"), 1),
                            (TimeoutError("read timed out"), 1)):
            world = World(capacity=5)
            provider, queues, tickets, allocator, _ = make(world)
            provider.fail_create_with = error
            world.enqueue(1)
            with run_detached_threads_inline():
                allocator.drain_wait_queue_for_type(CT, no_stale)
            # 응답 없는 생성은 서버에 반영됐을 수 있어 상한까지 자리를 쥔다.
            self.assertEqual(len(world.pending), kept, repr(error))
            self.assertEqual(world.tickets["t0001"]["status"], "queued")
            self.assertEqual(world.tickets["t0001"]["retry_count"], 1)

    def test_mount_failure_after_assignment_keeps_the_pod(self):
        """할당이 확정된 뒤의 예외(문맥 기록 실패 등)로 쓰는 중인 파드를 지우지 않는다."""
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)

        class Exploding(SimAllocator):
            def _execute_allocated_ticket(self, ticket):
                with world.mu:
                    world.tickets[ticket["ticket_id"]]["status"] = "assigned"
                raise OSError("redis down after mark_assigned")

        allocator = Exploding(provider, queues, tickets)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        pod = world.tickets["t0001"]["compute_pod"]
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")
        self.assertFalse(world.pods[pod]["terminating"])

    def test_error_mid_batch_still_starts_earlier_claims(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        original_add = queues.add_pending_create
        calls = {"n": 0}

        def second_fails(compute_type, name, kind):
            calls["n"] += 1
            if calls["n"] == 2:
                raise FakeQueueUnavailable("redis timeout")
            return original_add(compute_type, name, kind)

        queues.add_pending_create = second_fails
        world.enqueue(3)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")
        self.assertEqual(world.tickets["t0002"]["status"], "queued")
        self.assertEqual(world.tickets["t0002"]["retry_count"], 0)
        self.assertEqual(len(result["errors"]), 1)

    def test_missing_capacity_creates_nothing(self):
        world = World(capacity=None)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(2)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 0)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created, {"request": 0})
        self.assertEqual(world.pods, {})

    def test_unknown_capacity_claims_nothing(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(3)

        def broken_snapshot(compute_type=None):
            raise OSError("apiserver timeout")

        provider.snapshot = broken_snapshot
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 0)
        self.assertEqual(result.get("capacity_blocked"), "capacity_unknown")
        self.assertEqual(sum(1 for t in world.tickets.values() if t["status"] == "queued"), 3)

    def test_provision_failure_requeues_and_releases(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)

        def dies(name, keep_claim=None):
            raise RuntimeError("pod failed")

        provider.wait_pod_ready = dies
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        ticket = world.tickets["t0001"]
        self.assertEqual(ticket["status"], "queued")
        self.assertEqual(ticket["retry_count"], 1)
        self.assertTrue(all(p["terminating"] for p in world.pods.values()))

    def test_request_pod_with_lost_reservation_but_fresh_claim_is_created(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        original_refresh = queues.refresh_pending_creates

        def reservation_pruned(compute_type, names, kind):
            with world.mu:
                for name in names:
                    world.pending.pop(name, None)
            return original_refresh(compute_type, names, kind)

        queues.refresh_pending_creates = reservation_pruned
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.created["request"], 1)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")

    def test_request_pod_with_lost_reservation_and_stale_claim_is_requeued(self):
        world = World(capacity=5)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        original_refresh = queues.refresh_pending_creates

        def reservation_and_claim_expired(compute_type, names, kind):
            with world.mu:
                for name in names:
                    world.pending.pop(name, None)
                world.tickets["t0001"]["stale"] = True
            return original_refresh(compute_type, names, kind)

        queues.refresh_pending_creates = reservation_and_claim_expired
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.created["request"], 0)
        self.assertEqual(world.tickets["t0001"]["status"], "queued")
        self.assertEqual(world.tickets["t0001"]["retry_count"], 0)


# ---------------------------------------------------------------------- 리더


class DirectReconcilerTests(unittest.TestCase):
    def test_never_creates_pods(self):
        world = World(capacity=10)
        provider, queues, tickets, allocator, reconciler = make(world)
        for _ in range(3):
            reconciler.reconcile_type(CT)
        world.enqueue(3)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.pods, {})
        self.assertEqual(world.pending, {})
        self.assertEqual([c for c in provider.calls if c[0] != "delete_idle"], [])

    def test_leftover_idle_is_deleted_and_assigned_is_left_alone(self):
        world = World(capacity=10)
        world.add_pod("idle-ready", "idle", {}, ready=True)
        world.add_pod("idle-booting", "idle", {}, ready=False)
        world.add_pod("in-use", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"]["idle"], 2)
        self.assertEqual(outcome["record"]["deleted"], 2)
        self.assertTrue(world.pods["idle-ready"]["terminating"])
        self.assertTrue(world.pods["idle-booting"]["terminating"])
        self.assertFalse(world.pods["in-use"]["terminating"])

    def test_delete_precondition_spares_pod_changed_in_between(self):
        world = World(capacity=10)
        world.add_pod("idle-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        original_delete = provider.delete_idle_pod

        def changed_first(name, uid, rv):
            # 목록을 읽은 뒤 삭제 호출 전에 다른 쪽이 이 파드를 바꿨다.
            with world.mu:
                world.pods[name]["buffer_status"] = "assigned"
                world.bump(world.pods[name])
            return original_delete(name, uid, rv)

        provider.delete_idle_pod = changed_first
        reconciler.reconcile_type(CT)
        self.assertFalse(world.pods["idle-a"]["terminating"])

    def test_nothing_to_do_until_idle_remains_or_resync(self):
        world = World(capacity=10)
        provider, queues, tickets, allocator, reconciler = make(world)
        self.assertFalse(reconciler._nothing_to_do(CT))  # 직전 기록 없음
        reconciler.reconcile_type(CT)  # idle 없음
        self.assertTrue(reconciler._nothing_to_do(CT))
        with reconciler._status_lock:
            reconciler._status[CT]["updated_at"] -= reconciler.resync_seconds
        self.assertFalse(reconciler._nothing_to_do(CT))

        # 지우지 못한 idle 이 남았으면 거르지 않는다.
        world.add_pod("idle-a", "idle", {}, ready=True)
        provider.delete_idle_pod = lambda name, uid, rv: False
        reconciler.reconcile_type(CT)
        self.assertFalse(reconciler._nothing_to_do(CT))

    def test_only_idle_pod_events_wake_the_leader(self):
        world = World(capacity=10)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.on_buffer_event("MODIFIED", pod_event("assigned"))
        self.assertFalse(reconciler._wake)
        reconciler.on_buffer_event("ADDED", pod_event("idle"))
        self.assertTrue(reconciler._wake)

    def test_deletion_stops_when_leadership_is_lost(self):
        world = World(capacity=10)
        for index in range(3):
            world.add_pod(f"idle-{index}", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        leader = {"ok": True}
        reconciler.set_leadership_validator(lambda: leader["ok"])
        original_delete = provider.delete_idle_pod

        def delete_then_lose(name, uid, rv):
            leader["ok"] = False
            return original_delete(name, uid, rv)

        provider.delete_idle_pod = delete_then_lose
        reconciler.reconcile_type(CT)
        self.assertEqual(sum(1 for p in world.pods.values() if p["terminating"]), 1)

    def test_stale_generation_does_not_delete(self):
        world = World(capacity=10)
        for index in range(3):
            world.add_pod(f"idle-{index}", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler._generation = 5
        reconciler.reconcile_type(CT, generation=4)  # 이전 세대의 바퀴
        self.assertFalse(any(p["terminating"] for p in world.pods.values()))


# ---------------------------------------------------------------------- 동시 실행


class DirectConcurrencyTests(unittest.TestCase):
    def _simulate(self, capacity, tickets_total=60, seed=7, leftover_idle=0):
        random.seed(seed)
        world = World(capacity=capacity, boot_seconds=0.03)
        for index in range(leftover_idle):
            world.add_pod(f"leftover-{index}", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world, create_latency=0.01)
        # 두 번째 복제본: 같은 Redis·apiserver 를 보는 다른 프로세스.
        allocator2 = SimAllocator(provider, queues, tickets)
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
            emptied = wait_until(lambda: not world.live(), timeout=10.0)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=2)
        self.assertEqual(errors, [])
        self.assertEqual(world.violations, [], f"N={capacity} 을 넘었다: {world.violations[:5]}")
        self.assertTrue(emptied, "수요가 끝났는데 파드가 남았다")
        # 파드 하나는 요청 하나에만 쓰고, 남은 idle 은 아무 요청도 받지 않았다.
        for pod, owners in world.assignments.items():
            self.assertEqual(len(owners), 1, pod)
            self.assertFalse(pod.startswith("leftover-"), pod)
        done = [t for t in ids if world.tickets[t]["status"] == "done"]
        self.assertGreater(len(done), 0)
        # 만든 파드는 할당됐거나, 만든 뒤 취소된 티켓 몫으로 버려졌다.
        cancelled = sum(1 for t in ids if world.tickets[t]["status"] == "cancelled")
        self.assertLessEqual(world.created["request"] - len(world.assignments), cancelled)
        return world

    def test_capacity_holds(self):
        world = self._simulate(capacity=6)
        self.assertLessEqual(world.max_live, 6)

    def test_tight_capacity(self):
        self._simulate(capacity=2, tickets_total=40, seed=11)

    def test_leftover_idle_is_cleared_and_never_used(self):
        self._simulate(capacity=6, seed=3, leftover_idle=3)


# ---------------------------------------------------------------------- 배포 계약


class DirectDeployContractTests(unittest.TestCase):
    REPO = ROOT.parent

    def _load_yaml(self, path):
        import yaml

        return list(yaml.safe_load_all((self.REPO / path).read_text(encoding="utf-8")))

    def test_pod_template_keeps_production_selector(self):
        pod = self._load_yaml("controller/manifests/direct_buffer/compute-general-pod.yaml")[0]
        self.assertEqual(pod["kind"], "Pod")
        # 운영 노드셀렉터는 그대로 두고 우리 라벨만 더한다.
        self.assertEqual(pod["spec"]["nodeSelector"]["kubessh"], "general_node")
        self.assertEqual(pod["spec"]["nodeSelector"]["k8s-dynamic-allocator/compute"], "true")
        labels = pod["metadata"]["labels"]
        self.assertEqual(labels["app"], "compute-pod")
        self.assertIn("assigned-user", labels)
        self.assertNotEqual(labels["compute-status"], "available")
        # 다른 arm(warm buffer, reuse)의 compute 파드와 같은 사양이어야 할당 방식만 다른
        # 비교가 된다.
        agent = pod["spec"]["containers"][0]
        self.assertEqual(agent["resources"]["requests"], {"cpu": "1", "memory": "2Gi"})
        self.assertEqual(agent["resources"]["limits"], {"cpu": "1", "memory": "2Gi"})
        self.assertEqual(agent["imagePullPolicy"], "Always")
        self.assertEqual(agent["readinessProbe"]["periodSeconds"], 2)
        self.assertNotIn("schedulingGates", pod["spec"])

    def test_no_precreation_path_in_the_controller(self):
        """미리 만드는 경로는 끄는 것이 아니라 없다(정적 R=0 이나 토글로 cold 를 흉내 내지 않는다)."""
        compute = ROOT / "services" / "compute"
        provider = (compute / "direct_buffer_provider.py").read_text(encoding="utf-8")
        allocator = (compute / "allocator.py").read_text(encoding="utf-8")
        reconciler = (compute / "direct_buffer_reconciler.py").read_text(encoding="utf-8")
        for name in ("def create_idle_pod", "def claim_idle_pod", "def new_idle_name", "def read_reserve"):
            self.assertNotIn(name, provider)
        self.assertNotIn("claim_idle", allocator)
        self.assertNotIn("create_pod", reconciler)
        self.assertNotIn("BufferPolicy", reconciler)
        self.assertFalse((compute / "buffer_policy.py").exists())
        self.assertIn('allocation_mode = "cold_start"', provider)

    def test_deploy_passes_n_through_controller_env_with_one_rollout(self):
        script = (self.REPO / "deploy/scripts/deploy.sh").read_text(encoding="utf-8")
        inject = script.index('set_buffer_policy.sh" "${rendered}" "${buffer_capacity}"')
        # 적용보다 먼저 넣는다. 적용 뒤 set env 로 바꾸면 rollout 이 두 번 돈다.
        self.assertLess(inject, script.index("apply_stage bootstrap"))
        self.assertNotIn("kubectl set env deployment/controller", script)
        self.assertNotIn("BUFFER_RESERVE", script)
        # smoke: 이 arm 으로 돌고, R 이 없고, idle 파드가 0 이다.
        self.assertIn('status.get("allocation_mode") == "cold_start"', script)
        self.assertIn('status.get("R") is None', script)
        self.assertIn('status.get("available") == 0', script)
        self.assertIn("kubectl delete deployment -l app=compute-pod", script)

    def test_n_injection_sets_the_env_value_exactly_once(self):
        import shutil
        import subprocess
        import tempfile

        shell = shutil.which("sh")
        if not shell or not shutil.which("awk"):
            self.skipTest("sh/awk 가 없는 환경")
        script = str(self.REPO / "deploy/scripts/set_buffer_policy.sh")
        rendered = "\n".join([
            "        env:",
            "        - name: BUFFER_CAPACITY",
            '          value: ""',
            "",
        ])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "rendered.yaml"
            path.write_text(rendered, encoding="utf-8")
            subprocess.run([shell, script, str(path), "40"], check=True)
            self.assertIn('value: "40"', path.read_text(encoding="utf-8"))
            # 없으면 실패해서 배포를 멈춘다.
            path.write_text("        env:\n", encoding="utf-8")
            failed = subprocess.run([shell, script, str(path), "40"], capture_output=True)
            self.assertNotEqual(failed.returncode, 0)

    def test_controller_manifest_names_the_arm_and_has_no_r(self):
        documents = self._load_yaml("deploy/base/controller.yaml")
        controller = next(d for d in documents if d and d.get("kind") == "Deployment")
        env = {
            item["name"]: item.get("value")
            for item in controller["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertEqual(env["COMPUTE_ALLOCATION_MODE"], "cold_start")
        self.assertNotIn("BUFFER_RESERVE", env)
        self.assertNotIn("BUFFER_DYNAMIC_RESERVE_ENABLED", env)
        self.assertIn("BUFFER_CAPACITY", env)

    def test_jenkins_takes_only_n(self):
        jenkinsfile = self.REPO / "Jenkinsfile"
        if not jenkinsfile.exists():
            self.skipTest("테스트 이미지에는 Jenkinsfile 이 없다")
        text = jenkinsfile.read_text(encoding="utf-8")
        self.assertIn("name: 'BUFFER_CAPACITY'", text)
        self.assertNotIn("BUFFER_RESERVE", text)


if __name__ == "__main__":
    unittest.main()
