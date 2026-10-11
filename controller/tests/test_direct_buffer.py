"""컨트롤러가 직접 관리하는 웜 버퍼(warm_buffer_direct)의 불변식.

실제 클러스터 없이 가짜 클러스터·큐 위에서 확인한다.
  - 두 복제본의 할당(drain)과 리더 조정이 동시에 돌아도 살아 있는 파드가 N 을 넘지 않는다.
  - idle 파드 할당이 충돌하면 다음 파드로 넘어가고, 다 실패하면 티켓을 돌려놓는다.
  - 묶는 사이 취소된 티켓 몫으로 파드를 새로 만들지 않는다.
  - 생성 중 목록이 N 계산에 들어가고, 거절(4xx)만 목록에서 뺀다.
  - 버퍼 목표는 동적 R 하나다: R 이 바닥이고, 몰림 뒤에는 쿨다운 창만큼 버퍼를 붙잡는다.
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
    # 정책(BufferPolicy)
    BUFFER_RECONCILE_RESYNC_SECONDS=5,
    BUFFER_SCALE_DOWN_COOLDOWN_SAMPLES=20,
    BUFFER_SCALE_DOWN_COOLDOWN_MIN_SAMPLES=3,
    # direct 조정
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
    kubernetes = types.ModuleType("kubernetes")
    kclient = types.ModuleType("kubernetes.client")
    rest = types.ModuleType("kubernetes.client.rest")
    rest.ApiException = FakeApiException
    kubernetes.client = kclient
    kclient.rest = rest
    modules.update({
        "config": config,
        queue.__name__: queue,
        "kubernetes": kubernetes,
        "kubernetes.client": kclient,
        "kubernetes.client.rest": rest,
    })
    loaded = {}
    with patch.dict(sys.modules, modules):
        for short in ("buffer_policy", "direct_buffer_reconciler"):
            name = f"controller.services.compute.{short}"
            spec = importlib.util.spec_from_file_location(
                name, ROOT / "services" / "compute" / f"{short}.py"
            )
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
            loaded[short] = module
    return loaded["direct_buffer_reconciler"].DirectBufferReconciler


ComputeAllocator = load_allocator_class()
DirectBufferReconciler = load_direct_reconciler_class()

CT = "general"


# ---------------------------------------------------------------------- 가짜 세계


class World:
    """apiserver(파드)와 Redis(티켓·락·생성 중 목록)를 한곳에 둔다.

    파드가 하나 생길 때마다 살아 있는(지우는 중이 아닌) 파드 수가 N 이하인지 본다.
    """

    def __init__(self, capacity: Optional[int], reserve: int, boot_seconds: float = 0.03):
        self.mu = threading.RLock()
        self.template_hash = "gen-1"
        self.capacity = capacity
        self.reserve = reserve
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
        self.created = {"idle": 0, "request": 0}
        self.assignments: Dict[str, List[str]] = {}

    # 파드
    def live(self) -> List[Dict]:
        # 다른 스레드가 파드를 넣고 빼므로 잠금 안에서 복사한다.
        with self.mu:
            return [dict(p) for p in self.pods.values() if not p["terminating"]]

    def add_pod(self, name: str, status: str, annotations: Dict, *, ready: bool = False,
                age_seconds: float = 0.0, not_ready_since: Optional[datetime] = None,
                template_hash: Optional[str] = None) -> Dict:
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
                "not_ready_since": not_ready_since,
                "template_hash": self.template_hash if template_hash is None else template_hash,
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
    allocation_mode = "warm_buffer_direct"
    LABEL_COMPUTE_TYPE = "compute-type"
    STATUS_IDLE = "idle"
    STATUS_ASSIGNED = "assigned"

    def __init__(self, world: World, create_latency: float = 0.0):
        self.world = world
        self.create_latency = create_latency
        self.names = itertools.count(1)
        self.fail_create_with: Optional[Exception] = None
        self.claim_conflicts = set()
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
                    "ready_at": datetime.now(timezone.utc) if p["ready"] else None,
                    "not_ready_since": p.get("not_ready_since"),
                    "ip": "10.0.0.9" if p["ready"] else None,
                    "template_hash": p.get("template_hash", ""),
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

    def read_reserve(self, compute_type=None):
        return self.world.reserve

    def new_idle_name(self, compute_type):
        return f"compute-{compute_type}-warm-{next(self.names)}"

    def template_hash(self, compute_type):
        return self.world.template_hash

    def request_pod_name(self, ticket):
        return f"compute-general-req-{ticket['ticket_id']}-{ticket.get('retry_count') or 0}"

    def _latency(self):
        if self.create_latency:
            time.sleep(random.uniform(0, self.create_latency))

    def create_idle_pod(self, compute_type, name):
        self.calls.append(("create_idle", name))
        if self.fail_create_with is not None:
            raise self.fail_create_with
        self._latency()
        self.world.add_pod(name, "idle", {"origin": "idle"})
        with self.world.mu:
            self.world.created["idle"] += 1

    def create_pod_for_ticket(self, ticket, pod_name=None):
        name = pod_name or self.request_pod_name(ticket)
        self.calls.append(("create_request", name))
        self._latency()
        self.world.add_pod(name, "assigned", {
            "ticket": ticket["ticket_id"], "claim": ticket.get("claim_token") or "",
        })
        with self.world.mu:
            self.world.created["request"] += 1
        return name

    def claim_idle_pod(self, name, rv, user_pod, ticket_id, claim_token, expected_annotations=None):
        self.calls.append(("claim_idle", name, ticket_id))
        if name in self.claim_conflicts:
            return False
        with self.world.mu:
            pod = self.world.pods.get(name)
            if (not pod or pod["terminating"] or pod["resource_version"] != rv
                    or pod["buffer_status"] != "idle"):
                return False
            pod["buffer_status"] = "assigned"
            pod["annotations"].update({"ticket": ticket_id, "claim": claim_token})
            self.world.bump(pod)
            self.world.assignments.setdefault(name, []).append(ticket_id)
            return True

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

    def wait_pod_ready(self, name, keep_claim=None, must_exist=False):
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

    def queued_count(self, compute_type):
        with self.world.mu:
            return sum(1 for _, tid in self.world.queue if self.world.tickets[tid]["status"] == "queued")

    def claimable_queued_count(self, compute_type):
        with self.world.mu:
            return sum(
                1 for _, tid in self.world.queue
                if self.world.tickets[tid]["status"] == "queued"
                and not self.world.tickets[tid].get("disconnected")
            )

    def pending_creates(self, compute_type, max_age_seconds):
        cutoff = time.time() - max_age_seconds
        with self.world.mu:
            for name in [n for n, at in self.world.pending.items() if at < cutoff]:
                del self.world.pending[name]
            return {name: self.world.pending_kind.get(name, "idle") for name in self.world.pending}

    def add_pending_create(self, compute_type, name, kind):
        with self.world.mu:
            self.world.pending[name] = time.time()
            self.world.pending_kind[name] = kind

    def add_pending_creates_if_locked(self, compute_type, names, lock_token, kind="idle"):
        with self.world.mu:
            if self.world.lock_token != lock_token:
                return False
            for name in names:
                self.world.pending[name] = time.time()
                self.world.pending_kind[name] = kind
            return True

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

    def churn_samples(self, compute_type, kind):
        raise FakeQueueUnavailable("no shared samples in tests")

    def record_churn_sample(self, *args, **kwargs):
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
        if self.before_record is not None:
            self.before_record(ticket_id)
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


# ---------------------------------------------------------------------- 할당(drain)


class DirectDrainTests(unittest.TestCase):
    def test_ready_idle_pod_is_used_before_creating(self):
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 1)
        self.assertEqual(world.assignments.get("warm-a"), ["t0001"])
        self.assertEqual(world.created["request"], 0)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")

    def test_ready_idle_is_preferred_over_booting_idle(self):
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-booting", "idle", {}, ready=False, age_seconds=10)
        world.add_pod("warm-ready", "idle", {}, ready=True, age_seconds=1)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.assignments.get("warm-ready"), ["t0001"])
        self.assertNotIn("warm-booting", world.assignments)

    def test_idle_claim_conflict_falls_through_to_next_pod(self):
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True, age_seconds=2)
        world.add_pod("warm-b", "idle", {}, ready=True, age_seconds=1)
        provider, queues, tickets, allocator, _ = make(world)
        provider.claim_conflicts.add("warm-a")
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.assignments.get("warm-b"), ["t0001"])
        self.assertEqual(world.created["request"], 0)

    def test_all_idle_lost_and_no_room_requeues_ticket(self):
        world = World(capacity=2, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True)
        world.add_pod("busy", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        provider.claim_conflicts.add("warm-a")
        world.enqueue(2)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 0)
        self.assertEqual(tickets.requeued, ["t0001"])
        self.assertEqual(world.tickets["t0001"]["status"], "queued")
        self.assertEqual(world.tickets["t0001"]["retry_count"], 0)
        # 맨 앞으로 돌아간다.
        self.assertEqual(world.queue[0][1], "t0001")
        self.assertEqual(world.created["request"], 0)

    def test_ticket_cancelled_while_binding_idle_creates_nothing(self):
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        tickets.before_record = world.cancel
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.tickets["t0001"]["status"], "cancelled")
        self.assertEqual(world.created["request"], 0)
        self.assertTrue(world.pods["warm-a"]["terminating"])

    def test_capacity_counts_pending_creates_and_missing_claims(self):
        world = World(capacity=4, reserve=0)
        world.add_pod("busy", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        # 생성 중인 idle 하나 + 파드가 아직 안 보이는 claim 하나.
        queues.add_pending_create(CT, "warm-pending", "idle")
        world.enqueue(1)
        with world.mu:
            world.tickets["t0001"].update(status="allocating", claim_token="c")
            world.queue.clear()
        world.enqueue(5)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        # 4 - 1(busy) - 1(생성 중 idle) - 1(파드 없는 claim) = 1
        self.assertEqual(result["claimed"], 1)
        self.assertEqual(world.created["request"], 1)

    def test_listed_pending_name_is_not_counted_twice(self):
        world = World(capacity=3, reserve=0)
        provider, queues, tickets, allocator, _ = make(world)
        # 생성 중 목록에 남아 있지만 이미 목록에 보이는(지우는 중) 파드.
        world.add_pod("warm-gone", "idle", {}, ready=True)
        queues.add_pending_create(CT, "warm-gone", "idle")
        world.terminate("warm-gone")
        world.enqueue(5)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 3)

    def test_cancel_before_reservation_check_creates_nothing(self):
        world = World(capacity=5, reserve=0)
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
        world = World(capacity=2, reserve=0)
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

    def test_ready_idle_is_committed_without_rereading_the_pod(self):
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, _ = make(world)
        provider.wait_pod_ready = None  # 불리면 TypeError 로 실패한다
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")
        self.assertEqual(world.tickets["t0001"]["compute_pod_ip"], "10.0.0.9")

    def test_broken_idle_is_not_handed_out(self):
        """Ready 였다가 NotReady 가 된 idle(에이전트가 죽음)은 내주지 않고 새로 만든다."""
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-broken", "idle", {}, ready=False,
                      not_ready_since=datetime.now(timezone.utc) - timedelta(seconds=5))
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertNotIn("warm-broken", world.assignments)
        self.assertEqual(world.created["request"], 1)

    def test_mount_failure_after_assignment_keeps_the_pod(self):
        """할당이 확정된 뒤의 예외(문맥 기록 실패 등)로 쓰는 중인 파드를 지우지 않는다."""
        world = World(capacity=5, reserve=0)
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
        world = World(capacity=5, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True, age_seconds=2)
        world.add_pod("warm-b", "idle", {}, ready=True, age_seconds=1)
        provider, queues, tickets, allocator, _ = make(world)
        original_claim = provider.claim_idle_pod

        def second_times_out(name, *args, **kwargs):
            if name == "warm-b":
                raise TimeoutError("patch timed out")
            return original_claim(name, *args, **kwargs)

        provider.claim_idle_pod = second_times_out
        world.enqueue(3)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(world.tickets["t0001"]["status"], "assigned")
        self.assertEqual(world.tickets["t0002"]["status"], "queued")
        self.assertEqual(len(result["errors"]), 1)

    def test_missing_capacity_creates_nothing(self):
        world = World(capacity=None, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(2)
        with run_detached_threads_inline():
            result = allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertEqual(result["claimed"], 0)
        with world.mu:
            world.queue.clear()
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created, {"idle": 0, "request": 0})

    def test_unknown_capacity_claims_nothing(self):
        world = World(capacity=5, reserve=0)
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
        world = World(capacity=5, reserve=0)
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)

        def dies(name, keep_claim=None, must_exist=False):
            raise RuntimeError("pod failed")

        provider.wait_pod_ready = dies
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        ticket = world.tickets["t0001"]
        self.assertEqual(ticket["status"], "queued")
        self.assertEqual(ticket["retry_count"], 1)
        self.assertTrue(all(p["terminating"] for p in world.pods.values()))


# ---------------------------------------------------------------------- 리더 조정


class DirectReconcilerTests(unittest.TestCase):
    def test_creates_reserve_when_idle(self):
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"]["target"], 2)
        self.assertEqual(world.created["idle"], 2)
        # 만든 이름은 목록에 보이기 전까지 생성 중 목록에 있다.
        self.assertEqual(len(world.pending), 2)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 2)
        self.assertEqual(world.pending, {})

    def test_r_zero_creates_nothing(self):
        world = World(capacity=10, reserve=0)
        provider, queues, tickets, allocator, reconciler = make(world)
        for _ in range(3):
            reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 0)

    def test_target_is_bounded_by_room(self):
        world = World(capacity=5, reserve=2)
        for i in range(4):
            world.add_pod(f"busy-{i}", "assigned", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"]["target"], 1)
        self.assertEqual(outcome["record"]["bound_by"], "room")
        self.assertEqual(world.created["idle"], 1)

    def test_does_not_prebuild_while_tickets_are_queued(self):
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(3)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 0)

    def test_burst_is_held_as_buffer_after_it_is_served(self):
        """몰린 요청이 자기 파드를 띄우는 동안은 그 파드가 버퍼다(Deployment 버퍼와 같다).

        받아 간 뒤에는 쿨다운 창이 목표를 붙잡아 그만큼 idle 을 채운다.
        """
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.policy.cooldown_seconds = lambda compute_type: 30.0
        ids = world.enqueue(5)
        with world.mu:
            world.queue.clear()
            for index, ticket_id in enumerate(ids):
                world.tickets[ticket_id].update(status="allocating", claim_token="c")
        # 셋은 파드가 부팅 중, 둘은 아직 파드가 안 보인다.
        for ticket_id in ids[:3]:
            world.add_pod(f"req-{ticket_id}", "assigned", {}, ready=False)
            world.tickets[ticket_id]["compute_pod"] = f"req-{ticket_id}"
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"]["in_flight"], 5)
        self.assertEqual(outcome["record"]["desired"], 5)
        self.assertEqual(world.created["idle"], 0)

        # 다섯 다 Ready 파드를 받아 갔다.
        with world.mu:
            for ticket_id in ids[3:]:
                world.add_pod(f"req-{ticket_id}", "assigned", {}, ready=True)
            for ticket_id in ids:
                world.pods[f"req-{ticket_id}"]["ready"] = True
                world.tickets[ticket_id].update(status="assigned", compute_pod=f"req-{ticket_id}")
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"]["desired"], 2)
        self.assertEqual(outcome["record"]["target"], 5)
        self.assertEqual(world.created["idle"], 5)

    def test_rejected_create_is_forgotten_but_unanswered_is_kept(self):
        world = World(capacity=10, reserve=1)
        provider, queues, tickets, allocator, reconciler = make(world)
        provider.fail_create_with = FakeApiException(422, "Invalid")
        reconciler.reconcile_type(CT)
        self.assertEqual(world.pending, {})

        world2 = World(capacity=10, reserve=1)
        provider2, queues2, tickets2, allocator2, reconciler2 = make(world2)
        provider2.fail_create_with = FakeApiException(504, "Timeout")
        reconciler2.reconcile_type(CT)
        self.assertEqual(len(world2.pending), 1)
        # 응답 없는 생성은 서버에 반영됐을 수 있어 자리를 계속 쥔다 - 다시 만들지 않는다.
        provider2.fail_create_with = None
        reconciler2.reconcile_type(CT)
        self.assertEqual(world2.created["idle"], 0)

        provider3_error = TimeoutError("read timed out")
        world3 = World(capacity=10, reserve=1)
        provider3, _, _, _, reconciler3 = make(world3)
        provider3.fail_create_with = provider3_error
        reconciler3.reconcile_type(CT)
        self.assertEqual(len(world3.pending), 1)

    def test_scale_down_deletes_booting_then_newest(self):
        world = World(capacity=10, reserve=1)
        world.add_pod("old-ready", "idle", {}, ready=True, age_seconds=30)
        world.add_pod("new-ready", "idle", {}, ready=True, age_seconds=5)
        world.add_pod("booting", "idle", {}, ready=False, age_seconds=1)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.reconcile_type(CT)
        self.assertTrue(world.pods["booting"]["terminating"])
        self.assertTrue(world.pods["new-ready"]["terminating"])
        self.assertFalse(world.pods["old-ready"]["terminating"])

    def test_delete_precondition_spares_pod_assigned_in_between(self):
        world = World(capacity=10, reserve=0)
        world.add_pod("warm-a", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        original_delete = provider.delete_idle_pod

        def assign_first(name, uid, rv):
            # 락을 놓은 뒤 삭제 호출 전에 다른 복제본이 이 파드를 가져갔다.
            self.assertTrue(provider.claim_idle_pod(name, world.pods[name]["resource_version"],
                                                    "user", "t-x", "c"))
            return original_delete(name, uid, rv)

        provider.delete_idle_pod = assign_first
        reconciler.reconcile_type(CT)
        self.assertFalse(world.pods["warm-a"]["terminating"])
        self.assertEqual(world.pods["warm-a"]["buffer_status"], "assigned")

    def test_stuck_idle_is_deleted_after_ready_timeout(self):
        world = World(capacity=10, reserve=1)
        world.add_pod("stuck", "idle", {}, ready=False,
                      age_seconds=SETTINGS.COLD_START_POD_READY_TIMEOUT_SECONDS + 1)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.reconcile_type(CT)
        self.assertTrue(world.pods["stuck"]["terminating"])

    def test_nothing_to_do_only_when_saturated_and_recent(self):
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        self.assertFalse(reconciler._nothing_to_do(CT))  # 직전 기록 없음
        world.enqueue(1)
        reconciler.reconcile_type(CT)  # 대기 중이라 만들지 않는다 → idle 0
        self.assertTrue(reconciler._nothing_to_do(CT))
        with world.mu:
            world.queue.clear()
            world.tickets["t0001"]["status"] = "cancelled"
        # 거름망은 Redis 를 보지 않고 직전 기록만 본다. 큐가 비어도 기록이 resync 주기
        # 안이면 건너뛰고, 주기가 지나면 다시 계산한다(보충 지연은 그 주기까지).
        self.assertTrue(reconciler._nothing_to_do(CT))
        with reconciler._status_lock:
            reconciler._status[CT]["updated_at"] -= reconciler.resync_seconds
        self.assertFalse(reconciler._nothing_to_do(CT))
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 2)

    def test_disconnected_ticket_does_not_stop_the_buffer(self):
        """폴링을 멈춘 클라이언트의 티켓은 claim 이 건너뛴다. 그것 때문에 미리 만들기를 멈추지 않는다."""
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        ids = world.enqueue(1)
        world.tickets[ids[0]]["disconnected"] = True
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 2)

    def test_lock_missed_asks_for_retry(self):
        world = World(capacity=10, reserve=2)
        provider, queues, tickets, allocator, reconciler = make(world)
        world.lock_token = "someone-else"
        outcome = reconciler.reconcile_type(CT)
        self.assertTrue(outcome["retry"])
        self.assertEqual(world.created["idle"], 0)


# ---------------------------------------------------------------------- 동시 실행


class DirectConcurrencyTests(unittest.TestCase):
    def _simulate(self, capacity, reserve, tickets_total=60, seed=7):
        random.seed(seed)
        world = World(capacity=capacity, reserve=reserve, boot_seconds=0.03)
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
        cancelled = set()
        try:
            # 실험처럼 R 개가 Ready 로 준비된 상태에서 시작한다. 요청을 바로 몰아넣으면
            # 첫 바퀴보다 대기가 먼저 생기고, 이 부하(N 을 내내 넘는 몰림)에서는 대기가
            # 끝까지 비지 않아 설계대로 미리 만들기가 한 번도 일어나지 않는다. 그러면
            # "미리 만든 파드를 썼나" 가 시작 타이밍에 따라 갈렸다(스레드 전환을 잦게 하면
            # 40회 중 26회 실패로 재현).
            initial = min(reserve, capacity or 0)
            self.assertTrue(
                wait_until(
                    lambda: sum(1 for p in world.live() if p["buffer_status"] == "idle" and p["ready"]) >= initial,
                    timeout=5.0,
                ),
                "시작 전 idle 이 Ready 로 준비되지 않았다",
            )
            remaining = tickets_total
            while remaining > 0:
                burst = min(remaining, random.choice([1, 1, 2, 5, 8]))
                ids += world.enqueue(burst)
                remaining -= burst
                if random.random() < 0.3:
                    victim = random.choice(ids)
                    world.cancel(victim)
                    cancelled.add(victim)
                time.sleep(random.uniform(0.0, 0.05))

            def settled():
                with world.mu:
                    return all(world.tickets[t]["status"] in ("done", "cancelled") for t in ids)

            self.assertTrue(wait_until(settled, timeout=20.0), "티켓이 끝나지 않았다")
            # 수요가 끝난 뒤 버퍼가 R 로 돌아온다(쿨다운 창이 지나면).
            converged = wait_until(
                lambda: len([p for p in world.live() if p["buffer_status"] == "idle"]) == min(reserve, capacity)
                and not any(p["buffer_status"] == "assigned" for p in world.live()),
                timeout=10.0,
            )
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=2)
        self.assertEqual(errors, [])
        self.assertEqual(world.violations, [], f"N={capacity} 을 넘었다: {world.violations[:5]}")
        self.assertTrue(converged, "버퍼가 R 로 돌아오지 않았다")
        # 파드 하나는 요청 하나에만 쓴다.
        for pod, owners in world.assignments.items():
            self.assertEqual(len(owners), 1, pod)
        done = [t for t in ids if world.tickets[t]["status"] == "done"]
        self.assertGreater(len(done), 0)
        return world, done

    def test_capacity_holds_with_reserve(self):
        world, done = self._simulate(capacity=6, reserve=2)
        self.assertGreater(world.created["idle"], 0)
        self.assertTrue(world.assignments, "미리 만든 파드를 한 번도 쓰지 않았다")
        self.assertLessEqual(world.max_live, 6)

    def test_reserve_zero_holds_capacity_and_returns_to_zero(self):
        """R=0 이어도 동적 R 은 몰림 뒤 버퍼를 둔다. 수요가 끝나면 0 으로 돌아온다."""
        self._simulate(capacity=6, reserve=0, seed=3)

    def test_tight_capacity(self):
        self._simulate(capacity=2, reserve=2, tickets_total=40, seed=11)



# ---------------------------------------------------------------------- 배포 계약


class BaselineDeployContractTests(unittest.TestCase):
    """exp-baseline 의 배포 계약: compute 파드를 쓰지 않고, R/N 이 없다.

    위의 직접 관리 테스트는 이 브랜치에도 남아 있는 공용 compute 모듈을 확인한다. 이
    arm 의 오케스트레이터는 그 모듈로 공급·할당 루프를 만들지 않는다(아래에서 확인).
    """

    REPO = ROOT.parent

    def _load_yaml(self, path):
        import yaml

        return list(yaml.safe_load_all((self.REPO / path).read_text(encoding="utf-8")))

    def test_orchestrator_starts_no_compute_supply_or_allocation(self):
        source = (ROOT / "services" / "orchestrator.py").read_text(encoding="utf-8")
        for started in (
            "DirectBufferReconciler",
            "ComputeManager",
            "ComputeAvailabilityWatcher",
            "LeaseLeaderElector",
            "queue_worker",
            "threading.Thread",
        ):
            self.assertNotIn(started, source)

    def test_controller_manifest_names_the_arm_and_has_no_r_or_n(self):
        documents = self._load_yaml("deploy/base/controller.yaml")
        controller = next(d for d in documents if d and d.get("kind") == "Deployment")
        env = {
            item["name"]: item.get("value")
            for item in controller["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertEqual(env["COMPUTE_ALLOCATION_MODE"], "baseline")
        for name in ("BUFFER_RESERVE", "BUFFER_CAPACITY", "BUFFER_DYNAMIC_RESERVE_ENABLED"):
            self.assertNotIn(name, env)

    def test_deploy_takes_no_policy_and_checks_zero_compute_pods(self):
        script = (self.REPO / "deploy/scripts/deploy.sh").read_text(encoding="utf-8")
        self.assertNotIn("BUFFER_RESERVE", script)
        self.assertNotIn("BUFFER_CAPACITY", script)
        self.assertNotIn("set_buffer_policy", script)
        self.assertFalse((self.REPO / "deploy/scripts/set_buffer_policy.sh").exists())
        self.assertIn('status.get("allocation_mode") == "baseline"', script)
        self.assertIn('status.get("R") is None', script)
        self.assertIn('status.get("N") is None', script)
        self.assertIn('status.get("physical_total") == 0', script)
        # 다른 arm 이 남긴 compute 자원은 배포가 지운다(이 arm 의 컨트롤러에는 정리 루프가 없다).
        self.assertIn("kubectl delete deployment -l app=compute-pod", script)
        self.assertIn("kubectl delete pod -l app=compute-pod", script)

    def test_jenkins_takes_no_r_or_n(self):
        jenkinsfile = self.REPO / "Jenkinsfile"
        if not jenkinsfile.exists():
            self.skipTest("테스트 이미지에는 Jenkinsfile 이 없다")
        text = jenkinsfile.read_text(encoding="utf-8")
        self.assertNotIn("BUFFER_RESERVE", text)
        self.assertNotIn("BUFFER_CAPACITY", text)
        self.assertNotIn("parameters {", text)


# ---------------------------------------------------------------- 장애 조건


class HoldAllocator(SimAllocator):
    """세션이 끝나지 않는 할당(자리를 계속 쥔다)."""

    def _execute_allocated_ticket(self, ticket):
        with self.provider.world.mu:
            self.provider.world.tickets[ticket["ticket_id"]]["status"] = "assigned"
        return {"status": "assigned"}


class DirectFaultTests(unittest.TestCase):
    def test_slow_creates_cannot_exceed_n_after_reservations_expire(self):
        """리뷰 재현: 호출당 4.9초로 순차 생성하면 뒤쪽 예약이 60초를 넘겨 만료된다.

        그 자리를 다른 복제본이 빈자리로 세어 요청 파드를 만든다. 고치기 전에는 리더가
        남은 이름을 마저 만들어 N=16 에 19 개가 됐다.
        """
        clock = {"now": 1_000_000.0}
        world = World(capacity=16, reserve=16)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.policy.cooldown_seconds = lambda compute_type: 0.0
        original_create = provider.create_idle_pod
        state = {"stolen": False}

        def slow_create(compute_type, name):
            clock["now"] += 4.9
            if clock["now"] - 1_000_000.0 > 60 and not state["stolen"]:
                state["stolen"] = True
                world.tick()
                time.sleep(0.05)
                world.tick()
                world.enqueue(16)
                for _ in range(2):
                    with run_detached_threads_inline():
                        HoldAllocator(provider, queues, tickets).drain_wait_queue_for_type(CT, no_stale)
            return original_create(compute_type, name)

        provider.create_idle_pod = slow_create
        with patch("time.time", lambda: clock["now"]):
            reconciler.reconcile_type(CT)
        self.assertTrue(state["stolen"])
        self.assertEqual(world.violations, [])
        self.assertLessEqual(len(world.live()), 16)

    def test_reservations_stay_alive_while_creates_are_slow(self):
        """다른 쪽이 끼어들지 않으면 느린 생성도 끝까지 다 만든다(꼬리 예약이 만료되지 않는다)."""
        clock = {"now": 2_000_000.0}
        world = World(capacity=16, reserve=16)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.policy.cooldown_seconds = lambda compute_type: 0.0
        original_create = provider.create_idle_pod

        def slow_create(compute_type, name):
            clock["now"] += 4.9
            # 다른 복제본이 예약을 세면서 만료된 것을 지운다.
            queues.pending_creates(CT, 60.0)
            return original_create(compute_type, name)

        provider.create_idle_pod = slow_create
        with patch("time.time", lambda: clock["now"]):
            reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 16)

    def test_plan_is_not_registered_after_the_lock_is_lost(self):
        world = World(capacity=10, reserve=3)
        provider, queues, tickets, allocator, reconciler = make(world)
        original_plan = reconciler._plan

        def plan_then_lose_lock(ct):
            plan = original_plan(ct)
            world.lock_token = "someone-else"  # 계획 중에 락이 만료돼 남이 잡았다
            return plan

        reconciler._plan = plan_then_lose_lock
        outcome = reconciler.reconcile_type(CT)
        self.assertEqual(outcome["record"].get("create_aborted"), "lock_lost")
        self.assertEqual(world.created["idle"], 0)
        self.assertEqual(world.pending, {})

    def test_request_pod_with_lost_reservation_but_fresh_claim_is_created(self):
        world = World(capacity=5, reserve=0)
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
        world = World(capacity=5, reserve=0)
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



class DirectLeadershipTests(unittest.TestCase):
    def test_creation_stops_when_leadership_is_lost_mid_batch(self):
        world = World(capacity=10, reserve=5)
        provider, queues, tickets, allocator, reconciler = make(world)
        leader = {"ok": True}
        reconciler.set_leadership_validator(lambda: leader["ok"])
        original_create = provider.create_idle_pod
        calls = {"n": 0}

        def create_then_lose(compute_type, name):
            calls["n"] += 1
            if calls["n"] == 2:
                leader["ok"] = False  # 두 번째 생성 도중 리더를 잃었다
            return original_create(compute_type, name)

        provider.create_idle_pod = create_then_lose
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 2)
        # 만들지 않은 세 이름의 예약은 남기지 않는다.
        with world.mu:
            unlisted = [name for name in world.pending if name not in world.pods]
        self.assertEqual(unlisted, [])

    def test_creation_stops_on_stop_signal(self):
        world = World(capacity=10, reserve=4)
        provider, queues, tickets, allocator, reconciler = make(world)
        original_create = provider.create_idle_pod

        def create_then_stop(compute_type, name):
            reconciler._stop_event.set()
            return original_create(compute_type, name)

        provider.create_idle_pod = create_then_stop
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 1)

    def test_stale_generation_does_not_delete(self):
        world = World(capacity=10, reserve=0)
        for index in range(3):
            world.add_pod(f"warm-{index}", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler._generation = 5
        reconciler.reconcile_type(CT, generation=4)  # 이전 세대의 바퀴
        self.assertFalse(any(p["terminating"] for p in world.pods.values()))



class DirectTemplateGenerationTests(unittest.TestCase):
    def _live(self, world):
        return sorted((p["name"], p["template_hash"]) for p in world.live())

    def test_outdated_idle_is_not_handed_out(self):
        world = World(capacity=5, reserve=1)
        world.add_pod("warm-old", "idle", {}, ready=True, template_hash="gen-0")
        provider, queues, tickets, allocator, _ = make(world)
        world.enqueue(1)
        with run_detached_threads_inline():
            allocator.drain_wait_queue_for_type(CT, no_stale)
        self.assertNotIn("warm-old", world.assignments)
        self.assertEqual(world.created["request"], 1)

    def test_replacements_are_ready_before_outdated_idle_is_deleted(self):
        world = World(capacity=10, reserve=2)
        world.add_pod("warm-old-a", "idle", {}, ready=True, template_hash="gen-0")
        world.add_pod("warm-old-b", "idle", {}, ready=True, template_hash="gen-0")
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 2)
        self.assertFalse(world.pods["warm-old-a"]["terminating"])  # 교체본이 아직 부팅 중
        self.assertTrue(wait_until(lambda: all(p["ready"] for p in world.live()), world=world))
        reconciler.reconcile_type(CT)
        self.assertTrue(world.pods["warm-old-a"]["terminating"])
        self.assertTrue(world.pods["warm-old-b"]["terminating"])
        self.assertEqual(world.violations, [])

    def test_full_capacity_replaces_one_at_a_time_without_exceeding_n(self):
        world = World(capacity=2, reserve=2)
        world.add_pod("warm-old-a", "idle", {}, ready=True, template_hash="gen-0", age_seconds=2)
        world.add_pod("warm-old-b", "idle", {}, ready=True, template_hash="gen-0", age_seconds=1)
        provider, queues, tickets, allocator, reconciler = make(world)
        for _ in range(12):
            reconciler.reconcile_type(CT)
            wait_until(lambda: False, timeout=0.05, world=world)
            self.assertLessEqual(len(world.live()), 2)
        self.assertEqual([h for _, h in self._live(world)], ["gen-1", "gen-1"])
        self.assertEqual(world.violations, [])

    def test_outdated_idle_is_dropped_while_requests_wait(self):
        world = World(capacity=10, reserve=2)
        world.add_pod("warm-old", "idle", {}, ready=True, template_hash="gen-0")
        provider, queues, tickets, allocator, reconciler = make(world)
        world.enqueue(1)
        reconciler.reconcile_type(CT)
        self.assertTrue(world.pods["warm-old"]["terminating"])

    def test_current_generation_buffer_is_left_alone(self):
        world = World(capacity=10, reserve=2)
        world.add_pod("warm-a", "idle", {}, ready=True)
        world.add_pod("warm-b", "idle", {}, ready=True)
        provider, queues, tickets, allocator, reconciler = make(world)
        reconciler.reconcile_type(CT)
        self.assertEqual(world.created["idle"], 0)
        self.assertFalse(any(p["terminating"] for p in world.pods.values()))


if __name__ == "__main__":
    unittest.main()
