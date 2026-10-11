from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from config import settings

# Queue repair and claim both walk Redis-backed ticket indexes. Keep their
# timing summaries sparse so a saturated queue can be diagnosed without a log
# line (or a ticket identifier) for every claim.
QUEUE_TIMING_WINDOW_SECONDS = 30.0
QUEUE_TIMING_SLOW_MS = 100.0
QUEUE_TIMING_SLOW_WINDOW_MS = 500.0

try:
    import redis
    from redis.exceptions import RedisError
except Exception:  # pragma: no cover - import fallback for local analysis
    redis = None

    class RedisError(Exception):
        pass

from .tickets import (
    QueueUnavailableError,
    Tickets,
    WAIT_ABANDONED_REASON,
    _iso_now,
    _utc_now,
    parse_datetime,
    safe_int,
)

try:
    from .. import timing
except ImportError:  # 단위 테스트는 이 파일 하나만 따로 읽어 패키지가 없다
    from contextlib import nullcontext as _timing_nullcontext

    class timing:  # noqa: N801 - 모듈 대역. 계측이 꺼진 것과 같게 동작한다.
        ENABLED = False

        @staticmethod
        def timed(kind):
            return lambda fn: fn

        @staticmethod
        def emit(*args, **kwargs):
            pass

        @staticmethod
        def phase(*args, **kwargs):
            pass

        @staticmethod
        def note(*args, **kwargs):
            pass

        @staticmethod
        def current_purpose():
            return "other"

        @staticmethod
        def current_trigger():
            return "direct"

        @staticmethod
        def lock_acquired(*args, **kwargs):
            pass

        @staticmethod
        def lock_missed(*args, **kwargs):
            pass

        @staticmethod
        def lock_released(*args, **kwargs):
            pass

        @staticmethod
        def set_leader_probe(*args, **kwargs):
            pass

        @staticmethod
        def start(*args, **kwargs):
            pass

        @staticmethod
        def scope(*args, **kwargs):
            return _timing_nullcontext()

        @staticmethod
        def phase_timer():
            return _timing_nullcontext()


logger = logging.getLogger(__name__)


class ComputeQueues:
    """Redis-backed compute-type queues and ticket facade."""

    FINAL_STATES = Tickets.FINAL_STATES
    ACTIVE_STATES = Tickets.ACTIVE_STATES
    TRANSIENT_STATES = Tickets.TRANSIENT_STATES

    def __init__(
        self,
        redis_url: Optional[str] = None,
        prefix: Optional[str] = None,
        default_compute_type: Optional[str] = None,
        lock_ttl_seconds: Optional[int] = None,
        wait_timeout_seconds: Optional[int] = None,
        ticket_ttl_seconds: Optional[int] = None,
        allocating_ttl_seconds: Optional[int] = None,
        assigned_context_ttl_seconds: Optional[int] = None,
        max_retries: Optional[int] = None,
        worker_identity: Optional[str] = None,
    ):
        self.redis_url = redis_url or settings.REDIS_URL
        self.prefix = prefix or settings.WAIT_QUEUE_PREFIX
        self.default_compute_type = (
            (default_compute_type or settings.DEFAULT_COMPUTE_TYPE).strip().lower() or "general"
        )
        self.lock_ttl_seconds = lock_ttl_seconds or settings.WAIT_QUEUE_LOCK_TTL_SECONDS
        self.wait_timeout_seconds = wait_timeout_seconds or settings.WAIT_QUEUE_TIMEOUT_SECONDS
        self.ticket_ttl_seconds = ticket_ttl_seconds or settings.WAIT_QUEUE_TICKET_TTL_SECONDS
        self.allocating_ttl_seconds = allocating_ttl_seconds or settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS
        self.client_timeout_seconds = settings.WAIT_QUEUE_CLIENT_TIMEOUT_SECONDS
        self.assigned_context_ttl_seconds = (
            assigned_context_ttl_seconds or settings.ASSIGNED_CONTEXT_TTL_SECONDS
        )
        self.max_retries = max_retries or settings.WAIT_QUEUE_MAX_RETRIES
        self.worker_identity = worker_identity or os.getenv("HOSTNAME", "controller-unknown")
        self._client = None
        self._client_lock = threading.Lock()
        self._queue_timing_lock = threading.Lock()
        self._queue_timing = {}
        self.tickets = Tickets(self)

    def _record_queue_timing(
        self,
        compute_type: str,
        operation: str,
        elapsed_ms: float,
        *,
        examined: int = 0,
        repair_ms: float = 0.0,
        scan_ms: float = 0.0,
        outcome: str = "ok",
    ) -> None:
        """Emit one identifier-free summary per operation and time window."""
        now = time.monotonic()
        with self._queue_timing_lock:
            key = (compute_type, operation)
            state = self._queue_timing.get(key)
            if state is None:
                state = {
                    "started": now,
                    "count": 0,
                    "total_ms": 0.0,
                    "max_ms": 0.0,
                    "examined": 0,
                    "repair_ms": 0.0,
                    "scan_ms": 0.0,
                    "claimed": 0,
                    "empty": 0,
                    "errors": 0,
                }
                self._queue_timing[key] = state
            state["count"] += 1
            state["total_ms"] += elapsed_ms
            state["max_ms"] = max(state["max_ms"], elapsed_ms)
            state["examined"] += examined
            state["repair_ms"] += repair_ms
            state["scan_ms"] += scan_ms
            state["claimed"] += int(outcome == "claimed")
            state["empty"] += int(outcome == "empty")
            state["errors"] += int(outcome == "error")
            if now - state["started"] < QUEUE_TIMING_WINDOW_SECONDS:
                return
            summary = state.copy()
            del self._queue_timing[key]

        level = (
            logging.WARNING
            if summary["max_ms"] >= QUEUE_TIMING_SLOW_MS
            or summary["total_ms"] >= QUEUE_TIMING_SLOW_WINDOW_MS
            or summary["errors"]
            else logging.DEBUG
        )
        logger.log(
            level,
            "[QueueTiming] compute_type=%s operation=%s window_s=%.1f "
            "calls=%s total_ms=%.1f "
            "avg_ms=%.1f max_ms=%.1f examined=%s repair_ms=%.1f "
            "scan_ms=%.1f claimed=%s empty=%s errors=%s",
            compute_type,
            operation,
            now - summary["started"],
            summary["count"],
            summary["total_ms"],
            summary["total_ms"] / summary["count"],
            summary["max_ms"],
            summary["examined"],
            summary["repair_ms"],
            summary["scan_ms"],
            summary["claimed"],
            summary["empty"],
            summary["errors"],
        )

    def normalize_compute_type(self, compute_type: Optional[str]) -> str:
        value = (compute_type or self.default_compute_type).strip().lower()
        return value or self.default_compute_type

    def validate_compute_type(
        self,
        compute_type: Optional[str],
        known_compute_types: Optional[Iterable[str]] = None,
    ) -> str:
        value = self.normalize_compute_type(compute_type)
        if known_compute_types is None:
            known_compute_types = self.known_compute_types()

        normalized_known = []
        for item in known_compute_types:
            normalized = self.normalize_compute_type(item)
            if normalized not in normalized_known:
                normalized_known.append(normalized)

        if not normalized_known:
            normalized_known = [self.default_compute_type]

        if value not in normalized_known:
            raise ValueError(
                f"Unknown compute_type '{value}'. Known compute types: {', '.join(sorted(normalized_known))}"
            )
        return value

    def _redis_client(self):
        if redis is None:
            raise QueueUnavailableError("redis package is not available")

        with self._client_lock:
            if self._client is None:
                self._client = redis.Redis.from_url(
                    self.redis_url,
                    decode_responses=True,
                    health_check_interval=30,
                    socket_connect_timeout=2,
                    socket_timeout=3,
                )
            return self._client

    def _queue_key(self, compute_type: str) -> str:
        return f"{self.prefix}:queue:{compute_type}"

    def _ticket_key(self, ticket_id: str) -> str:
        return f"{self.prefix}:ticket:{ticket_id}"

    def _active_key(self, compute_type: str) -> str:
        return f"{self.prefix}:active:{compute_type}"

    def _types_key(self) -> str:
        return f"{self.prefix}:types"

    def _lock_key(self, compute_type: str) -> str:
        return f"{self.prefix}:lock:{compute_type}"

    def _assigned_request_key(self, compute_pod: str) -> str:
        return f"{self.prefix}:assigned-request:{compute_pod}"

    def _compute_ticket_key(self, compute_pod: str) -> str:
        return f"{self.prefix}:compute-ticket:{compute_pod}"

    def register_compute_types(self, compute_types: Iterable[str]) -> List[str]:
        normalized = []
        for compute_type in compute_types:
            value = self.normalize_compute_type(compute_type)
            if value not in normalized:
                normalized.append(value)

        if not normalized:
            return []

        client = self._redis_client()
        try:
            client.sadd(self._types_key(), *normalized)
            return normalized
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to register compute types: {exc}") from exc

    def known_compute_types(self) -> List[str]:
        client = self._redis_client()
        try:
            values = client.smembers(self._types_key())
            if not values:
                return [self.default_compute_type]
            return sorted(self.normalize_compute_type(value) for value in values if value)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read compute types: {exc}") from exc

    def has_queued_tickets(self, compute_type: str) -> bool:
        compute_type_value = self.normalize_compute_type(compute_type)
        self._repair_queue_membership(compute_type_value)
        for ticket_id in self._queue_ids(compute_type_value):
            ticket = self.tickets.get_ticket_raw(ticket_id)
            if not ticket:
                continue
            if str(ticket.get("status") or "").lower() == "queued":
                return True
        return False

    def queued_count(self, compute_type: str) -> int:
        """How many tickets are waiting for a Compute Pod right now.

        The same test has_queued_tickets applies, counting instead of stopping at
        the first hit. It deliberately skips _repair_queue_membership: that is the
        write half of the pair, the allocator's drain already repairs on every
        pass, and a stale id is dropped by the status test here anyway. This runs
        on every reconcile, so the repair would double the write load for nothing.
        """
        compute_type_value = self.normalize_compute_type(compute_type)
        ticket_ids = self._queue_ids(compute_type_value)
        if not ticket_ids:
            return 0

        # Only the status field is needed. Pipeline the reads so a large queue
        # costs one Redis round trip instead of one round trip per ticket.
        client = self._redis_client()
        try:
            with client.pipeline(transaction=False) as pipe:
                for ticket_id in ticket_ids:
                    pipe.hget(self._ticket_key(ticket_id), "status")
                statuses = pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read queued ticket statuses for {compute_type_value}: {exc}"
            ) from exc
        return sum(str(status or "").lower() == "queued" for status in statuses)

    def compute_types_with_queued_tickets(self) -> List[str]:
        queued_types = []
        for compute_type in self.known_compute_types():
            compute_type_value = self.normalize_compute_type(compute_type)
            if self.has_queued_tickets(compute_type_value):
                queued_types.append(compute_type_value)
        return sorted(set(queued_types))

    def get_ticket_position(self, ticket_id: str) -> Optional[int]:
        """1-based place in the arrival-ordered queue, or None when absent."""
        ticket = self.tickets.get_ticket_raw(ticket_id)
        if not ticket:
            return None
        compute_type = self.normalize_compute_type(ticket.get("compute_type"))
        client = self._redis_client()
        try:
            rank = client.zrank(self._queue_key(compute_type), ticket_id)
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read queue position for ticket {ticket_id}: {exc}"
            ) from exc
        return None if rank is None else int(rank) + 1

    def queue_score(self, raw: Optional[Dict[str, object]]) -> float:
        """Queue ordering key: arrival time, so a requeued ticket keeps its place.

        A missing timestamp falls back to now instead of 0, which would sort
        ahead of everyone forever.
        """
        request_at_ms = safe_int((raw or {}).get("request_at_ms"), 0)
        if request_at_ms <= 0:
            request_at_ms = int(time.time() * 1000)
        return float(request_at_ms)

    def _queue_ids(self, compute_type: str) -> List[str]:
        client = self._redis_client()
        try:
            return client.zrange(self._queue_key(compute_type), 0, -1)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read queue {compute_type}: {exc}") from exc

    def _active_ticket_ids(self, compute_type: str) -> List[str]:
        client = self._redis_client()
        try:
            return sorted(client.smembers(self._active_key(compute_type)))
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read active tickets for {compute_type}: {exc}") from exc

    def _queue_contains(self, client, compute_type: str, ticket_id: str) -> bool:
        try:
            return client.zscore(self._queue_key(compute_type), ticket_id) is not None
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to inspect queue {compute_type} for ticket {ticket_id}: {exc}"
            ) from exc

    def _repair_queue_membership(self, compute_type: str) -> None:
        client = self._redis_client()
        queue_key = self._queue_key(compute_type)
        active_key = self._active_key(compute_type)
        started = time.perf_counter()
        active_count = 0
        outcome = "error"
        try:
            active_ids = list(client.smembers(active_key))
            active_count = len(active_ids)
            for ticket_id in active_ids:
                raw = client.hgetall(self._ticket_key(ticket_id))
                if not raw:
                    client.srem(active_key, ticket_id)
                    client.zrem(queue_key, ticket_id)
                    continue

                status = (raw.get("status") or "").lower()
                if status in self.FINAL_STATES:
                    client.srem(active_key, ticket_id)
                    client.zrem(queue_key, ticket_id)
                    continue

                if status in self.ACTIVE_STATES:
                    if not self._queue_contains(client, compute_type, ticket_id):
                        client.zadd(queue_key, {ticket_id: self.queue_score(raw)})
                    client.sadd(active_key, ticket_id)
                else:
                    client.srem(active_key, ticket_id)
                    client.zrem(queue_key, ticket_id)
            outcome = "ok"
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to repair queue state for {compute_type}: {exc}") from exc
        finally:
            self._record_queue_timing(
                compute_type,
                "repair",
                (time.perf_counter() - started) * 1000.0,
                examined=active_count,
                outcome=outcome,
            )

    def list_waiting_users(
        self,
        compute_type: str,
        *,
        now_ms: Optional[int] = None,
    ) -> Dict[str, object]:
        compute_type_value = self.normalize_compute_type(compute_type)
        now_ms_value = now_ms if now_ms is not None else int(_utc_now().timestamp() * 1000)
        waiting_users: List[Dict[str, object]] = []
        queue_position = 0

        for ticket_id in self._queue_ids(compute_type_value):
            raw = self.tickets.get_ticket_raw(ticket_id)
            if not raw:
                continue
            if str(raw.get("status") or "").lower() != "queued":
                continue

            queue_position += 1
            request_at_ms = safe_int(raw.get("request_at_ms"), 0)
            created_ms = request_at_ms
            if not created_ms:
                created_at = parse_datetime(raw.get("created_at"))
                if created_at:
                    created_ms = int(created_at.timestamp() * 1000)

            wait_ms = max(0, now_ms_value - created_ms) if created_ms else None
            waiting_users.append(
                {
                    "user_pod": (raw.get("user_pod") or "").strip(),
                    "ticket_id": ticket_id,
                    "queue_position": queue_position,
                    "wait_ms": wait_ms,
                }
            )

        return {
            "compute_type": compute_type_value,
            "queued_count": len(waiting_users),
            "waiting_users": waiting_users,
        }

    # ---- 토큰 락 공통 (SET NX + 토큰, 토큰 비교 후 expire/del)
    #
    # 락은 할당 락(lock:{type}) 하나다. 두 복제본의 할당(N 여유 계산 + claim + idle
    # 파드 할당)과 리더의 버퍼 계획(만들 이름 예약, 지울 파드 결정)이 이 락 아래에서
    # 같은 목록을 보고 결정한다. 파드 생성·삭제 호출 자체는 락 밖에서 한다.

    def _acquire_token_lock(
        self, key: str, ttl: int, owner: Optional[str], lock: str, label: str
    ) -> Optional[str]:
        client = self._redis_client()
        token = owner or uuid.uuid4().hex
        try:
            acquired = client.set(key, token, nx=True, ex=ttl)
            if acquired:
                timing.lock_acquired(token, lock=lock)
            else:
                timing.lock_missed(lock=lock)
            return token if acquired else None
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to acquire lock for {label}: {exc}") from exc

    def _renew_token_lock(self, key: str, ttl: int, token: Optional[str], label: str) -> bool:
        if not token:
            return False
        client = self._redis_client()
        renew_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        end
        return 0
        """
        try:
            return bool(client.eval(renew_script, 1, key, token, ttl))
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to renew lock for {label}: {exc}") from exc

    def _release_token_lock(self, key: str, token: Optional[str], label: str) -> bool:
        if not token:
            return False
        timing.lock_released(token)
        client = self._redis_client()
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        try:
            return bool(client.eval(release_script, 1, key, token))
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to release lock for {label}: {exc}") from exc

    def acquire_allocator_lock(self, compute_type: str, owner: Optional[str] = None) -> Optional[str]:
        compute_type_value = self.normalize_compute_type(compute_type)
        return self._acquire_token_lock(
            self._lock_key(compute_type_value),
            self.lock_ttl_seconds,
            owner,
            "alloc",
            compute_type_value,
        )

    def renew_allocator_lock(self, compute_type: str, token: Optional[str]) -> bool:
        compute_type_value = self.normalize_compute_type(compute_type)
        return self._renew_token_lock(
            self._lock_key(compute_type_value),
            self.lock_ttl_seconds,
            token,
            compute_type_value,
        )

    def release_allocator_lock(self, compute_type: str, token: Optional[str]) -> bool:
        compute_type_value = self.normalize_compute_type(compute_type)
        return self._release_token_lock(
            self._lock_key(compute_type_value), token, compute_type_value
        )

    def is_allocation_stale(self, ticket: Dict[str, object]) -> bool:
        allocation_deadline = ticket.get("allocation_deadline")
        if isinstance(allocation_deadline, datetime):
            return allocation_deadline <= _utc_now()
        return False

    def is_wait_timeout_expired(self, ticket: Dict[str, object]) -> bool:
        """True when the waiting client has been silent for the whole wait timeout.

        The timeout counts from the last poll, not from enqueue, so a client
        that keeps polling waits as long as the queue takes. It only reclaims
        tickets whose client went away without cancelling.
        """
        timeout_seconds = self.wait_timeout_seconds
        if timeout_seconds <= 0:
            return False
        silent_seconds = self._client_silent_seconds(ticket)
        return silent_seconds is not None and silent_seconds > timeout_seconds

    def is_client_disconnected(self, ticket: Dict[str, object]) -> bool:
        """True when a queued client has stopped polling for its ticket."""
        timeout_seconds = self.client_timeout_seconds
        if timeout_seconds <= 0:
            return False
        silent_seconds = self._client_silent_seconds(ticket)
        return silent_seconds is not None and silent_seconds > timeout_seconds

    @staticmethod
    def _client_silent_seconds(ticket: Dict[str, object]) -> Optional[float]:
        """Seconds since the client last polled.

        Clients poll about once a second while waiting; tickets predating the
        field fall back to their creation time.
        """
        last_poll_ms = safe_int(ticket.get("last_poll_ms"), 0)
        if last_poll_ms > 0:
            return (time.time() * 1000 - last_poll_ms) / 1000.0
        created_at = ticket.get("created_at")
        if not isinstance(created_at, datetime):
            return None
        return (_utc_now() - created_at).total_seconds()

    def find_stale_allocating_tickets(self, compute_type: Optional[str] = None) -> List[Dict[str, object]]:
        types = [self.normalize_compute_type(compute_type)] if compute_type else self.known_compute_types()
        stale: List[Dict[str, object]] = []
        for type_name in types:
            for ticket_id in self._active_ticket_ids(type_name):
                ticket = self.tickets.get_ticket(ticket_id)
                if not ticket:
                    continue
                if ticket.get("status") == "allocating" and self.is_allocation_stale(ticket):
                    stale.append(ticket)
        stale.sort(key=lambda item: item.get("created_at") or _utc_now())
        return stale

    # ---- 버퍼 파드 직접 관리: 자리 예약(파드 없는 claim, 생성 중 목록)

    def allocating_missing_pods(
        self,
        active_pod_names,
        compute_type: Optional[str] = None,
    ) -> List[str]:
        """파드가 아직 목록에 안 보여서 자리를 대신 쥐고 있는 claim 들의 파드 이름.

        이름을 아직 안 적은 claim 은 "" 다. 호출자가 생성 중 목록과 이름으로 겹침을
        빼고 센다 - 요청 파드는 만들기 전에 이름을 티켓과 생성 중 목록 양쪽에 올리므로,
        그대로 더하면 같은 자리를 두 번 센다.

        요청 때 만드는 파드는 claim 뒤 할당 락 밖에서 만들어진다. 그래서 "이 claim 의
        파드가 방금 읽은 목록에 있나" 로 판정한다 - 티켓에 이름이 적혔는지로 보면
        생성 응답과 LIST 가 따라잡는 사이의 틈에서 자리가 사라진다. 파드가 목록에 있으면
        세지 않는다(두 번 세면 용량이 반으로 준다). 만료된 claim 도 세지 않는다 - 생성
        전에 죽은 작업자의 티켓은 정리가 큐로 돌려보낸다. 그런 claim 이 그래도 파드를
        만들 수 있는 경우는 생성 중 목록이 막는다(할당 쪽 _reserve_request_pod).

        할당 락 아래에서 매 패스 부르므로 티켓은 한 번의 왕복으로 읽는다.
        """
        known = set(active_pod_names or ())
        types = [self.normalize_compute_type(compute_type)] if compute_type else self.known_compute_types()
        ticket_ids = [ticket_id for type_name in types for ticket_id in self._active_ticket_ids(type_name)]
        if not ticket_ids:
            return []
        client = self._redis_client()
        try:
            with client.pipeline(transaction=False) as pipe:
                for ticket_id in ticket_ids:
                    pipe.hgetall(self._ticket_key(ticket_id))
                raws = pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read allocating tickets: {exc}") from exc
        missing: List[str] = []
        for ticket_id, raw in zip(ticket_ids, raws):
            # 상태부터 거른다. _ticket_to_dict 는 티켓마다 큐 위치(ZRANK)를 다시 읽어,
            # 대기 티켓이 많은 포화에서 그 수만큼 왕복이 는다.
            if not raw or str(raw.get("status") or "").lower() != "allocating":
                continue
            ticket = self.tickets._raw_to_ticket_dict(ticket_id, raw)
            compute_pod = str(ticket.get("compute_pod") or "")
            if compute_pod and compute_pod in known:
                continue
            if self.is_allocation_stale(ticket):
                continue
            missing.append(compute_pod)
        return missing

    # 생성 중 예약은 종류별로 따로 둔다(arm 사이에 같은 키 형식). 이 arm 은 request 만
    # 올린다. N 계산은 두 종류를 합쳐 센다 - 다른 arm 이 남긴 idle 예약도 만료 전까지는
    # 자리를 쥔 것으로 본다.
    PENDING_KINDS = ("idle", "request")

    def _pending_create_key(self, compute_type: str, kind: str) -> str:
        if kind not in self.PENDING_KINDS:
            raise ValueError(f"unknown pending create kind: {kind}")
        return f"{self.prefix}:direct-creating:{kind}:{compute_type}"

    def add_pending_create(self, compute_type: str, pod_name: str, kind: str) -> None:
        """만들기로 한 파드 이름을 생성 요청 전에 적는다. N 계산에 들어간다."""
        client = self._redis_client()
        try:
            client.zadd(
                self._pending_create_key(self.normalize_compute_type(compute_type), kind),
                {pod_name: time.time()},
            )
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to record pending create {pod_name}: {exc}") from exc

    def refresh_pending_creates(self, compute_type: str, pod_names: Iterable[str], kind: str) -> set:
        """아직 남아 있는 생성 예약만 시각을 갱신하고, 남아 있던 이름을 돌려준다.

        예약을 세는 쪽(pending_creates)은 항상 만료된 것을 먼저 지우고 센다. 그래서
        "누군가 빈자리로 셌다" 는 곧 "지워졌다" 이고, 지워진 예약은 되살리지 않는다
        (XX: 있을 때만 갱신). 있는지는 ZADD 의 반환값이 아니라 ZSCORE 로 본다 - 갱신
        시각이 이전과 같으면 ZADD XX CH 도 0 을 돌려준다. 한 트랜잭션, 한 번의 왕복이다.
        """
        names = [name for name in pod_names if name]
        if not names:
            return set()
        key = self._pending_create_key(self.normalize_compute_type(compute_type), kind)
        now = time.time()
        client = self._redis_client()
        try:
            with client.pipeline(transaction=True) as pipe:
                for name in names:
                    pipe.zadd(key, {name: now}, xx=True)
                    pipe.zscore(key, name)
                results = pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to refresh pending creates: {exc}") from exc
        return {name for name, score in zip(names, results[1::2]) if score is not None}

    def remove_pending_creates(self, compute_type: str, pod_names: Iterable[str]) -> None:
        names = [name for name in pod_names if name]
        if not names:
            return
        compute_type_value = self.normalize_compute_type(compute_type)
        client = self._redis_client()
        try:
            # 이름은 종류와 관계없이 하나뿐이므로 두 목록에서 모두 지운다.
            with client.pipeline(transaction=False) as pipe:
                for kind in self.PENDING_KINDS:
                    pipe.zrem(self._pending_create_key(compute_type_value, kind), *names)
                pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to clear pending creates: {exc}") from exc

    def pending_creates(self, compute_type: str, max_age_seconds: float) -> Dict[str, str]:
        """아직 목록에 안 보였을 수 있는 생성 요청 {이름: 종류}. 상한보다 오래된 것은 버린다.

        응답 없는 생성 요청이 서버에 남아 있을 수 있는 시간의 상한(apiserver 요청
        timeout)이 지나면 그 요청은 끝났다. 그때까지 목록에 없으면 만들어지지 않았다.
        """
        compute_type_value = self.normalize_compute_type(compute_type)
        cutoff = time.time() - max(0.0, float(max_age_seconds))
        client = self._redis_client()
        try:
            # 만료된 것을 먼저 지우고 센다. 그래서 "빈자리로 셌다" 는 곧 "지워졌다" 이다
            # (refresh_pending_creates 가 지워진 예약을 되살리지 않는 근거).
            with client.pipeline(transaction=True) as pipe:
                for kind in self.PENDING_KINDS:
                    key = self._pending_create_key(compute_type_value, kind)
                    pipe.zremrangebyscore(key, "-inf", cutoff)
                    pipe.zrange(key, 0, -1)
                results = pipe.execute()
            pending: Dict[str, str] = {}
            for kind, names in zip(self.PENDING_KINDS, results[1::2]):
                for name in names:
                    pending[name] = kind
            return pending
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read pending creates: {exc}") from exc

    def claim_next_ticket(self, compute_type: str, worker_id: Optional[str] = None) -> Optional[Dict[str, object]]:
        started = time.perf_counter()
        compute_type_value = self.normalize_compute_type(compute_type)
        client = self._redis_client()
        worker_name = worker_id or self.worker_identity
        repair_started = time.perf_counter()
        self._repair_queue_membership(compute_type_value)
        repair_ms = (time.perf_counter() - repair_started) * 1000.0

        scan_started = time.perf_counter()
        examined = 0
        for ticket_id in self._queue_ids(compute_type_value):
            examined += 1
            ticket = self.tickets.get_ticket(ticket_id)
            if not ticket:
                self._remove_ticket_from_queue(compute_type_value, ticket_id)
                client.srem(self._active_key(compute_type_value), ticket_id)
                continue

            status = ticket.get("status")
            if status in self.FINAL_STATES:
                self._remove_ticket_from_queue(compute_type_value, ticket_id)
                client.srem(self._active_key(compute_type_value), ticket_id)
                continue
            if status != "queued":
                continue
            if self.is_wait_timeout_expired(ticket):
                self.tickets.mark_failed(ticket_id, WAIT_ABANDONED_REASON)
                continue
            if self.is_client_disconnected(ticket):
                # Left in the queue on purpose: a client that reconnects keeps
                # its place, and one that stays silent for the wait timeout is
                # failed above.
                logger.debug(
                    "[QueueSkipped] ticket_id=%s reason=%r",
                    ticket_id,
                    "client stopped polling",
                )
                continue

            claim_token = uuid.uuid4().hex
            claimed_at = _iso_now()
            allocation_deadline = (_utc_now() + timedelta(seconds=self.allocating_ttl_seconds)).isoformat()
            ticket = self.tickets._ticket_transition(
                ticket_id,
                compute_type=compute_type_value,
                expected_statuses={"queued"},
                updates={
                    "status": "allocating",
                    "claimed_by": worker_name,
                    "claim_token": claim_token,
                    "claimed_at": claimed_at,
                    "allocation_deadline": allocation_deadline,
                    "error": "",
                },
            )
            if not ticket:
                continue
            ticket["claim_token"] = claim_token
            ticket["claimed_by"] = worker_name
            finished = time.perf_counter()
            self._record_queue_timing(
                compute_type_value,
                "claim",
                (finished - started) * 1000.0,
                examined=examined,
                repair_ms=repair_ms,
                scan_ms=(finished - scan_started) * 1000.0,
                outcome="claimed",
            )
            return ticket

        finished = time.perf_counter()
        self._record_queue_timing(
            compute_type_value,
            "claim",
            (finished - started) * 1000.0,
            examined=examined,
            repair_ms=repair_ms,
            scan_ms=(finished - scan_started) * 1000.0,
            outcome="empty",
        )
        return None

    def _remove_ticket_from_queue(self, compute_type: str, ticket_id: str) -> None:
        client = self._redis_client()
        try:
            client.zrem(self._queue_key(compute_type), ticket_id)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to remove ticket {ticket_id} from queue {compute_type}: {exc}") from exc


# 계측 전용. KDA_TIMING_LOG 가 꺼져 있으면 timed() 가 원본을 그대로 돌려준다.
# 리더의 큐 정리(repair)가 claim 시간의 대부분을 차지한다는 의심이 있어, 락 안에서
# 도는 Redis 작업을 종류별로 30초마다 집계한다.
for _name, _kind in (
    ("_repair_queue_membership", "redis.repair"),
    ("claim_next_ticket", "redis.claim"),
    ("find_stale_allocating_tickets", "redis.stale_scan"),
    ("has_queued_tickets", "redis.has_queued"),
    ("queued_count", "redis.queued_count"),
):
    setattr(ComputeQueues, _name, timing.timed(_kind)(getattr(ComputeQueues, _name)))
