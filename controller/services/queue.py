from __future__ import annotations

import logging
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from config import settings

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
    _iso_now,
    _parse_datetime,
    _safe_int,
    _utc_now,
    parse_datetime,
    safe_int,
)

logger = logging.getLogger(__name__)


class BackendQueues:
    """Redis-backed backend-type queues and ticket facade."""

    FINAL_STATES = Tickets.FINAL_STATES
    ACTIVE_STATES = Tickets.ACTIVE_STATES
    TRANSIENT_STATES = Tickets.TRANSIENT_STATES

    def __init__(
        self,
        redis_url: Optional[str] = None,
        prefix: Optional[str] = None,
        default_backend_type: Optional[str] = None,
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
        self.default_backend_type = (
            (default_backend_type or settings.DEFAULT_BACKEND_TYPE).strip().lower() or "general"
        )
        self.lock_ttl_seconds = lock_ttl_seconds or settings.WAIT_QUEUE_LOCK_TTL_SECONDS
        self.wait_timeout_seconds = wait_timeout_seconds or settings.WAIT_QUEUE_TIMEOUT_SECONDS
        self.ticket_ttl_seconds = ticket_ttl_seconds or settings.WAIT_QUEUE_TICKET_TTL_SECONDS
        self.allocating_ttl_seconds = allocating_ttl_seconds or settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS
        self.assigned_context_ttl_seconds = (
            assigned_context_ttl_seconds or settings.ASSIGNED_CONTEXT_TTL_SECONDS
        )
        self.max_retries = max_retries or settings.WAIT_QUEUE_MAX_RETRIES
        self.worker_identity = worker_identity or os.getenv("HOSTNAME", "controller-unknown")
        self._client = None
        self._client_lock = threading.Lock()
        self.tickets = Tickets(self)

    def normalize_backend_type(self, backend_type: Optional[str]) -> str:
        value = (backend_type or self.default_backend_type).strip().lower()
        return value or self.default_backend_type

    def validate_backend_type(
        self,
        backend_type: Optional[str],
        known_backend_types: Optional[Iterable[str]] = None,
    ) -> str:
        value = self.normalize_backend_type(backend_type)
        if known_backend_types is None:
            known_backend_types = self.known_backend_types()

        normalized_known = []
        for item in known_backend_types:
            normalized = self.normalize_backend_type(item)
            if normalized not in normalized_known:
                normalized_known.append(normalized)

        if not normalized_known:
            normalized_known = [self.default_backend_type]

        if value not in normalized_known:
            raise ValueError(
                f"Unknown backend_type '{value}'. Known backend types: {', '.join(sorted(normalized_known))}"
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

    def _ping(self) -> None:
        client = self._redis_client()
        try:
            client.ping()
        except RedisError as exc:
            raise QueueUnavailableError(f"Redis unavailable: {exc}") from exc

    def _queue_key(self, backend_type: str) -> str:
        return f"{self.prefix}:queue:{backend_type}"

    def _ticket_key(self, ticket_id: str) -> str:
        return f"{self.prefix}:ticket:{ticket_id}"

    def _active_key(self, backend_type: str) -> str:
        return f"{self.prefix}:active:{backend_type}"

    def _types_key(self) -> str:
        return f"{self.prefix}:types"

    def _lock_key(self, backend_type: str) -> str:
        return f"{self.prefix}:lock:{backend_type}"

    def _assigned_request_key(self, backend_pod: str) -> str:
        return f"{self.prefix}:assigned-request:{backend_pod}"

    def _backend_ticket_key(self, backend_pod: str) -> str:
        return f"{self.prefix}:backend-ticket:{backend_pod}"

    def register_backend_types(self, backend_types: Iterable[str]) -> List[str]:
        normalized = []
        for backend_type in backend_types:
            value = self.normalize_backend_type(backend_type)
            if value not in normalized:
                normalized.append(value)

        if not normalized:
            return []

        client = self._redis_client()
        try:
            client.sadd(self._types_key(), *normalized)
            return normalized
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to register backend types: {exc}") from exc

    def known_backend_types(self) -> List[str]:
        client = self._redis_client()
        try:
            values = client.smembers(self._types_key())
            if not values:
                return [self.default_backend_type]
            return sorted(self.normalize_backend_type(value) for value in values if value)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read backend types: {exc}") from exc

    def _queue_position_snapshot(self, ticket_id: str, backend_type: str) -> Optional[int]:
        backend_type_value = self.normalize_backend_type(backend_type)
        position = 0
        for current_ticket_id in self._queue_ids(backend_type_value):
            current = self.get_ticket_raw(current_ticket_id)
            if not current:
                continue
            if str(current.get("status") or "").lower() != "queued":
                continue
            position += 1
            if current_ticket_id == ticket_id:
                return position
        return None

    def get_ticket_position(self, ticket_id: str) -> Optional[int]:
        ticket = self.get_ticket_raw(ticket_id)
        if not ticket:
            return None
        backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        self._repair_queue_membership(backend_type)
        client = self._redis_client()
        queue_ids = self._queue_ids(backend_type)
        position = 0
        for current_ticket_id in queue_ids:
            current = self.get_ticket_raw(current_ticket_id)
            if not current:
                self._remove_ticket_from_queue(backend_type, current_ticket_id)
                client.srem(self._active_key(backend_type), current_ticket_id)
                continue

            status = current.get("status", "")
            if status in self.FINAL_STATES:
                self._remove_ticket_from_queue(backend_type, current_ticket_id)
                client.srem(self._active_key(backend_type), current_ticket_id)
                continue
            if status == "queued":
                position += 1
                if current_ticket_id == ticket_id:
                    return position
        return None

    def _queue_ids(self, backend_type: str) -> List[str]:
        client = self._redis_client()
        try:
            return client.lrange(self._queue_key(backend_type), 0, -1)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read queue {backend_type}: {exc}") from exc

    def _active_ticket_ids(self, backend_type: str) -> List[str]:
        client = self._redis_client()
        try:
            return sorted(client.smembers(self._active_key(backend_type)))
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read active tickets for {backend_type}: {exc}") from exc

    def _queue_contains(self, client, backend_type: str, ticket_id: str) -> bool:
        try:
            return client.lpos(self._queue_key(backend_type), ticket_id) is not None
        except (AttributeError, RedisError):
            try:
                return ticket_id in client.lrange(self._queue_key(backend_type), 0, -1)
            except RedisError as exc:
                raise QueueUnavailableError(
                    f"Failed to inspect queue {backend_type} for ticket {ticket_id}: {exc}"
                ) from exc

    def _ensure_ticket_in_queue(self, client, backend_type: str, ticket_id: str) -> None:
        if not self._queue_contains(client, backend_type, ticket_id):
            client.rpush(self._queue_key(backend_type), ticket_id)

    def _repair_queue_membership(self, backend_type: str) -> None:
        client = self._redis_client()
        queue_key = self._queue_key(backend_type)
        active_key = self._active_key(backend_type)
        try:
            active_ids = list(client.smembers(active_key))
            for ticket_id in active_ids:
                raw = client.hgetall(self._ticket_key(ticket_id))
                if not raw:
                    client.srem(active_key, ticket_id)
                    client.lrem(queue_key, 0, ticket_id)
                    continue

                status = (raw.get("status") or "").lower()
                if status in self.FINAL_STATES:
                    client.srem(active_key, ticket_id)
                    client.lrem(queue_key, 0, ticket_id)
                    continue

                if status in self.ACTIVE_STATES:
                    if not self._queue_contains(client, backend_type, ticket_id):
                        client.rpush(queue_key, ticket_id)
                    client.sadd(active_key, ticket_id)
                else:
                    client.srem(active_key, ticket_id)
                    client.lrem(queue_key, 0, ticket_id)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to repair queue state for {backend_type}: {exc}") from exc

    def _snapshot_ticket_ids(self, backend_type: str) -> List[str]:
        backend_type_value = self.normalize_backend_type(backend_type)
        ordered: List[str] = []
        seen = set()
        for ticket_id in self._queue_ids(backend_type_value):
            if ticket_id and ticket_id not in seen:
                seen.add(ticket_id)
                ordered.append(ticket_id)
        for ticket_id in self._active_ticket_ids(backend_type_value):
            if ticket_id and ticket_id not in seen:
                seen.add(ticket_id)
                ordered.append(ticket_id)
        return ordered

    def list_waiting_frontends(
        self,
        backend_type: str,
        *,
        now_ms: Optional[int] = None,
    ) -> Dict[str, object]:
        backend_type_value = self.normalize_backend_type(backend_type)
        now_ms_value = now_ms if now_ms is not None else int(_utc_now().timestamp() * 1000)
        waiting_frontends: List[Dict[str, object]] = []
        queue_position = 0

        for ticket_id in self._queue_ids(backend_type_value):
            raw = self.get_ticket_raw(ticket_id)
            if not raw:
                continue
            if str(raw.get("status") or "").lower() != "queued":
                continue

            queue_position += 1
            ingress_ts_ms = _safe_int(raw.get("ingress_ts_ms"), 0)
            created_ms = ingress_ts_ms
            if not created_ms:
                created_at = _parse_datetime(raw.get("created_at"))
                if created_at:
                    created_ms = int(created_at.timestamp() * 1000)

            wait_ms = max(0, now_ms_value - created_ms) if created_ms else None
            waiting_frontends.append(
                {
                    "frontend_pod": (raw.get("frontend_pod") or "").strip(),
                    "ticket_id": ticket_id,
                    "queue_position": queue_position,
                    "wait_ms": wait_ms,
                }
            )

        return {
            "backend_type": backend_type_value,
            "queued_count": len(waiting_frontends),
            "waiting_frontends": waiting_frontends,
        }

    def acquire_allocator_lock(self, backend_type: str, owner: Optional[str] = None) -> Optional[str]:
        backend_type_value = self.normalize_backend_type(backend_type)
        client = self._redis_client()
        token = owner or uuid.uuid4().hex
        try:
            acquired = client.set(
                self._lock_key(backend_type_value),
                token,
                nx=True,
                ex=self.lock_ttl_seconds,
            )
            return token if acquired else None
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to acquire lock for {backend_type_value}: {exc}") from exc

    def release_allocator_lock(self, backend_type: str, token: Optional[str]) -> bool:
        if not token:
            return False

        backend_type_value = self.normalize_backend_type(backend_type)
        client = self._redis_client()
        release_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        end
        return 0
        """
        try:
            result = client.eval(release_script, 1, self._lock_key(backend_type_value), token)
            return bool(result)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to release lock for {backend_type_value}: {exc}") from exc

    @contextmanager
    def allocator_lock(self, backend_type: str, owner: Optional[str] = None):
        token = self.acquire_allocator_lock(backend_type, owner=owner)
        try:
            yield token
        finally:
            if token:
                self.release_allocator_lock(backend_type, token)

    def is_allocation_stale(self, ticket: Dict[str, object]) -> bool:
        allocation_deadline = ticket.get("allocation_deadline")
        if isinstance(allocation_deadline, datetime):
            return allocation_deadline <= _utc_now()
        return False

    def is_wait_timeout_expired(self, ticket: Dict[str, object]) -> bool:
        wait_deadline = ticket.get("wait_deadline")
        if isinstance(wait_deadline, datetime):
            return wait_deadline <= _utc_now()
        return False

    def find_stale_allocating_tickets(self, backend_type: Optional[str] = None) -> List[Dict[str, object]]:
        types = [self.normalize_backend_type(backend_type)] if backend_type else self.known_backend_types()
        stale: List[Dict[str, object]] = []
        for type_name in types:
            for ticket_id in self._active_ticket_ids(type_name):
                ticket = self.get_ticket(ticket_id)
                if not ticket:
                    continue
                if ticket.get("status") == "allocating" and self.is_allocation_stale(ticket):
                    stale.append(ticket)
        stale.sort(key=lambda item: item.get("created_at") or _utc_now())
        return stale

    def claim_next_ticket(self, backend_type: str, worker_id: Optional[str] = None) -> Optional[Dict[str, object]]:
        backend_type_value = self.normalize_backend_type(backend_type)
        client = self._redis_client()
        worker_name = worker_id or self.worker_identity
        self._repair_queue_membership(backend_type_value)

        for ticket_id in self._queue_ids(backend_type_value):
            ticket = self.get_ticket(ticket_id)
            if not ticket:
                self._remove_ticket_from_queue(backend_type_value, ticket_id)
                client.srem(self._active_key(backend_type_value), ticket_id)
                continue

            status = ticket.get("status")
            if status in self.FINAL_STATES:
                self._remove_ticket_from_queue(backend_type_value, ticket_id)
                client.srem(self._active_key(backend_type_value), ticket_id)
                continue
            if status != "queued":
                continue
            if self.is_wait_timeout_expired(ticket):
                self.mark_failed(ticket_id, "Queue wait timeout exceeded")
                continue

            claim_token = uuid.uuid4().hex
            claimed_at = _iso_now()
            allocation_deadline = (_utc_now() + timedelta(seconds=self.allocating_ttl_seconds)).isoformat()
            ticket = self._ticket_transition(
                ticket_id,
                backend_type=backend_type_value,
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
            return ticket

        return None

    def _remove_ticket_from_queue(self, backend_type: str, ticket_id: str) -> None:
        client = self._redis_client()
        try:
            client.lrem(self._queue_key(backend_type), 0, ticket_id)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to remove ticket {ticket_id} from queue {backend_type}: {exc}") from exc

    def _raw_to_ticket_dict(self, *args, **kwargs):
        return self.tickets._raw_to_ticket_dict(*args, **kwargs)

    def _ticket_to_dict(self, *args, **kwargs):
        return self.tickets._ticket_to_dict(*args, **kwargs)

    def _ticket_fields(self, *args, **kwargs):
        return self.tickets._ticket_fields(*args, **kwargs)

    def create_ticket(self, *args, **kwargs):
        return self.tickets.create_ticket(*args, **kwargs)

    def set_assigned_request_context(self, *args, **kwargs):
        return self.tickets.set_assigned_request_context(*args, **kwargs)

    def get_assigned_request_context(self, *args, **kwargs):
        return self.tickets.get_assigned_request_context(*args, **kwargs)

    def clear_assigned_request_context(self, *args, **kwargs):
        return self.tickets.clear_assigned_request_context(*args, **kwargs)

    def set_backend_ticket_index(self, *args, **kwargs):
        return self.tickets.set_backend_ticket_index(*args, **kwargs)

    def get_ticket_id_for_backend_pod(self, *args, **kwargs):
        return self.tickets.get_ticket_id_for_backend_pod(*args, **kwargs)

    def get_ticket(self, *args, **kwargs):
        return self.tickets.get_ticket(*args, **kwargs)

    def get_ticket_snapshot(self, *args, **kwargs):
        return self.tickets.get_ticket_snapshot(*args, **kwargs)

    def get_ticket_raw(self, *args, **kwargs):
        return self.tickets.get_ticket_raw(*args, **kwargs)

    def find_ticket_by_backend_pod(self, *args, **kwargs):
        return self.tickets.find_ticket_by_backend_pod(*args, **kwargs)

    def _ticket_transition(self, *args, **kwargs):
        return self.tickets._ticket_transition(*args, **kwargs)

    def _refresh_ticket_ttl(self, *args, **kwargs):
        return self.tickets._refresh_ticket_ttl(*args, **kwargs)

    def list_tickets(self, *args, **kwargs):
        return self.tickets.list_tickets(*args, **kwargs)

    def list_tickets_snapshot(self, *args, **kwargs):
        return self.tickets.list_tickets_snapshot(*args, **kwargs)

    def summarize_active_tickets(self, *args, **kwargs):
        return self.tickets.summarize_active_tickets(*args, **kwargs)

    def find_ticket_by_backend_pod_index_only(self, *args, **kwargs):
        return self.tickets.find_ticket_by_backend_pod_index_only(*args, **kwargs)

    def mark_allocating(self, *args, **kwargs):
        return self.tickets.mark_allocating(*args, **kwargs)

    def requeue_ticket(self, *args, **kwargs):
        return self.tickets.requeue_ticket(*args, **kwargs)

    def mark_assigned(self, *args, **kwargs):
        return self.tickets.mark_assigned(*args, **kwargs)

    def mark_failed(self, *args, **kwargs):
        return self.tickets.mark_failed(*args, **kwargs)

    def cancel_ticket(self, *args, **kwargs):
        return self.tickets.cancel_ticket(*args, **kwargs)


WaitQueue = BackendQueues

__all__ = [
    "BackendQueues",
    "WaitQueue",
    "QueueUnavailableError",
    "parse_datetime",
    "safe_int",
]
