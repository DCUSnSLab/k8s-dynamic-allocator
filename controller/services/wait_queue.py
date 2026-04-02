from __future__ import annotations

import logging
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

from config import settings

try:
    import redis
    from redis.exceptions import RedisError, WatchError
except Exception:  # pragma: no cover - import fallback for local analysis
    redis = None

    class RedisError(Exception):
        pass

    class WatchError(Exception):
        pass


logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class QueueUnavailableError(RuntimeError):
    pass


class WaitQueue:
    """Redis-backed per-backend_type wait queue and ticket store."""

    FINAL_STATES = {"assigned", "failed", "cancelled"}
    ACTIVE_STATES = {"queued", "allocating"}
    TRANSIENT_STATES = ACTIVE_STATES | FINAL_STATES

    def __init__(
        self,
        redis_url: Optional[str] = None,
        prefix: Optional[str] = None,
        default_backend_type: Optional[str] = None,
        lock_ttl_seconds: Optional[int] = None,
        wait_timeout_seconds: Optional[int] = None,
        ticket_ttl_seconds: Optional[int] = None,
        allocating_ttl_seconds: Optional[int] = None,
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
        self.max_retries = max_retries or settings.WAIT_QUEUE_MAX_RETRIES
        self.worker_identity = worker_identity or os.getenv("HOSTNAME", "controller-unknown")
        self._client = None
        self._client_lock = threading.Lock()

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

    def _ticket_to_dict(self, ticket_id: str, raw: Dict[str, str]) -> Dict[str, object]:
        ticket = dict(raw)
        ticket["ticket_id"] = ticket_id
        ticket["retry_count"] = _safe_int(ticket.get("retry_count"), 0)
        ticket["max_retries"] = _safe_int(ticket.get("max_retries"), self.max_retries)
        for field in (
            "created_at",
            "updated_at",
            "wait_deadline",
            "claimed_at",
            "allocation_deadline",
            "assigned_at",
            "failed_at",
            "cancelled_at",
        ):
            ticket[field] = _parse_datetime(ticket.get(field))
        queue_position = self.get_ticket_position(ticket_id)
        ticket["queue_position"] = queue_position
        return ticket

    def _ticket_fields(self, **overrides) -> Dict[str, str]:
        ticket = {
            "status": "queued",
            "backend_type": self.default_backend_type,
            "username": "",
            "command": "",
            "frontend_pod": "",
            "frontend_ip": "",
            "request_id": "",
            "wait_deadline": "",
            "claimed_by": "",
            "claim_token": "",
            "claimed_at": "",
            "allocation_deadline": "",
            "backend_pod": "",
            "backend_ip": "",
            "retry_count": "0",
            "max_retries": str(self.max_retries),
            "created_at": _iso_now(),
            "updated_at": _iso_now(),
            "assigned_at": "",
            "failed_at": "",
            "cancelled_at": "",
            "error": "",
        }
        ticket.update({k: "" if v is None else str(v) for k, v in overrides.items()})
        return ticket

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

    def create_ticket(
        self,
        username: str,
        command: str,
        frontend_pod: str,
        frontend_ip: str,
        backend_type: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> Dict[str, object]:
        backend_type_value = self.normalize_backend_type(backend_type)
        ticket_id = uuid.uuid4().hex
        ticket = self._ticket_fields(
            status="queued",
            backend_type=backend_type_value,
            username=username,
            command=command,
            frontend_pod=frontend_pod,
            frontend_ip=frontend_ip,
            request_id=request_id or "",
            wait_deadline=(_utc_now() + timedelta(seconds=self.wait_timeout_seconds)).isoformat(),
        )
        client = self._redis_client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hset(self._ticket_key(ticket_id), mapping=ticket)
            pipe.expire(self._ticket_key(ticket_id), self.ticket_ttl_seconds)
            pipe.rpush(self._queue_key(backend_type_value), ticket_id)
            pipe.sadd(self._active_key(backend_type_value), ticket_id)
            pipe.sadd(self._types_key(), backend_type_value)
            pipe.execute()
            return self.get_ticket(ticket_id) or {"ticket_id": ticket_id, **ticket}
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to create ticket: {exc}") from exc

    def get_ticket(self, ticket_id: str) -> Optional[Dict[str, object]]:
        client = self._redis_client()
        try:
            raw = client.hgetall(self._ticket_key(ticket_id))
            if not raw:
                return None
            return self._ticket_to_dict(ticket_id, raw)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read ticket {ticket_id}: {exc}") from exc

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

    def get_ticket_raw(self, ticket_id: str) -> Optional[Dict[str, str]]:
        client = self._redis_client()
        try:
            raw = client.hgetall(self._ticket_key(ticket_id))
            return raw or None
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read ticket {ticket_id}: {exc}") from exc

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

    def _ticket_transition(
        self,
        ticket_id: str,
        *,
        backend_type: Optional[str] = None,
        expected_statuses: Iterable[str],
        expected_claim_token: Optional[str] = None,
        updates: Optional[Dict[str, object]] = None,
        remove_from_queue: bool = False,
        remove_from_active: bool = False,
        ensure_in_queue: bool = False,
    ) -> Optional[Dict[str, object]]:
        client = self._redis_client()
        ticket_key = self._ticket_key(ticket_id)
        queue_backend_type = self.normalize_backend_type(backend_type) if backend_type else None
        queue_key = self._queue_key(queue_backend_type) if queue_backend_type else None
        active_key = self._active_key(queue_backend_type) if queue_backend_type else None
        expected = {status.lower() for status in expected_statuses}

        for _ in range(3):
            try:
                with client.pipeline() as pipe:
                    watch_keys = [ticket_key]
                    if queue_key:
                        watch_keys.append(queue_key)
                    if active_key:
                        watch_keys.append(active_key)
                    pipe.watch(*watch_keys)
                    raw = pipe.hgetall(ticket_key)
                    if not raw:
                        pipe.reset()
                        return None

                    current_status = (raw.get("status") or "").lower()
                    current_claim_token = raw.get("claim_token") or ""
                    if current_status not in expected:
                        pipe.reset()
                        return None
                    if expected_claim_token is not None and current_claim_token != expected_claim_token:
                        pipe.reset()
                        return None

                    backend_type_value = queue_backend_type or self.normalize_backend_type(raw.get("backend_type"))
                    queue_key = self._queue_key(backend_type_value)
                    active_key = self._active_key(backend_type_value)
                    if ensure_in_queue and not self._queue_contains(client, backend_type_value, ticket_id):
                        queue_should_add = True
                    else:
                        queue_should_add = False

                    payload = {k: "" if v is None else str(v) for k, v in (updates or {}).items()}
                    payload["updated_at"] = _iso_now()

                    pipe.multi()
                    if payload:
                        pipe.hset(ticket_key, mapping=payload)
                    if queue_should_add:
                        pipe.rpush(queue_key, ticket_id)
                    if remove_from_queue:
                        pipe.lrem(queue_key, 0, ticket_id)
                    if remove_from_active:
                        pipe.srem(active_key, ticket_id)
                    if ensure_in_queue:
                        pipe.sadd(active_key, ticket_id)
                    pipe.expire(ticket_key, self.ticket_ttl_seconds)
                    pipe.execute()
                    return self.get_ticket(ticket_id)
            except WatchError:
                continue
        raise QueueUnavailableError(f"Failed to update ticket {ticket_id}: concurrent modification detected")

    def _refresh_ticket_ttl(self, client, ticket_id: str) -> None:
        client.expire(self._ticket_key(ticket_id), self.ticket_ttl_seconds)

    def list_tickets(self, backend_type: Optional[str] = None) -> List[Dict[str, object]]:
        types = [self.normalize_backend_type(backend_type)] if backend_type else self.known_backend_types()
        tickets: List[Dict[str, object]] = []
        seen = set()
        for type_name in types:
            self._repair_queue_membership(type_name)
            for ticket_id in self._queue_ids(type_name):
                if ticket_id in seen:
                    continue
                seen.add(ticket_id)
                ticket = self.get_ticket(ticket_id)
                if ticket:
                    tickets.append(ticket)
        return tickets

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

    def mark_allocating(
        self,
        ticket_id: str,
        backend_pod: str,
        backend_ip: str,
        claimed_by: Optional[str] = None,
        claim_token: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None

        ticket_backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        return self._ticket_transition(
            ticket_id,
            backend_type=ticket_backend_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token or ticket.get("claim_token") or None,
            updates={
                "status": "allocating",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "claimed_by": claimed_by or ticket.get("claimed_by") or self.worker_identity,
                "claim_token": claim_token or ticket.get("claim_token") or uuid.uuid4().hex,
                "claimed_at": ticket.get("claimed_at") or _iso_now(),
                "allocation_deadline": (_utc_now() + timedelta(seconds=self.allocating_ttl_seconds)).isoformat(),
                "error": "",
            },
        )

    def requeue_ticket(
        self,
        ticket_id: str,
        reason: str = "",
        increment_retry: bool = True,
        claim_token: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None
        status = str(ticket.get("status") or "").lower()
        if status == "assigned":
            return ticket
        if status in self.FINAL_STATES:
            return ticket
        if self.is_wait_timeout_expired(ticket):
            return self.mark_failed(ticket_id, reason or "Queue wait timeout exceeded")

        backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        retry_count = _safe_int(ticket.get("retry_count"), 0)
        if increment_retry:
            retry_count = min(retry_count + 1, self.max_retries)
        return self._ticket_transition(
            ticket_id,
            backend_type=backend_type,
            expected_statuses={"queued", "allocating"},
            expected_claim_token=claim_token if claim_token is not None else None,
            updates={
                "status": "queued",
                "retry_count": str(retry_count),
                "claimed_by": "",
                "claim_token": "",
                "claimed_at": "",
                "allocation_deadline": "",
                "backend_pod": "",
                "backend_ip": "",
                "assigned_at": "",
                "failed_at": "",
                "cancelled_at": "",
                "error": reason or "",
            },
            ensure_in_queue=True,
        )

    def mark_assigned(
        self,
        ticket_id: str,
        backend_pod: str,
        backend_ip: str,
        claim_token: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None
        backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        return self._ticket_transition(
            ticket_id,
            backend_type=backend_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token if claim_token is not None else ticket.get("claim_token") or None,
            updates={
                "status": "assigned",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "assigned_at": _iso_now(),
                "claimed_by": ticket.get("claimed_by") or self.worker_identity,
                "error": "",
            },
            remove_from_queue=True,
            remove_from_active=True,
        )

    def mark_failed(
        self,
        ticket_id: str,
        reason: str = "",
        claim_token: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None
        status = str(ticket.get("status") or "").lower()
        if status in self.FINAL_STATES:
            return ticket
        backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        return self._ticket_transition(
            ticket_id,
            backend_type=backend_type,
            expected_statuses={"queued", "allocating"},
            expected_claim_token=claim_token if claim_token is not None else ticket.get("claim_token") or None,
            updates={
                "status": "failed",
                "failed_at": _iso_now(),
                "error": reason or "",
                "claimed_by": "",
                "claim_token": "",
                "claimed_at": "",
                "allocation_deadline": "",
            },
            remove_from_queue=True,
            remove_from_active=True,
        )

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None
        status = str(ticket.get("status") or "").lower()
        if status == "assigned":
            return ticket
        if status in self.FINAL_STATES:
            return ticket

        backend_type = self.normalize_backend_type(ticket.get("backend_type"))
        return self._ticket_transition(
            ticket_id,
            backend_type=backend_type,
            expected_statuses={"queued", "allocating"},
            expected_claim_token=None,
            updates={
                "status": "cancelled",
                "cancelled_at": _iso_now(),
                "error": reason or "",
                "claimed_by": ticket.get("claimed_by") or "",
                "claim_token": "",
                "claimed_at": "",
                "allocation_deadline": "",
            },
            remove_from_queue=True,
            remove_from_active=True,
        )

    def _remove_ticket_from_queue(self, backend_type: str, ticket_id: str) -> None:
        client = self._redis_client()
        try:
            client.lrem(self._queue_key(backend_type), 0, ticket_id)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to remove ticket {ticket_id} from queue {backend_type}: {exc}") from exc
