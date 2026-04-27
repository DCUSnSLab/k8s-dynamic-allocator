from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from config import settings

try:
    from redis.exceptions import RedisError, WatchError
except Exception:  # pragma: no cover - import fallback for local analysis
    class RedisError(Exception):
        pass

    class WatchError(Exception):
        pass

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    return _utc_now().isoformat()


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class QueueUnavailableError(RuntimeError):
    pass


class Tickets:
    """Ticket data, status transitions, and backend assignment indexes."""

    FINAL_STATES = {"assigned", "failed", "cancelled"}
    ACTIVE_STATES = {"queued", "allocating"}
    TRANSIENT_STATES = ACTIVE_STATES | FINAL_STATES

    def __init__(self, queue):
        self.queue = queue

    def _raw_to_ticket_dict(
        self,
        ticket_id: str,
        raw: Dict[str, str],
        *,
        queue_position: Optional[int] = None,
    ) -> Dict[str, object]:
        ticket = dict(raw)
        ticket["ticket_id"] = ticket_id
        ticket["retry_count"] = safe_int(ticket.get("retry_count"), 0)
        ticket["max_retries"] = safe_int(ticket.get("max_retries"), self.queue.max_retries)
        ticket["ingress_ts_ms"] = safe_int(ticket.get("ingress_ts_ms"), 0)
        for field in (
            "created_at",
            "updated_at",
            "wait_deadline",
            "claimed_at",
            "allocation_deadline",
            "assigned_at",
            "backend_unavailable_started_at",
            "failed_at",
            "cancelled_at",
        ):
            ticket[field] = parse_datetime(ticket.get(field))
        ticket["queue_position"] = queue_position
        return ticket

    def _ticket_to_dict(self, ticket_id: str, raw: Dict[str, str]) -> Dict[str, object]:
        return self._raw_to_ticket_dict(
            ticket_id,
            raw,
            queue_position=self.queue.get_ticket_position(ticket_id),
        )

    def _ticket_fields(self, **overrides) -> Dict[str, str]:
        ticket = {
            "status": "queued",
            "backend_type": self.queue.default_backend_type,
            "username": "",
            "command": "",
            "frontend_pod": "",
            "frontend_ip": "",
            "request_id": "",
            "request_label": "",
            "ticket_short": "",
            "ingress_ts_ms": "0",
            "wait_deadline": "",
            "claimed_by": "",
            "claim_token": "",
            "claimed_at": "",
            "allocation_deadline": "",
            "backend_pod": "",
            "backend_ip": "",
            "retry_count": "0",
            "max_retries": str(self.queue.max_retries),
            "created_at": _iso_now(),
            "updated_at": _iso_now(),
            "assigned_at": "",
            "backend_unavailable_started_at": "",
            "failed_at": "",
            "cancelled_at": "",
            "error": "",
        }
        ticket.update({k: "" if v is None else str(v) for k, v in overrides.items()})
        return ticket

    def create_ticket(
        self,
        username: str,
        command: str,
        frontend_pod: str,
        frontend_ip: str,
        backend_type: Optional[str] = None,
        request_id: Optional[str] = None,
        ingress_ts_ms: Optional[int] = None,
    ) -> Dict[str, object]:
        backend_type_value = self.queue.normalize_backend_type(backend_type)
        ticket_id = uuid.uuid4().hex
        ticket_short = ticket_id[:10]
        request_label = settings.build_request_label(username, ticket_short)
        ticket = self._ticket_fields(
            status="queued",
            backend_type=backend_type_value,
            username=username,
            command=command,
            frontend_pod=frontend_pod,
            frontend_ip=frontend_ip,
            request_id=request_id or "",
            request_label=request_label,
            ticket_short=ticket_short,
            ingress_ts_ms=ingress_ts_ms or 0,
            wait_deadline=(_utc_now() + timedelta(seconds=self.queue.wait_timeout_seconds)).isoformat(),
        )
        client = self.queue._redis_client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hset(self.queue._ticket_key(ticket_id), mapping=ticket)
            pipe.expire(self.queue._ticket_key(ticket_id), self.queue.ticket_ttl_seconds)
            pipe.rpush(self.queue._queue_key(backend_type_value), ticket_id)
            pipe.sadd(self.queue._active_key(backend_type_value), ticket_id)
            pipe.sadd(self.queue._types_key(), backend_type_value)
            pipe.execute()
            return self.get_ticket(ticket_id) or {"ticket_id": ticket_id, **ticket}
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to create ticket: {exc}") from exc

    def set_assigned_request_context(self, backend_pod: str, context: Dict[str, object]) -> None:
        backend_pod_value = (backend_pod or "").strip()
        if not backend_pod_value:
            return

        payload = {key: "" if value is None else str(value) for key, value in context.items()}
        client = self.queue._redis_client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hset(self.queue._assigned_request_key(backend_pod_value), mapping=payload)
            pipe.expire(self.queue._assigned_request_key(backend_pod_value), self.queue.assigned_context_ttl_seconds)
            ticket_id = (payload.get("ticket_id") or "").strip()
            if ticket_id:
                pipe.set(
                    self.queue._backend_ticket_key(backend_pod_value),
                    ticket_id,
                    ex=self.queue.assigned_context_ttl_seconds,
                )
            pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to store assigned request context for {backend_pod_value}: {exc}"
            ) from exc

    def get_assigned_request_context(self, backend_pod: str) -> Optional[Dict[str, object]]:
        backend_pod_value = (backend_pod or "").strip()
        if not backend_pod_value:
            return None

        client = self.queue._redis_client()
        try:
            raw = client.hgetall(self.queue._assigned_request_key(backend_pod_value))
            if not raw:
                return None
            raw["assigned_at_ms"] = safe_int(raw.get("assigned_at_ms"), 0)
            return raw
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read assigned request context for {backend_pod_value}: {exc}"
            ) from exc

    def clear_assigned_request_context(self, backend_pod: str) -> None:
        backend_pod_value = (backend_pod or "").strip()
        if not backend_pod_value:
            return

        client = self.queue._redis_client()
        try:
            client.delete(
                self.queue._assigned_request_key(backend_pod_value),
                self.queue._backend_ticket_key(backend_pod_value),
            )
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to clear assigned request context for {backend_pod_value}: {exc}"
            ) from exc

    def get_ticket_id_for_backend_pod(self, backend_pod: str) -> Optional[str]:
        backend_pod_value = (backend_pod or "").strip()
        if not backend_pod_value:
            return None

        client = self.queue._redis_client()
        try:
            ticket_id = client.get(self.queue._backend_ticket_key(backend_pod_value))
            return (ticket_id or "").strip() or None
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read backend ticket index for {backend_pod_value}: {exc}"
            ) from exc

    def get_ticket(self, ticket_id: str) -> Optional[Dict[str, object]]:
        client = self.queue._redis_client()
        try:
            raw = client.hgetall(self.queue._ticket_key(ticket_id))
            if not raw:
                return None
            return self._ticket_to_dict(ticket_id, raw)
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read ticket {ticket_id}: {exc}") from exc

    def get_ticket_snapshot(self, ticket_id: str) -> Optional[Dict[str, object]]:
        raw = self.get_ticket_raw(ticket_id)
        if not raw:
            return None
        backend_type = self.queue.normalize_backend_type(raw.get("backend_type"))
        status = str(raw.get("status") or "").lower()
        queue_position = None
        if status == "queued":
            queue_position = self.queue._queue_position_snapshot(ticket_id, backend_type)
        return self._raw_to_ticket_dict(
            ticket_id,
            raw,
            queue_position=queue_position,
        )

    def get_ticket_raw(self, ticket_id: str) -> Optional[Dict[str, str]]:
        client = self.queue._redis_client()
        try:
            raw = client.hgetall(self.queue._ticket_key(ticket_id))
            return raw or None
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to read ticket {ticket_id}: {exc}") from exc

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
        client = self.queue._redis_client()
        ticket_key = self.queue._ticket_key(ticket_id)
        queue_backend_type = self.queue.normalize_backend_type(backend_type) if backend_type else None
        queue_key = self.queue._queue_key(queue_backend_type) if queue_backend_type else None
        active_key = self.queue._active_key(queue_backend_type) if queue_backend_type else None
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

                    backend_type_value = queue_backend_type or self.queue.normalize_backend_type(raw.get("backend_type"))
                    queue_key = self.queue._queue_key(backend_type_value)
                    active_key = self.queue._active_key(backend_type_value)
                    if ensure_in_queue and not self.queue._queue_contains(client, backend_type_value, ticket_id):
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
                    pipe.expire(ticket_key, self.queue.ticket_ttl_seconds)
                    pipe.execute()
                    return self.get_ticket(ticket_id)
            except WatchError:
                continue
        raise QueueUnavailableError(f"Failed to update ticket {ticket_id}: concurrent modification detected")

    def find_ticket_by_backend_pod_index_only(
        self,
        backend_pod: str,
        backend_type: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        backend_pod_value = (backend_pod or "").strip()
        if not backend_pod_value:
            return None

        backend_type_value = self.queue.normalize_backend_type(backend_type) if backend_type else None
        try:
            ticket_id = self.get_ticket_id_for_backend_pod(backend_pod_value)
        except QueueUnavailableError:
            ticket_id = None

        if ticket_id:
            ticket = self.get_ticket_snapshot(ticket_id)
            if ticket:
                assigned_backend = (ticket.get("backend_pod") or "").strip()
                assigned_status = str(ticket.get("status") or "").lower()
                assigned_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
                if (
                    assigned_backend == backend_pod_value
                    and assigned_status == "assigned"
                    and (backend_type_value is None or assigned_type == backend_type_value)
                ):
                    return ticket

        logger.warning(
            "No backend-ticket index for backend pod %s; release correlation degraded",
            backend_pod_value,
        )
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

        ticket_backend_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
        return self._ticket_transition(
            ticket_id,
            backend_type=ticket_backend_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token or ticket.get("claim_token") or None,
            updates={
                "status": "allocating",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "claimed_by": claimed_by or ticket.get("claimed_by") or self.queue.worker_identity,
                "claim_token": claim_token or ticket.get("claim_token") or uuid.uuid4().hex,
                "claimed_at": ticket.get("claimed_at") or _iso_now(),
                "allocation_deadline": (_utc_now() + timedelta(seconds=self.queue.allocating_ttl_seconds)).isoformat(),
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
        if self.queue.is_wait_timeout_expired(ticket):
            return self.mark_failed(ticket_id, reason or "Queue wait timeout exceeded")

        backend_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
        retry_count = safe_int(ticket.get("retry_count"), 0)
        if increment_retry:
            retry_count = min(retry_count + 1, self.queue.max_retries)
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
        backend_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
        ticket = self._ticket_transition(
            ticket_id,
            backend_type=backend_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token if claim_token is not None else ticket.get("claim_token") or None,
            updates={
                "status": "assigned",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "assigned_at": _iso_now(),
                "claimed_by": ticket.get("claimed_by") or self.queue.worker_identity,
                "error": "",
            },
            remove_from_queue=True,
            remove_from_active=True,
        )
        if ticket and str(ticket.get("status") or "").lower() == "assigned":
            client = self.queue._redis_client()
            try:
                pipe = client.pipeline(transaction=False)
                pipe.expire(self.queue._ticket_key(ticket_id), self.queue.assigned_context_ttl_seconds)
                pipe.set(
                    self.queue._backend_ticket_key(backend_pod),
                    ticket_id,
                    ex=self.queue.assigned_context_ttl_seconds,
                )
                pipe.execute()
            except (RedisError, QueueUnavailableError):
                logger.debug("Failed to extend TTL / store backend ticket index for %s", backend_pod)
        return ticket

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
        backend_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
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

        backend_type = self.queue.normalize_backend_type(ticket.get("backend_type"))
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


__all__ = [
    "Tickets",
    "QueueUnavailableError",
    "parse_datetime",
    "safe_int",
]
