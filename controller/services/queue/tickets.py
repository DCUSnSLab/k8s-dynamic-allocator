from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

from config import settings

try:
    from redis.exceptions import RedisError, WatchError
except Exception:  # pragma: no cover - import fallback for local analysis
    class RedisError(Exception):
        pass

    class WatchError(Exception):
        pass

logger = logging.getLogger(__name__)

WAIT_ABANDONED_REASON = "Client stopped polling for the wait timeout"


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


def new_release_lock_token() -> str:
    # "<owner>:<epoch-ms>": the owner lets only its holder drop the lock, and
    # the time lets cleanup tell a release that died from one still running.
    return f"{uuid.uuid4().hex}:{int(time.time() * 1000)}"


def release_lock_age_seconds(token: Optional[str], now: Optional[float] = None) -> Optional[float]:
    """Seconds since a release lock was taken, or None if the value says nothing."""
    owner, _, taken_ms = (token or "").partition(":")
    now_value = time.time() if now is None else now
    if owner and taken_ms.isdigit():
        return now_value - int(taken_ms) / 1000.0
    # Locks written before the token format held an ISO timestamp.
    taken_at = parse_datetime(token)
    if taken_at is None:
        return None
    return now_value - taken_at.timestamp()


class QueueUnavailableError(RuntimeError):
    pass


class Tickets:
    """Ticket data, status transitions, and compute assignment indexes."""

    FINAL_STATES = {"assigned", "failed", "cancelled"}
    ACTIVE_STATES = {"queued", "allocating"}
    TRANSIENT_STATES = ACTIVE_STATES | FINAL_STATES

    # HSET has no "only if the key exists" form, so guard it in one round trip.
    # A waiting ticket's TTL is renewed too, so a long queue never expires the
    # ticket of a client that is still polling; assigned tickets keep their own.
    TOUCH_POLL_SCRIPT = (
        "local status = redis.call('HGET', KEYS[1], 'status'); "
        "if not status then return 0 end; "
        "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]); "
        "if status == 'queued' or status == 'allocating' then "
        "redis.call('EXPIRE', KEYS[1], ARGV[3]) end; "
        "return 1"
    )

    # Both keys are per Pod name, and a returned Pod can already be serving the
    # next user, so each is dropped only while it still names this ticket.
    # KEYS[1] = assigned-request hash, KEYS[2] = compute-ticket string.
    CLEAR_IF_OWNER_SCRIPT = (
        "local deleted = {}; "
        "if redis.call('GET', KEYS[2]) == ARGV[1] then "
        "redis.call('DEL', KEYS[2]); table.insert(deleted, KEYS[2]) end; "
        "if redis.call('HGET', KEYS[1], 'ticket_id') == ARGV[1] then "
        "redis.call('DEL', KEYS[1]); table.insert(deleted, KEYS[1]) end; "
        "return deleted"
    )

    # KEYS[1] = release lock. Only the holder of ARGV[1] may drop or replace it.
    RELEASE_LOCK_DROP_SCRIPT = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('DEL', KEYS[1]) end; "
        "return 0"
    )
    RELEASE_LOCK_TAKE_OVER_SCRIPT = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); return 1 end; "
        "return 0"
    )

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
        ticket["request_at_ms"] = safe_int(ticket.get("request_at_ms"), 0)
        ticket["last_poll_ms"] = safe_int(ticket.get("last_poll_ms"), 0)
        for field in (
            "created_at",
            "updated_at",
            "claimed_at",
            "allocation_deadline",
            "assigned_at",
            "compute_available_at",
            "compute_ready_at",
            "compute_unavailable_started_at",
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
            "compute_type": self.queue.default_compute_type,
            "username": "",
            "command": "",
            "user_pod": "",
            "user_pod_ip": "",
            "request_id": "",
            "request_label": "",
            "ticket_short": "",
            "request_at_ms": "0",
            "last_poll_ms": "0",
            "claimed_by": "",
            "claim_token": "",
            "claimed_at": "",
            "allocation_deadline": "",
            "compute_pod": "",
            "compute_pod_ip": "",
            "retry_count": "0",
            "max_retries": str(self.queue.max_retries),
            "created_at": _iso_now(),
            "updated_at": _iso_now(),
            "assigned_at": "",
            "compute_available_at": "",
            "compute_ready_at": "",
            "compute_unavailable_started_at": "",
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
        user_pod: str,
        user_pod_ip: str,
        compute_type: Optional[str] = None,
        request_id: Optional[str] = None,
        request_at_ms: Optional[int] = None,
        ticket_id: Optional[str] = None,
    ) -> Dict[str, object]:
        compute_type_value = self.queue.normalize_compute_type(compute_type)
        ticket_id_value = (ticket_id or "").strip() or uuid.uuid4().hex
        ticket_short = ticket_id_value[:10]
        request_label = settings.build_request_label(username, ticket_short)
        ticket = self._ticket_fields(
            status="queued",
            compute_type=compute_type_value,
            username=username,
            command=command,
            user_pod=user_pod,
            user_pod_ip=user_pod_ip,
            request_id=request_id or "",
            request_label=request_label,
            ticket_short=ticket_short,
            request_at_ms=request_at_ms or 0,
        )
        client = self.queue._redis_client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hset(self.queue._ticket_key(ticket_id_value), mapping=ticket)
            pipe.expire(self.queue._ticket_key(ticket_id_value), self.queue.ticket_ttl_seconds)
            pipe.zadd(
                self.queue._queue_key(compute_type_value),
                {ticket_id_value: self.queue.queue_score(ticket)},
            )
            pipe.sadd(self.queue._active_key(compute_type_value), ticket_id_value)
            pipe.sadd(self.queue._types_key(), compute_type_value)
            pipe.execute()
            return self.get_ticket(ticket_id_value) or {"ticket_id": ticket_id_value, **ticket}
        except RedisError as exc:
            raise QueueUnavailableError(f"Failed to create ticket: {exc}") from exc

    def set_assigned_request_context(self, compute_pod: str, context: Dict[str, object]) -> None:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return

        payload = {key: "" if value is None else str(value) for key, value in context.items()}
        client = self.queue._redis_client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hset(self.queue._assigned_request_key(compute_pod_value), mapping=payload)
            pipe.expire(self.queue._assigned_request_key(compute_pod_value), self.queue.assigned_context_ttl_seconds)
            ticket_id = (payload.get("ticket_id") or "").strip()
            if ticket_id:
                pipe.set(
                    self.queue._compute_ticket_key(compute_pod_value),
                    ticket_id,
                    ex=self.queue.assigned_context_ttl_seconds,
                )
            pipe.execute()
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to store assigned request context for {compute_pod_value}: {exc}"
            ) from exc

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return None

        client = self.queue._redis_client()
        try:
            raw = client.hgetall(self.queue._assigned_request_key(compute_pod_value))
            if not raw:
                return None
            raw["assigned_at_ms"] = safe_int(raw.get("assigned_at_ms"), 0)
            return raw
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read assigned request context for {compute_pod_value}: {exc}"
            ) from exc

    def clear_assigned_request_context(self, compute_pod: str) -> None:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return

        client = self.queue._redis_client()
        try:
            client.delete(
                self.queue._assigned_request_key(compute_pod_value),
                self.queue._compute_ticket_key(compute_pod_value),
            )
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to clear assigned request context for {compute_pod_value}: {exc}"
            ) from exc

    def clear_assigned_request_context_if_owner(self, compute_pod: str, ticket_id: str) -> List[str]:
        """Clear a Pod's assignment keys only if they still belong to ticket_id.

        Returns the keys actually deleted. The unconditional clear stays for a
        deleted Pod that named no ticket to check against.
        """
        compute_pod_value = (compute_pod or "").strip()
        ticket_id_value = (ticket_id or "").strip()
        if not compute_pod_value or not ticket_id_value:
            return []

        client = self.queue._redis_client()
        try:
            deleted = client.eval(
                self.CLEAR_IF_OWNER_SCRIPT,
                2,
                self.queue._assigned_request_key(compute_pod_value),
                self.queue._compute_ticket_key(compute_pod_value),
                ticket_id_value,
            )
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to clear assigned request context for {compute_pod_value}: {exc}"
            ) from exc
        return list(deleted or [])

    def acquire_release_lock(self, ticket_id: str) -> Optional[str]:
        """Claim the single release a ticket gets: its token, or None if held.

        Once a release has settled the Pod the lock is kept as long as the
        assigned context: the user client retries and the agent sends a
        fallback of its own, and a second scrub racing the first could reach
        the Pod after it went to the next user. Only a release that leaves the
        Pod still assigned to the ticket drops it (drop_release_lock).
        """
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value:
            return None

        key = self.queue._release_lock_key(ticket_id_value)
        token = new_release_lock_token()
        client = self.queue._redis_client()
        try:
            acquired = client.set(
                key,
                token,
                nx=True,
                ex=max(1, int(self.queue.assigned_context_ttl_seconds)),
            )
        except RedisError as exc:
            # The SET may have landed with only its reply lost, and a lock
            # nobody knows it owns is never dropped; the token tells.
            try:
                current = client.get(key)
            except RedisError:
                current = None
            if current == token:
                return token
            if current:
                return None
            raise QueueUnavailableError(
                f"Failed to acquire release lock for ticket {ticket_id_value}: {exc}"
            ) from exc
        return token if acquired else None

    def get_release_lock(self, ticket_id: str) -> Optional[str]:
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value:
            return None

        client = self.queue._redis_client()
        try:
            return client.get(self.queue._release_lock_key(ticket_id_value)) or None
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read release lock for ticket {ticket_id_value}: {exc}"
            ) from exc

    def drop_release_lock(self, ticket_id: str, token: Optional[str]) -> bool:
        """Delete the lock only if token still holds it.

        A release that outlived its lock (cleanup took it over as stale) must
        not drop the one that replaced it.
        """
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value or not token:
            return False

        client = self.queue._redis_client()
        try:
            return bool(
                client.eval(
                    self.RELEASE_LOCK_DROP_SCRIPT,
                    1,
                    self.queue._release_lock_key(ticket_id_value),
                    token,
                )
            )
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to drop release lock for ticket {ticket_id_value}: {exc}"
            ) from exc

    def take_over_release_lock(self, ticket_id: str, stale_token: str) -> Optional[str]:
        """Replace stale_token with a fresh one: the new token, or None if it moved on."""
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value or not stale_token:
            return None

        token = new_release_lock_token()
        client = self.queue._redis_client()
        try:
            replaced = client.eval(
                self.RELEASE_LOCK_TAKE_OVER_SCRIPT,
                1,
                self.queue._release_lock_key(ticket_id_value),
                stale_token,
                token,
                max(1, int(self.queue.assigned_context_ttl_seconds)),
            )
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to take over release lock for ticket {ticket_id_value}: {exc}"
            ) from exc
        return token if replaced else None

    def get_ticket_id_for_compute_pod(self, compute_pod: str) -> Optional[str]:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return None

        client = self.queue._redis_client()
        try:
            ticket_id = client.get(self.queue._compute_ticket_key(compute_pod_value))
            return (ticket_id or "").strip() or None
        except RedisError as exc:
            raise QueueUnavailableError(
                f"Failed to read compute ticket index for {compute_pod_value}: {exc}"
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
        status = str(raw.get("status") or "").lower()
        queue_position = None
        if status == "queued":
            queue_position = self.queue.get_ticket_position(ticket_id)
        return self._raw_to_ticket_dict(
            ticket_id,
            raw,
            queue_position=queue_position,
        )

    def touch_poll(self, ticket_id: str) -> None:
        """Record that the waiting client just asked about its ticket.

        Writes only to an existing ticket: a plain HSET would create an
        untracked hash with no TTL for any unknown id reaching the endpoint.
        Best effort, since losing a liveness sample must not fail the response.
        """
        client = self.queue._redis_client()
        try:
            client.eval(
                self.TOUCH_POLL_SCRIPT,
                1,
                self.queue._ticket_key(ticket_id),
                "last_poll_ms",
                str(int(time.time() * 1000)),
                str(self.queue.ticket_ttl_seconds),
            )
        except RedisError as exc:
            logger.warning(
                "[Warning] operation=touch_poll ticket_id=%s reason=%r",
                ticket_id,
                str(exc),
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
        compute_type: Optional[str] = None,
        expected_statuses: Iterable[str],
        expected_claim_token: Optional[str] = None,
        updates: Optional[Dict[str, object]] = None,
        remove_from_queue: bool = False,
        remove_from_active: bool = False,
        ensure_in_queue: bool = False,
    ) -> Optional[Dict[str, object]]:
        client = self.queue._redis_client()
        ticket_key = self.queue._ticket_key(ticket_id)
        queue_compute_type = self.queue.normalize_compute_type(compute_type) if compute_type else None
        queue_key = self.queue._queue_key(queue_compute_type) if queue_compute_type else None
        active_key = self.queue._active_key(queue_compute_type) if queue_compute_type else None
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

                    compute_type_value = queue_compute_type or self.queue.normalize_compute_type(raw.get("compute_type"))
                    queue_key = self.queue._queue_key(compute_type_value)
                    active_key = self.queue._active_key(compute_type_value)
                    if ensure_in_queue and not self.queue._queue_contains(client, compute_type_value, ticket_id):
                        queue_should_add = True
                    else:
                        queue_should_add = False

                    payload = {k: "" if v is None else str(v) for k, v in (updates or {}).items()}
                    payload["updated_at"] = _iso_now()

                    pipe.multi()
                    if payload:
                        pipe.hset(ticket_key, mapping=payload)
                    if queue_should_add:
                        pipe.zadd(queue_key, {ticket_id: self.queue.queue_score(raw)})
                    if remove_from_queue:
                        pipe.zrem(queue_key, ticket_id)
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

    def find_ticket_by_compute_pod_index_only(
        self,
        compute_pod: str,
        compute_type: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return None

        compute_type_value = self.queue.normalize_compute_type(compute_type) if compute_type else None
        try:
            ticket_id = self.get_ticket_id_for_compute_pod(compute_pod_value)
        except QueueUnavailableError:
            ticket_id = None

        if ticket_id:
            ticket = self.get_ticket_snapshot(ticket_id)
            if ticket:
                assigned_compute = (ticket.get("compute_pod") or "").strip()
                assigned_status = str(ticket.get("status") or "").lower()
                assigned_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
                if (
                    assigned_compute == compute_pod_value
                    and assigned_status == "assigned"
                    and (compute_type_value is None or assigned_type == compute_type_value)
                ):
                    return ticket

        logger.warning(
            "[Warning] operation=release_correlation compute_pod=%s reason=%r",
            compute_pod_value,
            "missing compute-ticket index",
        )
        return None

    def mark_allocating(
        self,
        ticket_id: str,
        compute_pod: str,
        compute_pod_ip: str,
        claimed_by: Optional[str] = None,
        claim_token: Optional[str] = None,
        compute_ready_at: Optional[object] = None,
        compute_available_at: Optional[object] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None

        ticket_compute_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
        return self._ticket_transition(
            ticket_id,
            compute_type=ticket_compute_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token or ticket.get("claim_token") or None,
            updates={
                "status": "allocating",
                "compute_pod": compute_pod,
                "compute_pod_ip": compute_pod_ip,
                "claimed_by": claimed_by or ticket.get("claimed_by") or self.queue.worker_identity,
                "claim_token": claim_token or ticket.get("claim_token") or uuid.uuid4().hex,
                "claimed_at": ticket.get("claimed_at") or _iso_now(),
                "allocation_deadline": (_utc_now() + timedelta(seconds=self.queue.allocating_ttl_seconds)).isoformat(),
                "compute_available_at": compute_available_at or ticket.get("compute_available_at") or "",
                "compute_ready_at": compute_ready_at or ticket.get("compute_ready_at") or "",
                "error": "",
            },
        )

    def extend_allocation_deadline(self, ticket_id: str, claim_token: str) -> bool:
        """Keep an allocating claim alive while its compute pod is still starting.

        Returns False once the claim is lost (cancelled, failed, or reclaimed by
        another worker), so the caller can stop waiting and delete its pod.
        """
        ticket = self._ticket_transition(
            ticket_id,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token,
            updates={
                "allocation_deadline": (
                    _utc_now() + timedelta(seconds=self.queue.allocating_ttl_seconds)
                ).isoformat(),
            },
        )
        return ticket is not None

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
            return self.mark_failed(ticket_id, reason or WAIT_ABANDONED_REASON)

        compute_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
        retry_count = safe_int(ticket.get("retry_count"), 0)
        if increment_retry:
            retry_count = min(retry_count + 1, self.queue.max_retries)
        return self._ticket_transition(
            ticket_id,
            compute_type=compute_type,
            expected_statuses={"queued", "allocating"},
            expected_claim_token=claim_token if claim_token is not None else None,
            updates={
                "status": "queued",
                "retry_count": str(retry_count),
                "claimed_by": "",
                "claim_token": "",
                "claimed_at": "",
                "allocation_deadline": "",
                "compute_pod": "",
                "compute_pod_ip": "",
                "assigned_at": "",
                "compute_available_at": "",
                "compute_ready_at": "",
                "failed_at": "",
                "cancelled_at": "",
                "error": reason or "",
            },
            ensure_in_queue=True,
        )

    def mark_assigned(
        self,
        ticket_id: str,
        compute_pod: str,
        compute_pod_ip: str,
        claim_token: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        ticket = self.get_ticket(ticket_id)
        if not ticket:
            return None
        compute_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
        ticket = self._ticket_transition(
            ticket_id,
            compute_type=compute_type,
            expected_statuses={"allocating"},
            expected_claim_token=claim_token if claim_token is not None else ticket.get("claim_token") or None,
            updates={
                "status": "assigned",
                "compute_pod": compute_pod,
                "compute_pod_ip": compute_pod_ip,
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
                    self.queue._compute_ticket_key(compute_pod),
                    ticket_id,
                    ex=self.queue.assigned_context_ttl_seconds,
                )
                pipe.execute()
            except (RedisError, QueueUnavailableError):
                logger.debug(
                    "[ComputeTicketIndexSkipped] compute_pod=%s reason=%r",
                    compute_pod,
                    "failed to extend TTL or store compute ticket index",
                )
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
        compute_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
        return self._ticket_transition(
            ticket_id,
            compute_type=compute_type,
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

        compute_type = self.queue.normalize_compute_type(ticket.get("compute_type"))
        return self._ticket_transition(
            ticket_id,
            compute_type=compute_type,
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
