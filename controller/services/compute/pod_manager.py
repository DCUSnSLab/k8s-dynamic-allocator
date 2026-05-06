import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from kubernetes.client.rest import ApiException

from config import settings
from config.settings import get_request_label, set_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError, safe_int
from .agent_client import ComputeAgent, ComputeAgentError
from .warm_pool import PodConflictError

logger = logging.getLogger(__name__)


class ComputePodManager:
    _RELEASE_MAX_ATTEMPTS = 2

    def __init__(self, pool, queues, tickets):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self._compute_refresh_lock = threading.Lock()
        self._compute_refresh_interval = max(settings.WAIT_QUEUE_COMPUTE_REFRESH_SECONDS, 1.0)
        self._last_compute_refresh = 0.0
        self._queue_kick_lock = threading.Lock()
        self._queue_kick_in_flight = False
        self._queue_kick_pending_compute_types = set()
        self._queue_kick_full_scan_pending = False

    def execute_command(
        self,
        username: str,
        command: str,
        user_pod_ip: str,
        user_pod: str = "",
        compute_type: Optional[str] = None,
        ingress_ts_ms: Optional[int] = None,
        ticket_id: Optional[str] = None,
    ) -> Dict:
        try:
            if not user_pod_ip:
                return {
                    "status": "error",
                    "message": "user_pod_ip is required",
                }

            compute_type_value = self._validate_compute_type_for_enqueue(compute_type)
            user_pod_value = user_pod or ""
            ticket = self.tickets.create_ticket(
                username=username,
                command=command,
                user_pod=user_pod_value,
                user_pod_ip=user_pod_ip,
                compute_type=compute_type_value,
                request_id=get_request_label(),
                ingress_ts_ms=ingress_ts_ms,
                ticket_id=ticket_id,
            )

            ticket_format.set_ticket_context(ticket)
            ticket_format.log_queue_event(
                "info",
                "Enqueued",
                ticket=ticket,
                include_ticket_fields=(),
                queue_position=ticket.get("queue_position"),
                ingress_ts_ms=ticket.get("ingress_ts_ms"),
            )

            self._kick_wait_queue_worker(compute_type_value)
            current_ticket = self.tickets.get_ticket(ticket["ticket_id"])
            response = ticket_format.ticket_response(current_ticket or ticket)
            if response["status"] == "assigned":
                response["message"] = "Command assigned to compute pod"
            elif response["status"] == "failed":
                response["message"] = "Command failed during allocation"
            elif response["status"] == "cancelled":
                response["message"] = "Ticket was cancelled during allocation"
            else:
                response["message"] = "Command queued for allocation"
            return response
        except ValueError as exc:
            return {
                "status": "error",
                "message": str(exc),
                "error_type": "invalid_compute_type",
            }
        except QueueUnavailableError as exc:
            ticket_format.log_queue_event(
                "error",
                "Failed",
                ticket=None,
                include_ticket_fields=(),
                operation="execute",
                error_type="queue_unavailable",
                reason=str(exc),
            )
            return {
                "status": "error",
                "message": str(exc),
            }
        except ApiException as exc:
            ticket_format.log_queue_event(
                "error",
                "Failed",
                ticket=None,
                include_ticket_fields=(),
                operation="execute",
                error_type="k8s_api",
                status=exc.status,
                reason=exc.reason,
            )
            return {
                "status": "error",
                "message": f"K8s API error: {exc.reason}",
            }
        except Exception as exc:
            ticket_format.log_queue_event(
                "error",
                "Failed",
                ticket=None,
                include_ticket_fields=(),
                operation="execute",
                error_type="unexpected",
                reason=str(exc),
            )
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}",
            }

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Dict:
        try:
            current = self.tickets.get_ticket(ticket_id)
            if not current:
                return {
                    "status": "error",
                    "message": f"Ticket not found: {ticket_id}",
                }
            if current.get("status") == "assigned":
                return {
                    "status": "error",
                    "message": "Ticket is already assigned",
                    "ticket": current,
                }

            cancelled = self.tickets.cancel_ticket(ticket_id, reason=reason)
            final_ticket = cancelled or self.tickets.get_ticket(ticket_id) or current
            if final_ticket and str(final_ticket.get("status") or "").lower() == "cancelled":
                queue_wait_ms = ticket_format.elapsed_since_ingress_ms(final_ticket, "cancelled_at")
                ticket_format.log_queue_event(
                    "info",
                    "Cancelled",
                    final_ticket,
                    include_ticket_fields=(),
                    reason=reason or final_ticket.get("error") or "user_cancel",
                    queue_wait_ms=queue_wait_ms,
                )
            elif final_ticket:
                ticket_format.log_queue_event(
                    "debug",
                    "CancelNoop",
                    final_ticket,
                    include_ticket_fields=(),
                    reason=reason or final_ticket.get("error") or "cancel no-op",
                )
            compute_pod = final_ticket.get("compute_pod") or ""
            if compute_pod and final_ticket.get("status") in {"cancelled", "failed"}:
                try:
                    self.pool.release_pod(compute_pod)
                except Exception as exc:
                    ticket_format.log_queue_event(
                        "warning",
                        "Warning",
                        final_ticket,
                        include_ticket_fields=(),
                        operation="cancel_release",
                        compute_pod=compute_pod,
                        reason=str(exc),
                    )
                self._clear_assigned_request_context_best_effort(compute_pod)

            if str(final_ticket.get("status") or "").lower() == "cancelled":
                self._kick_wait_queue_worker(final_ticket.get("compute_type"))

            if final_ticket.get("status") == "assigned":
                return {
                    "status": "error",
                    "message": "Ticket is already assigned",
                    "ticket": final_ticket,
                }
            message = "Ticket cancelled" if final_ticket.get("status") == "cancelled" else "Cancellation did not change ticket state"
            return ticket_format.ticket_response(final_ticket, message)
        except QueueUnavailableError as exc:
            return {
                "status": "error",
                "message": str(exc),
            }

    def release_compute_pod(self, compute_pod: str, request_context: Optional[Dict[str, object]] = None) -> Dict:
        release_started_ms = int(time.time() * 1000)
        request_context_value = dict(request_context or {})

        last_error: Optional[Exception] = None
        cleanup_context = False
        compute_unmounted = False
        released_compute_type: Optional[str] = None

        def _release_once() -> Dict:
            nonlocal cleanup_context, compute_unmounted, released_compute_type
            release_started = time.perf_counter()
            compute_type = self.queues.default_compute_type
            try:
                pod = self.pool.v1.read_namespaced_pod(compute_pod, self.pool.namespace)
                labels = pod.metadata.labels or {}
                compute_type = self.queues.normalize_compute_type(labels.get(self.pool.LABEL_COMPUTE_TYPE))
            except ApiException as exc:
                if exc.status != 404:
                    raise
                logger.debug("[Released] compute_pod=%s status=already_released", compute_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Compute already released: {compute_pod}",
                }

            assigned_context = dict(request_context_value)
            if not assigned_context:
                try:
                    assigned_context = self.tickets.get_assigned_request_context(compute_pod) or {}
                except QueueUnavailableError as exc:
                    logger.debug(
                        "[AssignedContextUnavailable] compute_pod=%s reason=%r",
                        compute_pod,
                        str(exc),
                    )

            if assigned_context.get("request_label"):
                set_request_label(assigned_context.get("request_label"))

            ticket = self._ticket_for_compute_pod_with_context(compute_pod, assigned_context)
            if ticket:
                ticket_format.set_ticket_context(ticket)
                compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
            elif assigned_context:
                compute_type = self.queues.normalize_compute_type(
                    assigned_context.get("compute_type") or compute_type
                )
            released_compute_type = compute_type

            compute_pod_ip = self.pool.get_pod_ip(compute_pod)
            if compute_pod_ip and not compute_unmounted:
                with ComputeAgent(compute_pod_ip) as agent:
                    agent.unmount()
                    compute_unmounted = True

            released_now = self.pool.release_pod(compute_pod)
            if not released_now:
                logger.debug("[Released] compute_pod=%s status=already_released_or_terminating", compute_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Compute already released: {compute_pod}",
                }

            release_ms = int((time.perf_counter() - release_started) * 1000)
            assigned_at_ms = 0
            if ticket:
                assigned_at_ms = ticket_format.datetime_to_epoch_ms(ticket.get("assigned_at"))
            if not assigned_at_ms:
                assigned_at_ms = safe_int(assigned_context.get("assigned_at_ms"), 0)
            session_ms = max(0, release_started_ms - assigned_at_ms) if assigned_at_ms else None
            ticket_format.log_queue_event(
                "info",
                "Released",
                ticket,
                component="SUCCESS",
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip or "",
                session_ms=session_ms,
                release_ms=release_ms,
            )
            cleanup_context = True
            return {
                "status": "success",
                "message": f"Released: {compute_pod}",
            }

        try:
            for attempt in range(1, self._RELEASE_MAX_ATTEMPTS + 1):
                try:
                    response = _release_once()
                    if response.get("status") == "success":
                        self._kick_wait_queue_worker(released_compute_type)
                    return response
                except Exception as exc:
                    last_error = exc
                    if attempt < self._RELEASE_MAX_ATTEMPTS:
                        logger.debug(
                            "[ReleaseRetry] compute_pod=%s attempt=%s max_attempts=%s reason=%r",
                            compute_pod,
                            attempt,
                            self._RELEASE_MAX_ATTEMPTS,
                            str(exc),
                        )
                        continue
                    logger.error("[Failed] operation=release compute_pod=%s reason=%r", compute_pod, str(exc))
                    return {
                        "status": "error",
                        "message": str(exc),
                    }
        finally:
            if cleanup_context:
                self._clear_assigned_request_context_best_effort(compute_pod)

        return {
            "status": "error",
            "message": str(last_error) if last_error else "Unknown release failure",
        }

    def process_wait_queues(self, compute_type: Optional[str] = None) -> Dict:
        try:
            if compute_type:
                compute_types = {self.queues.normalize_compute_type(compute_type)}
            else:
                compute_types = set(self.queues.compute_types_with_queued_tickets())
                compute_types.update(self._compute_types_with_stale_allocations())

            processed = []
            for compute_type in sorted(compute_types):
                try:
                    processed.append(self._drain_wait_queue_for_type(compute_type))
                except Exception as exc:
                    logger.exception(
                        "[Failed] operation=drain_wait_queue compute_type=%s reason=%r",
                        compute_type,
                        str(exc),
                    )
                    processed.append(
                        {
                            "compute_type": compute_type,
                            "status": "error",
                            "error": str(exc),
                        }
                    )

            return {
                "status": "success",
                "processed": processed,
            }
        except QueueUnavailableError as exc:
            return {
                "status": "error",
                "message": str(exc),
            }

    def notify_compute_available(
        self,
        compute_type: Optional[str],
        compute_pod: str = "",
        compute_available_at: str = "",
        source: str = "watch",
    ) -> None:
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        try:
            if not self.queues.has_queued_tickets(compute_type_value):
                return
        except QueueUnavailableError as exc:
            logger.debug(
                "[ComputeWatchSkipped] compute_type=%s reason=%r",
                compute_type_value,
                str(exc),
            )
            return

        self._remember_compute_available(
            compute_type_value=compute_type_value,
            compute_pod=compute_pod,
            compute_available_at=compute_available_at,
            source=source,
        )
        self._kick_wait_queue_worker(compute_type_value)

    def _remember_compute_available(
        self,
        *,
        compute_type_value: str,
        compute_pod: str,
        compute_available_at: str,
        source: str,
    ) -> None:
        compute_pod_value = (compute_pod or "").strip()
        if not compute_pod_value:
            return

        observed_at = compute_available_at or datetime.now(timezone.utc).isoformat()
        try:
            recorded = self.queues.record_compute_available(
                compute_pod_value,
                observed_at,
            )
        except QueueUnavailableError as exc:
            logger.debug(
                "[ComputeAvailableSkipped] compute_type=%s compute_pod=%s reason=%r",
                compute_type_value,
                compute_pod_value,
                str(exc),
            )
            return

        if recorded:
            logger.debug(
                "[ComputeAvailable] compute_type=%s compute_pod=%s compute_available_ts_ms=%s source=%s",
                compute_type_value,
                compute_pod_value,
                ticket_format.datetime_to_epoch_ms(observed_at),
                source or "watch",
            )

    def _pop_compute_available_at(self, compute_pod: str) -> str:
        try:
            return self.queues.pop_compute_available_at(compute_pod)
        except QueueUnavailableError as exc:
            logger.debug(
                "[ComputeAvailableLookupSkipped] compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )
            return ""

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.tickets.get_assigned_request_context(compute_pod)

    def refresh_compute_types(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_compute_refresh) < self._compute_refresh_interval:
            return

        with self._compute_refresh_lock:
            now = time.monotonic()
            if not force and (now - self._last_compute_refresh) < self._compute_refresh_interval:
                return

            self._last_compute_refresh = now
            compute_types = set()
            try:
                self.pool.initialize_pool(log_existing=False)
            except Exception as exc:
                logger.debug("[ComputeRefreshSkipped] reason=%r", str(exc))
            compute_types.update(self.queues.known_compute_types())
            for item in self.pool.list_pool_status():
                if item.get("compute_type"):
                    compute_types.add(self.queues.normalize_compute_type(item.get("compute_type")))

            if compute_types:
                self.queues.register_compute_types(sorted(compute_types))

    def recover_stale_ticket(self, ticket: Dict) -> Dict:
        """Public - shared with ComputeCleanup."""
        ticket_id = ticket.get("ticket_id", "")
        compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
        compute_pod = ticket.get("compute_pod", "")
        claim_token = ticket.get("claim_token") or None
        retry_count = int(ticket.get("retry_count") or 0)
        max_retries = int(ticket.get("max_retries") or self.queues.max_retries)
        skipped = {"ticket_id": ticket_id, "compute_type": compute_type, "status": "skipped"}

        try:
            current = self.tickets.get_ticket(ticket_id)
            if not current or str(current.get("status") or "").lower() != "allocating":
                return skipped
            if claim_token and (current.get("claim_token") or "") != claim_token:
                return skipped

            if retry_count < max_retries:
                recovered = self.tickets.requeue_ticket(
                    ticket_id,
                    reason="stale allocation recovered",
                    increment_retry=True,
                    claim_token=claim_token,
                )
                if recovered and recovered.get("status") == "queued":
                    self._release_pod_best_effort(compute_pod, ticket_id)
                    ticket_format.log_queue_event(
                        "debug",
                        "Requeued",
                        recovered,
                        include_ticket_fields=(),
                        source="stale_allocation",
                        compute_pod=compute_pod,
                        retry_count=recovered.get("retry_count"),
                        reason="stale allocation recovered",
                    )
                    return {"ticket_id": ticket_id, "compute_type": compute_type, "status": "requeued"}
                return skipped

            failed = self.tickets.mark_failed(
                ticket_id,
                "stale allocation exceeded retry budget",
                claim_token=claim_token,
            )
            if failed and failed.get("status") == "failed":
                self._release_pod_best_effort(compute_pod, ticket_id)
                ticket_format.log_queue_event(
                    "debug",
                    "Failed",
                    failed,
                    include_ticket_fields=(),
                    source="stale_allocation",
                    compute_pod=compute_pod,
                    retry_count=failed.get("retry_count"),
                    reason="stale allocation exceeded retry budget",
                )
                return {"ticket_id": ticket_id, "compute_type": compute_type, "status": "failed"}
            return skipped
        except QueueUnavailableError as exc:
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "error",
                "error": str(exc),
            }

    def _compute_types_with_stale_allocations(self) -> List[str]:
        compute_types = []
        for compute_type in self.queues.known_compute_types():
            compute_type_value = self.queues.normalize_compute_type(compute_type)
            if self.queues.find_stale_allocating_tickets(compute_type_value):
                compute_types.append(compute_type_value)
        return sorted(set(compute_types))

    def _kick_wait_queue_worker(self, compute_type: Optional[str] = None) -> None:
        compute_type_value = self.queues.normalize_compute_type(compute_type) if compute_type else None
        with self._queue_kick_lock:
            if self._queue_kick_in_flight:
                if compute_type_value:
                    self._queue_kick_pending_compute_types.add(compute_type_value)
                else:
                    self._queue_kick_full_scan_pending = True
                return
            self._queue_kick_in_flight = True

        def _runner():
            next_compute_type = compute_type_value
            lock_retry_count = 0
            try:
                while True:
                    result = self.process_wait_queues(compute_type=next_compute_type)
                    if (
                        next_compute_type
                        and self._process_result_had_lock_miss(result)
                        and self.queues.has_queued_tickets(next_compute_type)
                        and lock_retry_count < 3
                    ):
                        lock_retry_count += 1
                        time.sleep(0.1)
                        continue
                    lock_retry_count = 0
                    with self._queue_kick_lock:
                        if self._queue_kick_full_scan_pending:
                            self._queue_kick_full_scan_pending = False
                            self._queue_kick_pending_compute_types.clear()
                            next_compute_type = None
                            continue
                        if self._queue_kick_pending_compute_types:
                            next_compute_type = self._queue_kick_pending_compute_types.pop()
                            continue
                        self._queue_kick_in_flight = False
                        return
            finally:
                with self._queue_kick_lock:
                    if self._queue_kick_in_flight:
                        self._queue_kick_in_flight = False

        threading.Thread(
            target=_runner,
            name="queue-kick",
            daemon=True,
        ).start()

    def _process_result_had_lock_miss(self, result: Dict) -> bool:
        for item in result.get("processed") or []:
            if item.get("lock_missed"):
                return True
        return False

    def _compute_wait_queue_batch_plan(self) -> Tuple[int, int]:
        """Return (effective_batch, mount_concurrency) clamped against TTL.

        Reservations are committed under the allocator lock with status
        `allocating`, then mounted outside the lock. If the wall-clock for
        a mount wave exceeds WAIT_QUEUE_ALLOCATING_TTL_SECONDS, late
        reservations can be flagged stale before their mount even starts.

        We bound the batch so worst-case wall-clock fits inside the TTL,
        with a small margin for stale-scan jitter / scheduling.
        """
        configured_batch = max(1, int(getattr(settings, "WAIT_QUEUE_BATCH_LIMIT", 10)))
        configured_concurrency = max(
            1,
            int(getattr(settings, "WAIT_QUEUE_MOUNT_CONCURRENCY", configured_batch)),
        )

        mount_timeout = max(1.0, float(settings.COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS))
        ttl = max(1.0, float(settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS))
        worker_interval = max(0.0, float(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS))

        if ttl <= mount_timeout:
            logger.warning(
                "[Warning] operation=queue_batch_plan allocating_ttl_seconds=%s "
                "mount_timeout_seconds=%s reason=%r",
                ttl,
                mount_timeout,
                "allocating TTL is not greater than compute mount timeout",
            )

        usable_ttl = max(1.0, ttl - max(2.0, worker_interval))
        waves = max(1, int(usable_ttl // mount_timeout))

        safe_ceiling = max(1, waves * configured_concurrency)
        effective_batch = min(configured_batch, safe_ceiling)
        effective_concurrency = min(configured_concurrency, effective_batch)
        return effective_batch, effective_concurrency

    def _drain_wait_queue_for_type(self, compute_type: str) -> Dict:
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        result = {
            "compute_type": compute_type_value,
            "stale_recovered": 0,
            "claimed": 0,
            "assigned": 0,
            "queued": 0,
            "failed": 0,
            "errors": [],
        }

        reserved_tickets: List[Dict] = []
        effective_batch, mount_concurrency = self._compute_wait_queue_batch_plan()
        lock_token = None
        try:
            lock_token = self.queues.acquire_allocator_lock(compute_type_value)
            if not lock_token:
                result["queued"] += 1
                result["lock_missed"] = True
                return result

            for stale_ticket in self.queues.find_stale_allocating_tickets(compute_type_value):
                recovered = self.recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    result["stale_recovered"] += 1
                elif recovered["status"] == "failed":
                    result["failed"] += 1
                elif recovered["status"] == "error":
                    result["errors"].append(recovered)

            if not self.queues.has_queued_tickets(compute_type_value):
                return result

            available = self.pool.get_available_pods(
                compute_type=compute_type_value,
                limit=effective_batch,
            )
            if not available:
                self._mark_compute_unavailable_started(compute_type_value)
                result["queued"] += 1
                return result

            reserved_pods: set = set()
            max_claims = min(effective_batch, len(available))
            for _ in range(max_claims):
                ticket = self.queues.claim_next_ticket(
                    compute_type_value,
                    worker_id=self.queues.worker_identity,
                )
                if not ticket:
                    break

                reservation = self._reserve_compute_pod_for_ticket(
                    ticket=ticket,
                    compute_type_value=compute_type_value,
                    candidate_pods=available,
                    reserved_pods=reserved_pods,
                    result=result,
                )
                if reservation:
                    reserved_tickets.append(reservation)
                    result["claimed"] += 1
                    continue
                break
        finally:
            if lock_token:
                self.queues.release_allocator_lock(compute_type_value, lock_token)

        if not reserved_tickets:
            return result

        executions: List[Dict] = []
        if mount_concurrency <= 1 or len(reserved_tickets) == 1:
            for ticket in reserved_tickets:
                executions.append(self._safe_execute_allocated_ticket(ticket, compute_type_value))
        else:
            with ThreadPoolExecutor(
                max_workers=mount_concurrency,
                thread_name_prefix="waitq-mount",
            ) as executor:
                futures = [
                    executor.submit(self._safe_execute_allocated_ticket, ticket, compute_type_value)
                    for ticket in reserved_tickets
                ]
                for future in as_completed(futures):
                    executions.append(future.result())

        for execution in executions:
            status = execution.get("status")
            if status == "assigned":
                result["assigned"] += 1
            elif status == "queued":
                result["queued"] += 1
            elif status == "failed":
                result["failed"] += 1
            else:
                result["errors"].append(execution)
        return result

    def _mark_compute_unavailable_started(self, compute_type_value: str) -> None:
        try:
            self.queues.mark_compute_unavailable_started(compute_type_value)
        except QueueUnavailableError as exc:
            logger.debug(
                "[ComputeUnavailableMarkSkipped] compute_type=%s reason=%r",
                compute_type_value,
                str(exc),
            )

    def _safe_execute_allocated_ticket(self, ticket: Dict, compute_type_value: str) -> Dict:
        try:
            return self._execute_allocated_ticket(ticket)
        except Exception as exc:
            logger.exception(
                "[Failed] operation=execute_allocated_ticket ticket_id=%s compute_type=%s reason=%r",
                ticket.get("ticket_id"),
                compute_type_value,
                str(exc),
            )
            return {
                "ticket_id": ticket.get("ticket_id", ""),
                "compute_type": compute_type_value,
                "status": "error",
                "message": str(exc),
            }

    def _reserve_compute_pod_for_ticket(
        self,
        ticket: Dict,
        compute_type_value: str,
        candidate_pods: List[str],
        reserved_pods: set,
        result: Dict,
    ) -> Optional[Dict]:
        ticket_id = ticket["ticket_id"]
        claim_token = ticket.get("claim_token")
        user_pod_identity = ticket.get("user_pod") or "unknown"

        compute_pod = ""
        for candidate in candidate_pods:
            if candidate in reserved_pods:
                continue
            try:
                self.pool.assign_pod(candidate, user_pod_identity)
            except PodConflictError as exc:
                ticket_format.log_queue_event(
                    "debug",
                    "ComputeContention",
                    ticket,
                    include_ticket_fields=(),
                    compute_pod=candidate,
                    reason=f"Compute contention: {exc}",
                )
                continue
            compute_pod = candidate
            reserved_pods.add(compute_pod)
            break

        if not compute_pod:
            self.tickets.requeue_ticket(
                ticket_id,
                reason="No available compute pods",
                increment_retry=False,
                claim_token=claim_token,
            )
            result["queued"] += 1
            return None

        compute_ready_at = self.pool.get_pod_ready_at(compute_pod)
        compute_available_at = self._pop_compute_available_at(compute_pod)
        compute_pod_ip = self.pool.get_pod_ip(compute_pod) or ""
        if not compute_pod_ip:
            try:
                self.pool.release_pod(compute_pod)
            except Exception:
                pass
            reserved_pods.discard(compute_pod)
            self.tickets.requeue_ticket(
                ticket_id,
                reason="Compute IP unavailable",
                increment_retry=False,
                claim_token=claim_token,
            )
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Compute IP unavailable",
            )
            result["queued"] += 1
            return None

        committed = self.tickets.mark_allocating(
            ticket_id,
            compute_pod=compute_pod,
            compute_pod_ip=compute_pod_ip,
            claimed_by=ticket.get("claimed_by"),
            claim_token=claim_token,
            compute_ready_at=compute_ready_at,
            compute_available_at=compute_available_at,
        )
        if not committed or committed.get("status") != "allocating":
            try:
                self.pool.release_pod(compute_pod)
            except Exception:
                pass
            reserved_pods.discard(compute_pod)
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                committed or ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Ticket lost ownership before compute commit",
            )
            result["errors"].append(
                {
                    "ticket_id": ticket_id,
                    "compute_type": compute_type_value,
                    "error": "Ticket lost ownership before compute commit",
                }
            )
            return None

        return committed

    def _execute_allocated_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
        compute_pod = ticket.get("compute_pod", "")
        compute_pod_ip = ticket.get("compute_pod_ip", "")
        user_pod = ticket.get("user_pod") or ""
        user_pod_ip = ticket.get("user_pod_ip") or ""
        command = ticket.get("command") or ""
        claim_token = ticket.get("claim_token") or ""

        if not compute_pod or not user_pod_ip:
            failed = self.tickets.mark_failed(ticket_id, "Missing compute pod or user pod context", claim_token=claim_token)
            ticket_format.log_queue_event(
                "info",
                "Failed",
                failed or ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                retry_count=(failed or ticket).get("retry_count"),
                reason="Missing compute pod or user pod context",
            )
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "failed",
                "message": "Missing compute pod or user pod context",
            }

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(compute_pod)
            except Exception:
                pass
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Ticket no longer owns the allocation",
            )
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "queued",
                "message": "Ticket no longer owns the allocation",
            }

        try:
            with ComputeAgent(compute_pod_ip) as agent:
                agent.mount(user_pod_ip, command, user_pod)
        except ComputeAgentError as exc:
            ticket_format.log_queue_event(
                "warning",
                "Warning",
                ticket,
                include_ticket_fields=(),
                operation="mount",
                compute_pod=compute_pod,
                reason=str(exc),
            )
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                compute_pod=compute_pod,
                claim_token=claim_token,
                exc=exc,
            )
        except Exception as exc:
            ticket_format.set_ticket_context(ticket)
            logger.exception(
                "[Failed] operation=mount error_type=unexpected compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                compute_pod=compute_pod,
                claim_token=claim_token,
                exc=exc,
            )

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(compute_pod)
            except Exception:
                pass
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "queued",
                "message": "Ticket was cancelled before commit",
            }

        committed = self.tickets.mark_assigned(ticket_id, compute_pod, compute_pod_ip, claim_token=claim_token)
        if committed and committed.get("status") == "assigned":
            assigned_context = ticket_format.assigned_request_context(committed)
            self.tickets.set_assigned_request_context(compute_pod, assigned_context)
            ticket_format.log_queue_event(
                "info",
                "Assigned",
                committed,
                include_ticket_fields=(),
                claimed_by=committed.get("claimed_by"),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                retry_count=committed.get("retry_count"),
                **ticket_format.assignment_timing_fields(committed),
            )
            response = ticket_format.ticket_response(committed, "Command assigned to compute pod")
            response.update(
                {
                    "user_pod": user_pod,
                    "user_pod_ip": user_pod_ip,
                    "command": command,
                    "submitted_at": datetime.now().isoformat(),
                }
            )
            return response

        try:
            self.pool.release_pod(compute_pod)
        except Exception:
            pass
        current = self.tickets.get_ticket(ticket_id)
        if current:
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                current,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Ticket no longer owns the allocation",
            )
        return ticket_format.ticket_response(current or ticket, "Ticket no longer owns the allocation")

    def _handle_mount_failure(
        self,
        ticket_id: str,
        compute_pod: str,
        claim_token: str,
        exc: Exception,
    ) -> Dict:
        reason = f"Mount failed: {exc}"
        try:
            self.pool.release_pod(compute_pod)
        except Exception as release_exc:
            logger.warning(
                "[Warning] operation=release_after_mount_error ticket_id=%s compute_pod=%s reason=%r",
                ticket_id,
                compute_pod,
                str(release_exc),
            )

        current = self.tickets.get_ticket(ticket_id)
        if current and int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.queues.max_retries):
            requeued = self.tickets.requeue_ticket(
                ticket_id,
                reason=reason,
                increment_retry=True,
                claim_token=claim_token,
            )
            requeued_ticket = requeued if requeued and requeued.get("status") == "queued" else self.tickets.get_ticket(ticket_id)
            if requeued_ticket:
                ticket_format.log_queue_event(
                    "debug",
                    "Requeued",
                    requeued_ticket,
                    include_ticket_fields=(),
                    compute_pod=compute_pod,
                    compute_pod_ip=requeued_ticket.get("compute_pod_ip"),
                    retry_count=requeued_ticket.get("retry_count"),
                    reason=reason,
                )
            return ticket_format.ticket_response(requeued_ticket, str(exc))

        failed = self.tickets.mark_failed(ticket_id, reason, claim_token=claim_token)
        failed_ticket = failed if failed and failed.get("status") == "failed" else self.tickets.get_ticket(ticket_id)
        if failed_ticket:
            ticket_format.log_queue_event(
                "info",
                "Failed",
                failed_ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=failed_ticket.get("compute_pod_ip"),
                retry_count=failed_ticket.get("retry_count"),
                reason=reason,
            )
        return ticket_format.ticket_response(failed_ticket, str(exc))

    def _select_compute_pod(self, compute_type: str) -> Dict:
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        compute_pod = self.pool.get_available_pod(compute_type_value)
        if not compute_pod:
            return {}
        return {
            "name": compute_pod,
            "ip": self.pool.get_pod_ip(compute_pod) or "",
        }

    def _discover_compute_types(self) -> List[str]:
        self.refresh_compute_types()
        return sorted(self.queues.known_compute_types())

    def _validate_compute_type_for_enqueue(self, compute_type: Optional[str]) -> str:
        try:
            return self.queues.validate_compute_type(compute_type, self.queues.known_compute_types())
        except ValueError:
            self.refresh_compute_types(force=True)
            return self.queues.validate_compute_type(compute_type, self.queues.known_compute_types())

    def _ticket_for_compute_pod(self, compute_pod: str, compute_type: Optional[str] = None) -> Optional[Dict]:
        try:
            ticket = self.tickets.find_ticket_by_compute_pod_index_only(
                compute_pod,
                compute_type=compute_type,
            )
        except QueueUnavailableError:
            return None
        return ticket

    def _ticket_for_compute_pod_with_context(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Optional[Dict]:
        ticket_id = ""
        compute_type = None
        if request_context:
            ticket_id = (request_context.get("ticket_id") or "").strip()
            compute_type = request_context.get("compute_type") or None

        if ticket_id:
            try:
                ticket = self.tickets.get_ticket_snapshot(ticket_id)
            except QueueUnavailableError as exc:
                logger.debug(
                    "[TicketLookupSkipped] ticket_id=%s compute_pod=%s reason=%r",
                    ticket_id,
                    compute_pod,
                    str(exc),
                )
            else:
                if ticket:
                    assigned_compute = (ticket.get("compute_pod") or "").strip()
                    if assigned_compute == compute_pod and str(ticket.get("status") or "").lower() == "assigned":
                        return ticket

        return self._ticket_for_compute_pod(compute_pod, compute_type=compute_type)

    def _release_pod_best_effort(self, compute_pod: str, ticket_id: str) -> None:
        if not compute_pod:
            return
        try:
            self.pool.release_pod(compute_pod)
        except Exception as exc:
            logger.warning(
                "[Warning] operation=release_stale_compute compute_pod=%s ticket_id=%s reason=%r",
                compute_pod,
                ticket_id,
                str(exc),
            )

    def _clear_assigned_request_context_best_effort(self, compute_pod: str) -> None:
        try:
            self.tickets.clear_assigned_request_context(compute_pod)
        except QueueUnavailableError as exc:
            logger.warning(
                "[Warning] operation=assigned_context_cleanup compute_pod=%s status=skipped reason=%r",
                compute_pod,
                str(exc),
            )
        except Exception as exc:
            logger.warning(
                "[Warning] operation=assigned_context_cleanup compute_pod=%s status=failed reason=%r",
                compute_pod,
                str(exc),
            )
