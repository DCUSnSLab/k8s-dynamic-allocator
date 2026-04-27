import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from kubernetes.client.rest import ApiException

from config import settings
from config.settings import get_request_label, set_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError, safe_int
from .agent_client import BackendAgent, BackendAgentError
from .pool import PodConflictError

logger = logging.getLogger(__name__)


class BackendSessions:
    _RELEASE_MAX_ATTEMPTS = 2

    def __init__(self, pool, queues, tickets):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self._backend_refresh_lock = threading.Lock()
        self._backend_refresh_interval = max(settings.WAIT_QUEUE_BACKEND_REFRESH_SECONDS, 1.0)
        self._last_backend_refresh = 0.0
        self._queue_kick_lock = threading.Lock()
        self._queue_kick_in_flight = False

    def execute_command(
        self,
        username: str,
        command: str,
        frontend_ip: str,
        frontend_pod: str = "",
        backend_type: Optional[str] = None,
        ingress_ts_ms: Optional[int] = None,
    ) -> Dict:
        try:
            if not frontend_ip:
                return {
                    "status": "error",
                    "message": "frontend_ip is required",
                }

            backend_type_value = self.queues.validate_backend_type(
                backend_type,
                self._discover_backend_types(),
            )
            frontend_pod_value = frontend_pod or ""
            ticket = self.tickets.create_ticket(
                username=username,
                command=command,
                frontend_pod=frontend_pod_value,
                frontend_ip=frontend_ip,
                backend_type=backend_type_value,
                request_id=get_request_label(),
                ingress_ts_ms=ingress_ts_ms,
            )

            ticket_format.set_ticket_context(ticket)
            ticket_format.log_queue_event(
                "info",
                "Enqueued",
                ticket=ticket,
                queue_position=ticket.get("queue_position"),
                ingress_ts_ms=ticket.get("ingress_ts_ms"),
            )

            self._kick_wait_queue_worker()
            current_ticket = self.tickets.get_ticket(ticket["ticket_id"])
            response = ticket_format.ticket_response(current_ticket or ticket)
            if response["status"] == "assigned":
                response["message"] = "Command assigned to backend"
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
                "error_type": "invalid_backend_type",
            }
        except QueueUnavailableError as exc:
            logger.error("Queue unavailable during execute: %s", exc)
            return {
                "status": "error",
                "message": str(exc),
            }
        except ApiException as exc:
            logger.error("[FAILED] Execute: K8s API error (%s %s)", exc.status, exc.reason)
            return {
                "status": "error",
                "message": f"K8s API error: {exc.reason}",
            }
        except Exception as exc:
            logger.error("[FAILED] Execute: Unexpected error (%s)", exc)
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
                    reason=reason or final_ticket.get("error") or "user_cancel",
                    queue_wait_ms=queue_wait_ms,
                )
            elif final_ticket:
                logger.debug(
                    "Cancel no-op for ticket %s: %s",
                    ticket_id,
                    reason or final_ticket.get("error") or "cancel no-op",
                )
            backend_pod = final_ticket.get("backend_pod") or ""
            if backend_pod and final_ticket.get("status") in {"cancelled", "failed"}:
                try:
                    self.pool.release_pod(backend_pod)
                except Exception as exc:
                    logger.warning("Failed to release backend pod %s after cancel: %s", backend_pod, exc)
                self._clear_assigned_request_context_best_effort(backend_pod)

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

    def release_backend(self, backend_pod: str, request_context: Optional[Dict[str, object]] = None) -> Dict:
        release_started_ms = int(time.time() * 1000)
        request_context_value = dict(request_context or {})

        last_error: Optional[Exception] = None
        cleanup_context = False
        backend_unmounted = False

        def _release_once() -> Dict:
            nonlocal cleanup_context, backend_unmounted
            release_started = time.perf_counter()
            frontend = "unknown"
            backend_type = self.queues.default_backend_type
            try:
                pod = self.pool.v1.read_namespaced_pod(backend_pod, self.pool.namespace)
                labels = pod.metadata.labels or {}
                frontend = labels.get(self.pool.LABEL_FRONTEND, "unknown")
                backend_type = self.queues.normalize_backend_type(labels.get(self.pool.LABEL_BACKEND_TYPE))
            except ApiException as exc:
                if exc.status != 404:
                    raise
                logger.debug("Backend pod already released: %s", backend_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Backend already released: {backend_pod}",
                }

            assigned_context = dict(request_context_value)
            if not assigned_context:
                try:
                    assigned_context = self.tickets.get_assigned_request_context(backend_pod) or {}
                except QueueUnavailableError as exc:
                    logger.debug("Assigned request context unavailable for %s: %s", backend_pod, exc)

            if assigned_context.get("request_label"):
                set_request_label(assigned_context.get("request_label"))

            ticket = self._ticket_for_backend_pod_with_context(backend_pod, assigned_context)
            if ticket:
                ticket_format.set_ticket_context(ticket)
                frontend = ticket.get("frontend_pod") or frontend
                backend_type = self.queues.normalize_backend_type(ticket.get("backend_type"))
            elif assigned_context:
                frontend = assigned_context.get("frontend_pod") or frontend
                backend_type = self.queues.normalize_backend_type(
                    assigned_context.get("backend_type") or backend_type
                )

            backend_ip = self.pool.get_pod_ip(backend_pod)
            if backend_ip and not backend_unmounted:
                with BackendAgent(backend_ip) as agent:
                    agent.unmount()
                    backend_unmounted = True

            released_now = self.pool.release_pod(backend_pod)
            if not released_now:
                logger.debug("Backend pod already released or terminating during release: %s", backend_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Backend already released: {backend_pod}",
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
                frontend_pod=frontend,
                backend_pod=backend_pod,
                backend_ip=backend_ip or "",
                backend_type=backend_type,
                session_ms=session_ms,
                release_ms=release_ms,
            )
            cleanup_context = True
            return {
                "status": "success",
                "message": f"Released: {backend_pod}",
            }

        try:
            for attempt in range(1, self._RELEASE_MAX_ATTEMPTS + 1):
                try:
                    response = _release_once()
                    if response.get("status") == "success":
                        self._kick_wait_queue_worker()
                    return response
                except Exception as exc:
                    last_error = exc
                    if attempt < self._RELEASE_MAX_ATTEMPTS:
                        logger.debug(
                            "Release retry %s/%s for %s: %s",
                            attempt,
                            self._RELEASE_MAX_ATTEMPTS,
                            backend_pod,
                            exc,
                        )
                        continue
                    logger.error("[FAILED] Release: %s (%s)", backend_pod, exc)
                    return {
                        "status": "error",
                        "message": str(exc),
                    }
        finally:
            if cleanup_context:
                self._clear_assigned_request_context_best_effort(backend_pod)

        return {
            "status": "error",
            "message": str(last_error) if last_error else "Unknown release failure",
        }

    def process_wait_queues(self) -> Dict:
        try:
            backend_types = set(self._discover_backend_types())
            processed = []
            for backend_type in sorted(backend_types):
                try:
                    processed.append(self._drain_wait_queue_for_type(backend_type))
                except Exception as exc:
                    logger.exception("Failed to drain wait queue for %s", backend_type)
                    processed.append(
                        {
                            "backend_type": backend_type,
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

    def get_assigned_request_context(self, backend_pod: str) -> Optional[Dict[str, object]]:
        return self.tickets.get_assigned_request_context(backend_pod)

    def refresh_backend_types(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_backend_refresh) < self._backend_refresh_interval:
            return

        with self._backend_refresh_lock:
            now = time.monotonic()
            if not force and (now - self._last_backend_refresh) < self._backend_refresh_interval:
                return

            self._last_backend_refresh = now
            backend_types = set()
            try:
                self.pool.initialize_pool(log_existing=False)
            except Exception as exc:
                logger.debug("Backend manifest refresh skipped: %s", exc)
            backend_types.update(self.queues.known_backend_types())
            for item in self.pool.list_pool_status():
                if item.get("backend_type"):
                    backend_types.add(self.queues.normalize_backend_type(item.get("backend_type")))

            if backend_types:
                self.queues.register_backend_types(sorted(backend_types))

    def recover_stale_ticket(self, ticket: Dict) -> Dict:
        """Public - shared with BackendCleanup."""
        ticket_id = ticket.get("ticket_id", "")
        backend_type = self.queues.normalize_backend_type(ticket.get("backend_type"))
        backend_pod = ticket.get("backend_pod", "")
        claim_token = ticket.get("claim_token") or None
        retry_count = int(ticket.get("retry_count") or 0)
        max_retries = int(ticket.get("max_retries") or self.queues.max_retries)
        skipped = {"ticket_id": ticket_id, "backend_type": backend_type, "status": "skipped"}

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
                    self._release_pod_best_effort(backend_pod, ticket_id)
                    ticket_format.log_queue_event("debug", "stale_recovered", recovered, reason="stale allocation recovered")
                    return {"ticket_id": ticket_id, "backend_type": backend_type, "status": "requeued"}
                return skipped

            failed = self.tickets.mark_failed(
                ticket_id,
                "stale allocation exceeded retry budget",
                claim_token=claim_token,
            )
            if failed and failed.get("status") == "failed":
                self._release_pod_best_effort(backend_pod, ticket_id)
                ticket_format.log_queue_event("debug", "ticket_failed", failed, reason="stale allocation exceeded retry budget")
                return {"ticket_id": ticket_id, "backend_type": backend_type, "status": "failed"}
            return skipped
        except QueueUnavailableError as exc:
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "error",
                "error": str(exc),
            }

    def _kick_wait_queue_worker(self) -> None:
        with self._queue_kick_lock:
            if self._queue_kick_in_flight:
                return
            self._queue_kick_in_flight = True

        def _runner():
            try:
                self.process_wait_queues()
            finally:
                with self._queue_kick_lock:
                    self._queue_kick_in_flight = False

        threading.Thread(
            target=_runner,
            name="queue-kick",
            daemon=True,
        ).start()

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

        mount_timeout = max(1.0, float(settings.BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS))
        ttl = max(1.0, float(settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS))
        worker_interval = max(0.0, float(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS))

        if ttl <= mount_timeout:
            logger.warning(
                "WAIT_QUEUE_ALLOCATING_TTL_SECONDS (%s) <= "
                "BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS (%s); "
                "even a single mount can be flagged stale before completion",
                ttl,
                mount_timeout,
            )

        usable_ttl = max(1.0, ttl - max(2.0, worker_interval))
        waves = max(1, int(usable_ttl // mount_timeout))

        safe_ceiling = max(1, waves * configured_concurrency)
        effective_batch = min(configured_batch, safe_ceiling)
        effective_concurrency = min(configured_concurrency, effective_batch)
        return effective_batch, effective_concurrency

    def _drain_wait_queue_for_type(self, backend_type: str) -> Dict:
        backend_type_value = self.queues.normalize_backend_type(backend_type)
        result = {
            "backend_type": backend_type_value,
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
            lock_token = self.queues.acquire_allocator_lock(backend_type_value)
            if not lock_token:
                result["queued"] += 1
                return result

            for stale_ticket in self.queues.find_stale_allocating_tickets(backend_type_value):
                recovered = self.recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    result["stale_recovered"] += 1
                elif recovered["status"] == "failed":
                    result["failed"] += 1
                elif recovered["status"] == "error":
                    result["errors"].append(recovered)

            reserved_pods: set = set()

            for _ in range(effective_batch):
                available = self.pool.get_available_pods(
                    backend_type=backend_type_value,
                    exclude=reserved_pods,
                    limit=1,
                )
                if not available:
                    break

                ticket = self.queues.claim_next_ticket(
                    backend_type_value,
                    worker_id=self.queues.worker_identity,
                )
                if not ticket:
                    break

                reservation = self._reserve_backend_for_ticket(
                    ticket=ticket,
                    backend_type_value=backend_type_value,
                    candidate_pods=available,
                    reserved_pods=reserved_pods,
                    result=result,
                )
                if reservation:
                    reserved_tickets.append(reservation)
                    result["claimed"] += 1
        finally:
            if lock_token:
                self.queues.release_allocator_lock(backend_type_value, lock_token)

        if not reserved_tickets:
            return result

        executions: List[Dict] = []
        if mount_concurrency <= 1 or len(reserved_tickets) == 1:
            for ticket in reserved_tickets:
                executions.append(self._safe_execute_allocated_ticket(ticket, backend_type_value))
        else:
            with ThreadPoolExecutor(
                max_workers=mount_concurrency,
                thread_name_prefix="waitq-mount",
            ) as executor:
                futures = [
                    executor.submit(self._safe_execute_allocated_ticket, ticket, backend_type_value)
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

    def _safe_execute_allocated_ticket(self, ticket: Dict, backend_type_value: str) -> Dict:
        try:
            return self._execute_allocated_ticket(ticket)
        except Exception as exc:
            logger.exception(
                "Unexpected execution failure for ticket %s",
                ticket.get("ticket_id"),
            )
            return {
                "ticket_id": ticket.get("ticket_id", ""),
                "backend_type": backend_type_value,
                "status": "error",
                "message": str(exc),
            }

    def _reserve_backend_for_ticket(
        self,
        ticket: Dict,
        backend_type_value: str,
        candidate_pods: List[str],
        reserved_pods: set,
        result: Dict,
    ) -> Optional[Dict]:
        ticket_id = ticket["ticket_id"]
        claim_token = ticket.get("claim_token")
        frontend_identity = ticket.get("frontend_pod") or "unknown"

        backend_pod = ""
        for candidate in candidate_pods:
            if candidate in reserved_pods:
                continue
            try:
                self.pool.assign_pod(candidate, frontend_identity)
            except PodConflictError as exc:
                ticket_format.log_queue_event(
                    "debug",
                    "backend_contention_skip",
                    ticket,
                    backend_pod=candidate,
                    reason=f"Backend contention: {exc}",
                )
                continue
            backend_pod = candidate
            reserved_pods.add(backend_pod)
            break

        if not backend_pod:
            self.tickets.requeue_ticket(
                ticket_id,
                reason="No available backend pods",
                increment_retry=False,
                claim_token=claim_token,
            )
            result["queued"] += 1
            return None

        backend_ip = self.pool.get_pod_ip(backend_pod) or ""
        if not backend_ip:
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            reserved_pods.discard(backend_pod)
            self.tickets.requeue_ticket(
                ticket_id,
                reason="Backend IP unavailable",
                increment_retry=False,
                claim_token=claim_token,
            )
            ticket_format.log_queue_event(
                "debug",
                "ticket_requeued",
                ticket,
                backend_pod=backend_pod,
                reason="Backend IP unavailable",
            )
            result["queued"] += 1
            return None

        committed = self.tickets.mark_allocating(
            ticket_id,
            backend_pod=backend_pod,
            backend_ip=backend_ip,
            claimed_by=ticket.get("claimed_by"),
            claim_token=claim_token,
        )
        if not committed or committed.get("status") != "allocating":
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            reserved_pods.discard(backend_pod)
            ticket_format.log_queue_event(
                "debug",
                "ticket_requeued",
                committed or ticket,
                backend_pod=backend_pod,
                reason="Ticket lost ownership before backend commit",
            )
            result["errors"].append(
                {
                    "ticket_id": ticket_id,
                    "backend_type": backend_type_value,
                    "error": "Ticket lost ownership before backend commit",
                }
            )
            return None

        return committed

    def _execute_allocated_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        backend_type = self.queues.normalize_backend_type(ticket.get("backend_type"))
        backend_pod = ticket.get("backend_pod", "")
        backend_ip = ticket.get("backend_ip", "")
        frontend_pod = ticket.get("frontend_pod") or ""
        frontend_ip = ticket.get("frontend_ip") or ""
        command = ticket.get("command") or ""
        claim_token = ticket.get("claim_token") or ""

        if not backend_pod or not frontend_ip:
            self.tickets.mark_failed(ticket_id, "Missing backend or frontend context", claim_token=claim_token)
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "failed",
                "message": "Missing backend or frontend context",
            }

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            ticket_format.log_queue_event("debug", "ticket_requeued", ticket, reason="Ticket no longer owns the allocation")
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "queued",
                "message": "Ticket no longer owns the allocation",
            }

        try:
            with BackendAgent(backend_ip) as agent:
                agent.mount(frontend_ip, command, frontend_pod)
        except BackendAgentError as exc:
            logger.warning("Mount failed for ticket %s on %s: %s", ticket_id, backend_pod, exc)
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                backend_pod=backend_pod,
                claim_token=claim_token,
                exc=exc,
            )
        except Exception as exc:
            logger.exception("Unexpected mount failure for ticket %s on %s", ticket_id, backend_pod)
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                backend_pod=backend_pod,
                claim_token=claim_token,
                exc=exc,
            )

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "queued",
                "message": "Ticket was cancelled before commit",
            }

        committed = self.tickets.mark_assigned(ticket_id, backend_pod, backend_ip, claim_token=claim_token)
        if committed and committed.get("status") == "assigned":
            assigned_context = ticket_format.assigned_request_context(committed)
            self.tickets.set_assigned_request_context(backend_pod, assigned_context)
            ticket_format.log_queue_event(
                "info",
                "Assigned",
                committed,
                backend_pod=backend_pod,
                backend_ip=backend_ip,
                **ticket_format.assignment_timing_fields(committed),
            )
            response = ticket_format.ticket_response(committed, "Command assigned to backend")
            response.update(
                {
                    "frontend_pod": frontend_pod,
                    "frontend_ip": frontend_ip,
                    "command": command,
                    "submitted_at": datetime.now().isoformat(),
                }
            )
            return response

        try:
            self.pool.release_pod(backend_pod)
        except Exception:
            pass
        current = self.tickets.get_ticket(ticket_id)
        if current:
            ticket_format.log_queue_event("debug", "ticket_requeued", current, reason="Ticket no longer owns the allocation")
        return ticket_format.ticket_response(current or ticket, "Ticket no longer owns the allocation")

    def _handle_mount_failure(
        self,
        ticket_id: str,
        backend_pod: str,
        claim_token: str,
        exc: Exception,
    ) -> Dict:
        reason = f"Mount failed: {exc}"
        try:
            self.pool.release_pod(backend_pod)
        except Exception as release_exc:
            logger.warning(
                "Failed to release backend pod %s after mount error: %s",
                backend_pod,
                release_exc,
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
                ticket_format.log_queue_event("debug", "ticket_requeued", requeued_ticket, reason=reason)
            return ticket_format.ticket_response(requeued_ticket, str(exc))

        failed = self.tickets.mark_failed(ticket_id, reason, claim_token=claim_token)
        failed_ticket = failed if failed and failed.get("status") == "failed" else self.tickets.get_ticket(ticket_id)
        if failed_ticket:
            ticket_format.log_queue_event("info", "Failed", failed_ticket, reason=reason)
        return ticket_format.ticket_response(failed_ticket, str(exc))

    def _select_backend_pod(self, backend_type: str) -> Dict:
        backend_type_value = self.queues.normalize_backend_type(backend_type)
        backend_pod = self.pool.get_available_pod(backend_type_value)
        if not backend_pod:
            return {}
        return {
            "name": backend_pod,
            "ip": self.pool.get_pod_ip(backend_pod) or "",
        }

    def _discover_backend_types(self) -> List[str]:
        self.refresh_backend_types()
        return sorted(self.queues.known_backend_types())

    def _ticket_for_backend_pod(self, backend_pod: str, backend_type: Optional[str] = None) -> Optional[Dict]:
        try:
            ticket = self.tickets.find_ticket_by_backend_pod_index_only(
                backend_pod,
                backend_type=backend_type,
            )
        except QueueUnavailableError:
            return None
        return ticket

    def _ticket_for_backend_pod_with_context(
        self,
        backend_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Optional[Dict]:
        ticket_id = ""
        backend_type = None
        if request_context:
            ticket_id = (request_context.get("ticket_id") or "").strip()
            backend_type = request_context.get("backend_type") or None

        if ticket_id:
            try:
                ticket = self.tickets.get_ticket_snapshot(ticket_id)
            except QueueUnavailableError as exc:
                logger.debug("Ticket lookup by id failed for %s: %s", ticket_id, exc)
            else:
                if ticket:
                    assigned_backend = (ticket.get("backend_pod") or "").strip()
                    if assigned_backend == backend_pod and str(ticket.get("status") or "").lower() == "assigned":
                        return ticket

        return self._ticket_for_backend_pod(backend_pod, backend_type=backend_type)

    def _release_pod_best_effort(self, backend_pod: str, ticket_id: str) -> None:
        if not backend_pod:
            return
        try:
            self.pool.release_pod(backend_pod)
        except Exception as exc:
            logger.warning(
                "Failed to release stale backend %s for ticket %s: %s",
                backend_pod,
                ticket_id,
                exc,
            )

    def _clear_assigned_request_context_best_effort(self, backend_pod: str) -> None:
        try:
            self.tickets.clear_assigned_request_context(backend_pod)
        except QueueUnavailableError as exc:
            logger.warning("Assigned request context cleanup skipped for %s: %s", backend_pod, exc)
        except Exception as exc:
            logger.warning("Assigned request context cleanup failed for %s: %s", backend_pod, exc)
