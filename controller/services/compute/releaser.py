import logging
import time
from typing import Callable, Dict, Optional

from kubernetes.client.rest import ApiException

from config.settings import set_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError, safe_int
from .agent_client import ComputeAgent

logger = logging.getLogger(__name__)


class ComputeReleaser:
    _RELEASE_MAX_ATTEMPTS = 2

    def __init__(
        self,
        pool,
        queues,
        tickets,
        on_released: Optional[Callable[[Optional[str]], None]] = None,
    ):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self._on_released = on_released or (lambda compute_type: None)

    def set_release_callback(self, on_released: Callable[[Optional[str]], None]) -> None:
        self._on_released = on_released

    def release_compute_pod(self, compute_pod: str, request_context: Optional[Dict[str, object]] = None) -> Dict:
        release_started_ms = int(time.time() * 1000)
        request_context_value = dict(request_context or {})

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
                        self._on_released(released_compute_type)
                    return response
                except Exception as exc:
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
                self.clear_assigned_request_context_best_effort(compute_pod)

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.tickets.get_assigned_request_context(compute_pod)

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

    def release_pod_best_effort(self, compute_pod: str, ticket_id: str) -> None:
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

    def clear_assigned_request_context_best_effort(self, compute_pod: str) -> None:
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
