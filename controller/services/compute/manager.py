from typing import Dict, Optional

from kubernetes.client.rest import ApiException

from config.settings import get_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError
from .allocator import ComputeAllocator
from .queue_processor import ComputeQueueProcessor
from .releaser import ComputeReleaser


class ComputeManager:
    """Public facade for compute-pod allocation, release, and queue dispatch."""

    def __init__(self, pool, queues, tickets):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self.allocator = ComputeAllocator(pool, queues, tickets)
        self.releaser = ComputeReleaser(pool, queues, tickets)
        self.queue_processor = ComputeQueueProcessor(
            pool,
            queues,
            tickets,
            allocator=self.allocator,
            release_pod_best_effort=self.releaser.release_pod_best_effort,
        )
        self.releaser.set_release_callback(self.queue_processor.kick_wait_queue_worker)

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

            self.queue_processor.kick_wait_queue_worker(compute_type_value)
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
                self.releaser.clear_assigned_request_context_best_effort(compute_pod)

            if str(final_ticket.get("status") or "").lower() == "cancelled":
                self.queue_processor.kick_wait_queue_worker(final_ticket.get("compute_type"))

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

    def release_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Dict:
        return self.releaser.release_compute_pod(
            compute_pod=compute_pod,
            request_context=request_context,
        )

    def process_wait_queues(self, compute_type: Optional[str] = None) -> Dict:
        return self.queue_processor.process_wait_queues(compute_type=compute_type)

    def notify_compute_available(
        self,
        compute_type: Optional[str],
        compute_pod: str = "",
        compute_available_at: str = "",
        source: str = "watch",
    ) -> None:
        self.queue_processor.notify_compute_available(
            compute_type=compute_type,
            compute_pod=compute_pod,
            compute_available_at=compute_available_at,
            source=source,
        )

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.releaser.get_assigned_request_context(compute_pod)

    def refresh_compute_types(self, force: bool = False) -> None:
        self.queue_processor.refresh_compute_types(force=force)

    def recover_stale_ticket(self, ticket: Dict) -> Dict:
        return self.queue_processor.recover_stale_ticket(ticket)

    def _validate_compute_type_for_enqueue(self, compute_type: Optional[str]) -> str:
        try:
            return self.queues.validate_compute_type(compute_type, self.queues.known_compute_types())
        except ValueError:
            self.refresh_compute_types(force=True)
            return self.queues.validate_compute_type(compute_type, self.queues.known_compute_types())
