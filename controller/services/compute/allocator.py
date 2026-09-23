import logging
import threading
from datetime import datetime
from typing import Callable, Dict, List, Tuple

from config import settings

from .. import ticket_format
from .agent_client import ComputeAgent, ComputeAgentError
from .cold_start_provider import AllocationClaimLost

logger = logging.getLogger(__name__)


class ComputeAllocator:
    def __init__(self, provider, queues, tickets):
        self.provider = provider
        self.queues = queues
        self.tickets = tickets

    def drain_wait_queue_for_type(
        self,
        compute_type: str,
        recover_stale_ticket: Callable[[Dict], Dict],
    ) -> Dict:
        # Cold start arm: one pod per ticket, created when the ticket is claimed.
        # develop dispatches here on the provider's mode; this branch has only
        # the one path, so it is called directly.
        return self._drain_wait_queue_for_type_cold_start(
            compute_type,
            recover_stale_ticket,
        )

    def _drain_wait_queue_for_type_cold_start(
        self,
        compute_type: str,
        recover_stale_ticket: Callable[[Dict], Dict],
    ) -> Dict:
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

        claimed_tickets: List[Dict] = []
        effective_batch, _mount_concurrency = self._compute_wait_queue_batch_plan()
        lock_token = None
        try:
            lock_token = self.queues.acquire_allocator_lock(compute_type_value)
            if not lock_token:
                result["queued"] += 1
                result["lock_missed"] = True
                return result

            for stale_ticket in self.queues.find_stale_allocating_tickets(compute_type_value):
                recovered = recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    result["stale_recovered"] += 1
                elif recovered["status"] == "failed":
                    result["failed"] += 1
                elif recovered["status"] == "error":
                    result["errors"].append(recovered)

            if not self.queues.has_queued_tickets(compute_type_value):
                return result

            claim_limit = self._cold_start_claim_limit(
                compute_type_value,
                effective_batch,
                result,
            )
            for _ in range(claim_limit):
                ticket = self.queues.claim_next_ticket(
                    compute_type_value,
                    worker_id=self.queues.worker_identity,
                )
                if not ticket:
                    break
                claimed_tickets.append(ticket)
                result["claimed"] += 1
        finally:
            if lock_token:
                self.queues.release_allocator_lock(compute_type_value, lock_token)

        for ticket in claimed_tickets:
            threading.Thread(
                target=self._safe_execute_cold_start_ticket,
                args=(ticket, compute_type_value),
                name=f"cold-start-{str(ticket.get('ticket_short') or ticket.get('ticket_id') or '')[:10]}",
                daemon=True,
            ).start()

        if not claimed_tickets:
            result["queued"] += 1
        return result

    def _cold_start_claim_limit(
        self,
        compute_type: str,
        effective_batch: int,
        result: Dict,
    ) -> int:
        """How many tickets may be claimed now without going over N.

        Tickets over the cap stay queued and keep their position, the same way
        the warm buffer holds them, so both allocation modes face one capacity
        and their wait times can be compared.
        """
        try:
            capacity = self.provider.read_capacity(compute_type)
            if capacity is None:
                return effective_batch
            active = self.provider.count_active_pods(compute_type)
        except Exception as exc:
            # Reading capacity is a Kubernetes call; a blip must not stall the
            # queue, and the per-ticket create still fails safely on its own.
            logger.warning(
                "[Warning] operation=cold_start_capacity_check compute_type=%s reason=%r",
                compute_type,
                str(exc),
            )
            return effective_batch

        remaining = max(0, capacity - active)
        if remaining == 0:
            result["capacity_blocked"] = "capacity"
            if active == 0:
                # Being at the cap is ordinary backpressure, but only while
                # something is running that can finish and free a slot. With
                # nothing active the cap itself is the blocker and the queue
                # will never drain - tickets just accumulate with no error
                # anywhere. That needs a line in the log, because the symptom
                # is otherwise silence.
                logger.warning(
                    "[Warning] operation=cold_start_capacity_deadlock "
                    "compute_type=%s capacity=%s reason=%r",
                    compute_type,
                    capacity,
                    "capacity is not positive, so no ticket can ever be served",
                )
        return min(effective_batch, remaining)

    def _compute_wait_queue_batch_plan(self) -> Tuple[int, int]:
        """Return (effective_batch, mount_concurrency) clamped against TTL."""
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

    def _safe_execute_cold_start_ticket(self, ticket: Dict, compute_type_value: str) -> Dict:
        try:
            return self._execute_cold_start_ticket(ticket)
        except Exception as exc:
            logger.exception(
                "[Failed] operation=execute_cold_start_ticket ticket_id=%s compute_type=%s reason=%r",
                ticket.get("ticket_id"),
                compute_type_value,
                str(exc),
            )
            return self._handle_cold_start_failure(
                ticket=ticket,
                compute_pod="",
                exc=exc,
            )

    def _execute_cold_start_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        claim_token = ticket.get("claim_token") or ""
        compute_pod = ""

        try:
            compute_pod = self.provider.create_pod_for_ticket(ticket)
            ready_pod = self.provider.wait_pod_ready(
                compute_pod,
                keep_claim=lambda: self.tickets.extend_allocation_deadline(ticket_id, claim_token),
            )
            compute_pod_ip = getattr(ready_pod.status, "pod_ip", "") or self.provider.get_pod_ip(compute_pod) or ""
            if not compute_pod_ip:
                raise RuntimeError("Compute IP unavailable after pod Ready")

            compute_ready_at = self.provider.get_pod_ready_at(compute_pod)
            committed = self.tickets.mark_allocating(
                ticket_id,
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                claimed_by=ticket.get("claimed_by"),
                claim_token=claim_token,
                compute_ready_at=compute_ready_at,
            )
            if not committed or committed.get("status") != "allocating":
                try:
                    self.provider.release_pod(compute_pod)
                except Exception:
                    pass
                current = self.tickets.get_ticket(ticket_id)
                return ticket_format.ticket_response(
                    current or ticket,
                    "Ticket no longer owns the allocation",
                )

            return self._execute_allocated_ticket(committed)
        except AllocationClaimLost:
            # Whoever holds the ticket now (a cancel, or another worker after
            # the claim lapsed) owns what happens next; only drop our pod.
            try:
                self.provider.release_pod(compute_pod)
            except Exception:
                pass
            current = self.tickets.get_ticket(ticket_id)
            return ticket_format.ticket_response(
                current or ticket,
                "Ticket no longer owns the allocation",
            )
        except Exception as exc:
            return self._handle_cold_start_failure(
                ticket=ticket,
                compute_pod=compute_pod,
                exc=exc,
            )

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
            if compute_pod and not self._release_before_ticket_transition(
                compute_pod,
                ticket_id,
                "release_missing_allocation_context",
            ):
                return {
                    "ticket_id": ticket_id,
                    "compute_type": compute_type,
                    "status": "error",
                    "message": (
                        "Missing allocation context and compute Pod cleanup "
                        "is pending"
                    ),
                }
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
                self.provider.release_pod(compute_pod)
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
                self.provider.release_pod(compute_pod)
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
            self.provider.release_pod(compute_pod)
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
        if not self._release_before_ticket_transition(
            compute_pod,
            ticket_id,
            "release_after_mount_error",
        ):
            return {
                "ticket_id": ticket_id,
                "status": "error",
                "message": (
                    f"{reason}; compute Pod cleanup is pending before retry"
                ),
            }

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

    def _release_before_ticket_transition(
        self,
        compute_pod: str,
        ticket_id: str,
        operation: str,
    ) -> bool:
        if not compute_pod:
            return True
        try:
            self.provider.release_pod(compute_pod)
            return True
        except Exception as exc:
            logger.warning(
                "[Warning] operation=%s ticket_id=%s compute_pod=%s reason=%r",
                operation,
                ticket_id,
                compute_pod,
                str(exc),
            )
            return False

    def _handle_cold_start_failure(
        self,
        *,
        ticket: Dict,
        compute_pod: str,
        exc: Exception,
    ) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        claim_token = ticket.get("claim_token") or ""
        reason = f"Compute provisioning failed: {exc}"

        if compute_pod:
            try:
                self.provider.release_pod(compute_pod)
            except Exception as release_exc:
                logger.warning(
                    "[Warning] operation=release_after_cold_start_error ticket_id=%s compute_pod=%s reason=%r",
                    ticket_id,
                    compute_pod,
                    str(release_exc),
                )

        current = self.tickets.get_ticket(ticket_id)
        if not current:
            return {
                "ticket_id": ticket_id,
                "compute_type": self.queues.normalize_compute_type(ticket.get("compute_type")),
                "status": "failed",
                "message": reason,
            }

        current_status = str(current.get("status") or "").lower()
        if current_status in {"assigned", "failed", "cancelled"}:
            return ticket_format.ticket_response(current, reason)

        if int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.queues.max_retries):
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
            return ticket_format.ticket_response(requeued_ticket, reason)

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
        return ticket_format.ticket_response(failed_ticket, reason)

