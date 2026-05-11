import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from config import settings

from .. import ticket_format
from ..queue import QueueUnavailableError
from .agent_client import ComputeAgent, ComputeAgentError
from .warm_pod_pool import PodConflictError

logger = logging.getLogger(__name__)


class ComputeAllocator:
    def __init__(self, pool, queues, tickets):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets

    def drain_wait_queue_for_type(
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
                recovered = recover_stale_ticket(stale_ticket)
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
