import logging
from typing import Dict

from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class ComputeCleanup:
    """Periodic cleanup of stale queue tickets and orphaned compute pods.

    Runs independently of the allocation path. Tolerates partial failure:
    if the Redis queue is unavailable, stale-ticket recovery is skipped
    but pool-level orphan cleanup still proceeds so that compute pods bound
    to dead users are released.
    """

    def __init__(self, pool, queues, compute_manager):
        self.pool = pool
        self.queues = queues
        self.compute_manager = compute_manager

    def check_stale_allocations(self) -> Dict:
        queue_recovered = []
        queue_failed = []
        queue_recovery_skipped = False
        queue_recovery_error = ""

        try:
            for stale_ticket in self.queues.find_stale_allocating_tickets():
                recovered = self.compute_manager.recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    queue_recovered.append(recovered["ticket_id"])
                elif recovered["status"] == "failed":
                    queue_failed.append(recovered["ticket_id"])
        except QueueUnavailableError as exc:
            logger.warning("[Warning] operation=stale_recovery status=skipped reason=%r", str(exc))
            queue_recovery_skipped = True
            queue_recovery_error = str(exc)

        pool_list = self.pool.list_pool_status()
        journal_cleanup = self.recover_journaled_orphans(pool_list=pool_list)
        assigned = [pod for pod in pool_list if pod["pool_status"] == "assigned"]

        released = list(journal_cleanup["released"])
        already_released = set(released)
        errors = list(journal_cleanup["errors"])

        for pod_info in assigned:
            user_pod = pod_info.get("assigned_user", "")
            compute_pod = pod_info["name"]

            if compute_pod in already_released:
                continue
            if not user_pod or user_pod == "unknown":
                continue

            user_status = self.pool.get_pod_status(user_pod)

            if user_status is None or user_status != "Running":
                logger.warning(
                    "[Warning] operation=orphan_compute_release user_pod=%s user_pod_status=%s compute_pod=%s",
                    user_pod,
                    user_status,
                    compute_pod,
                )
                result = self.compute_manager.release_compute_pod(compute_pod)
                if result["status"] == "success":
                    released.append(compute_pod)
                else:
                    errors.append({"pod": compute_pod, "error": result["message"]})

        return {
            "checked": len(assigned),
            "released": released,
            "queue_recovered": queue_recovered,
            "queue_failed": queue_failed,
            "queue_recovery_skipped": queue_recovery_skipped,
            "queue_recovery_error": queue_recovery_error,
            "journal_checked": journal_cleanup["checked"],
            "journal_cleanup_skipped": journal_cleanup["skipped"],
            "errors": errors,
        }

    def recover_journaled_orphans(self, pool_list=None) -> Dict:
        """
        Retry deletion of assigned Pods whose reservation journal no longer
        matches the ticket that owns them.

        This is a convergence backstop for the partial-failure window where a
        ticket transition succeeds but the Kubernetes Pod delete call fails.
        It is safe during the flip-to-Redis-commit window: an allocating ticket
        with the same claim token and an empty compute_pod is still considered
        valid.
        """
        pods = pool_list if pool_list is not None else self.pool.list_pool_status()
        checked = 0
        released = []
        skipped = []
        errors = []

        for pod_info in pods:
            if (
                pod_info.get("pool_status") != "assigned"
                or pod_info.get("terminating")
            ):
                continue

            ticket_id = str(pod_info.get("allocation_ticket_id") or "").strip()
            claim_token = str(
                pod_info.get("allocation_claim_token") or ""
            ).strip()
            if not ticket_id:
                continue

            checked += 1
            try:
                ticket = self.compute_manager.tickets.get_ticket(ticket_id)
            except QueueUnavailableError as exc:
                logger.warning(
                    "[Warning] operation=reservation_journal_cleanup "
                    "ticket_id=%s reason=%r",
                    ticket_id,
                    str(exc),
                )
                skipped.append(pod_info["name"])
                continue

            reason = self._journal_orphan_reason(
                pod_info,
                ticket,
                claim_token,
            )
            if not reason:
                continue

            compute_pod = pod_info["name"]
            try:
                self.pool.release_pod(compute_pod)
                released.append(compute_pod)
                logger.warning(
                    "[ReservationOrphanReleased] compute_pod=%s "
                    "ticket_id=%s reason=%s",
                    compute_pod,
                    ticket_id,
                    reason,
                )
            except Exception as exc:
                errors.append({"pod": compute_pod, "error": str(exc)})
                logger.warning(
                    "[Warning] operation=reservation_journal_delete "
                    "compute_pod=%s ticket_id=%s reason=%r",
                    compute_pod,
                    ticket_id,
                    str(exc),
                )

        return {
            "checked": checked,
            "released": released,
            "skipped": skipped,
            "errors": errors,
        }

    @staticmethod
    def _journal_orphan_reason(
        pod_info: Dict,
        ticket,
        journal_claim_token: str,
    ) -> str:
        # A missing ticket can be an expired long-running assignment, so it is
        # not sufficient evidence for deletion.
        if not ticket:
            return ""

        status = str(ticket.get("status") or "").lower()
        if status in {"queued", "failed", "cancelled"}:
            return f"ticket_status_{status}"
        if status not in {"allocating", "assigned"}:
            return ""

        ticket_claim_token = str(ticket.get("claim_token") or "").strip()
        if (
            journal_claim_token
            and ticket_claim_token != journal_claim_token
        ):
            return "claim_token_mismatch"

        compute_pod = str(pod_info.get("name") or "").strip()
        ticket_compute_pod = str(ticket.get("compute_pod") or "").strip()
        if ticket_compute_pod and ticket_compute_pod != compute_pod:
            return "compute_pod_mismatch"
        if status == "assigned" and not ticket_compute_pod:
            return "assigned_ticket_missing_compute_pod"
        return ""
