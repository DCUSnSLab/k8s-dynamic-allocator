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

    def __init__(self, pool, queues, sessions):
        self.pool = pool
        self.queues = queues
        self.sessions = sessions

    def check_stale_allocations(self) -> Dict:
        queue_recovered = []
        queue_failed = []
        queue_recovery_skipped = False
        queue_recovery_error = ""

        try:
            for stale_ticket in self.queues.find_stale_allocating_tickets():
                recovered = self.sessions.recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    queue_recovered.append(recovered["ticket_id"])
                elif recovered["status"] == "failed":
                    queue_failed.append(recovered["ticket_id"])
        except QueueUnavailableError as exc:
            logger.warning("[Warning] operation=stale_recovery status=skipped reason=%r", str(exc))
            queue_recovery_skipped = True
            queue_recovery_error = str(exc)

        pool_list = self.pool.list_pool_status()
        assigned = [pod for pod in pool_list if pod["pool_status"] == "assigned"]

        released = []
        errors = []

        for pod_info in assigned:
            user_pod = pod_info.get("assigned_user", "")
            compute_pod = pod_info["name"]

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
                result = self.sessions.release_compute_pod(compute_pod)
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
            "errors": errors,
        }
