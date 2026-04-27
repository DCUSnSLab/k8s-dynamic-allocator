import logging
from typing import Dict

from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class BackendCleanup:
    """Periodic cleanup of stale queue tickets and orphaned backend pods.

    Runs independently of the allocation path. Tolerates partial failure:
    if the Redis queue is unavailable, stale-ticket recovery is skipped
    but pool-level orphan cleanup still proceeds so that backends bound
    to dead frontends are released.
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
            logger.warning("Queue stale recovery skipped: %s", exc)
            queue_recovery_skipped = True
            queue_recovery_error = str(exc)

        pool_list = self.pool.list_pool_status()
        assigned = [pod for pod in pool_list if pod["pool_status"] == "assigned"]

        released = []
        errors = []

        for pod_info in assigned:
            frontend_pod = pod_info.get("assigned_frontend", "")
            backend_pod = pod_info["name"]

            if not frontend_pod or frontend_pod == "unknown":
                continue

            frontend_status = self.pool.get_pod_status(frontend_pod)

            if frontend_status is None or frontend_status != "Running":
                logger.warning(
                    "Frontend '%s' is %s, releasing '%s'",
                    frontend_pod,
                    frontend_status,
                    backend_pod,
                )
                result = self.sessions.release_backend(backend_pod)
                if result["status"] == "success":
                    released.append(backend_pod)
                else:
                    errors.append({"pod": backend_pod, "error": result["message"]})

        return {
            "checked": len(assigned),
            "released": released,
            "queue_recovered": queue_recovered,
            "queue_failed": queue_failed,
            "queue_recovery_skipped": queue_recovery_skipped,
            "queue_recovery_error": queue_recovery_error,
            "errors": errors,
        }
