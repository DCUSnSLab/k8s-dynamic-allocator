import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from config import settings

from .. import ticket_format
from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class ComputeQueueProcessor:
    def __init__(
        self,
        pool,
        queues,
        tickets,
        allocator,
        release_pod_best_effort: Callable[[str, str], None],
    ):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self.allocator = allocator
        self.release_pod_best_effort = release_pod_best_effort
        self._compute_refresh_lock = threading.Lock()
        self._compute_refresh_interval = max(settings.WAIT_QUEUE_COMPUTE_REFRESH_SECONDS, 1.0)
        self._last_compute_refresh = 0.0
        self._queue_kick_lock = threading.Lock()
        self._queue_kick_in_flight = False
        self._queue_kick_pending_compute_types = set()
        self._queue_kick_full_scan_pending = False

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
                    processed.append(
                        self.allocator.drain_wait_queue_for_type(
                            compute_type,
                            recover_stale_ticket=self.recover_stale_ticket,
                        )
                    )
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
        self.kick_wait_queue_worker(compute_type_value)

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
        """Shared with ComputeCleanup through ComputeManager."""
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
                    self.release_pod_best_effort(compute_pod, ticket_id)
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
                self.release_pod_best_effort(compute_pod, ticket_id)
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

    def kick_wait_queue_worker(self, compute_type: Optional[str] = None) -> None:
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
            except Exception as exc:
                logger.exception(
                    "[Failed] operation=queue_kick_worker compute_type=%s reason=%r",
                    next_compute_type or "",
                    str(exc),
                )
            finally:
                with self._queue_kick_lock:
                    if self._queue_kick_in_flight:
                        self._queue_kick_in_flight = False

        threading.Thread(
            target=_runner,
            name="queue-kick",
            daemon=True,
        ).start()

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

    def _compute_types_with_stale_allocations(self) -> List[str]:
        compute_types = []
        for compute_type in self.queues.known_compute_types():
            compute_type_value = self.queues.normalize_compute_type(compute_type)
            if self.queues.find_stale_allocating_tickets(compute_type_value):
                compute_types.append(compute_type_value)
        return sorted(set(compute_types))

    @staticmethod
    def _process_result_had_lock_miss(result: Dict) -> bool:
        for item in result.get("processed") or []:
            if item.get("lock_missed"):
                return True
        return False
