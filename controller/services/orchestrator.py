import logging
import threading
from typing import Dict, Optional

from config import settings
from config.settings import set_request_label

from .backend import BackendAvailabilityWatcher, BackendCleanup, BackendPool, BackendSessions
from .infra import LeaseLeaderElector
from .queue import BackendQueues
from .status import ControllerStatus

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self):
        self.pool = BackendPool()
        self.queues = BackendQueues()
        self.tickets = self.queues.tickets
        self.sessions = BackendSessions(self.pool, self.queues, self.tickets)
        self.status = ControllerStatus(self.pool, self.queues, self.tickets)
        self.cleanup = BackendCleanup(self.pool, self.queues, self.sessions)
        self.backend_watcher = BackendAvailabilityWatcher(
            v1=self.pool.v1,
            namespace=self.pool.namespace,
            label_selector=f"{self.pool.LABEL_APP}={self.pool.APP_WARM_POOL}",
            on_backend_available=self.sessions.notify_backend_available,
            enabled=settings.BACKEND_AVAILABILITY_WATCH_ENABLED,
            timeout_seconds=settings.BACKEND_AVAILABILITY_WATCH_TIMEOUT_SECONDS,
            retry_seconds=settings.BACKEND_AVAILABILITY_WATCH_RETRY_SECONDS,
            app_label=self.pool.LABEL_APP,
            app_value=self.pool.APP_WARM_POOL,
            status_label=self.pool.LABEL_STATUS,
            available_status=self.pool.STATUS_AVAILABLE,
            backend_type_label=self.pool.LABEL_BACKEND_TYPE,
        )
        self.leader_elector = None
        self.queue_worker_thread: Optional[threading.Thread] = None
        self.queue_worker_stop_event = threading.Event()
        self.startup_completed = False
        self._initial_pool_result: Optional[Dict] = None

    def health_check(self) -> str:
        return "Orchestrator healthy"

    def initialize_pool(self) -> Dict:
        result = self.pool.initialize_pool()
        self.sessions.refresh_backend_types(force=True)
        return result

    def start(self) -> Dict:
        if self.startup_completed:
            return self._initial_pool_result or {"status": "success", "created": 0, "existing": 0}

        result = self.initialize_pool()
        self._initial_pool_result = result
        self._start_queue_worker()

        self.leader_elector = LeaseLeaderElector(
            on_started_leading=self._start_backend_watcher,
            on_stopped_leading=self._stop_backend_watcher,
        )
        self.leader_elector.start()
        logger.info("Leader election initialized")

        self.startup_completed = True
        return result

    def stop(self) -> None:
        if self.leader_elector:
            self.leader_elector.stop()
        self._stop_backend_watcher()
        self.queue_worker_stop_event.set()
        if self.queue_worker_thread and self.queue_worker_thread.is_alive():
            self.queue_worker_thread.join(timeout=5)

    def _queue_worker_loop(self) -> None:
        while not self.queue_worker_stop_event.wait(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS):
            set_request_label("-")
            try:
                self.process_wait_queues()
            except Exception as exc:
                logger.exception("[Failed] operation=queue_worker_iteration reason=%r", str(exc))
            finally:
                set_request_label("-")
        logger.info("Queue worker stopped")

    def _start_queue_worker(self) -> None:
        if self.queue_worker_thread and self.queue_worker_thread.is_alive():
            return
        self.queue_worker_stop_event.clear()
        self.queue_worker_thread = threading.Thread(
            target=self._queue_worker_loop,
            name="queue-worker",
            daemon=True,
        )
        self.queue_worker_thread.start()
        logger.info("Queue worker started")

    def _start_backend_watcher(self) -> None:
        self.backend_watcher.start()

    def _stop_backend_watcher(self) -> None:
        self.backend_watcher.stop()

    def execute_command(
        self,
        username: str,
        command: str,
        frontend_ip: str,
        frontend_pod: str = "",
        backend_type: Optional[str] = None,
        ingress_ts_ms: Optional[int] = None,
        ticket_id: Optional[str] = None,
    ) -> Dict:
        return self.sessions.execute_command(
            username=username,
            command=command,
            frontend_ip=frontend_ip,
            frontend_pod=frontend_pod,
            backend_type=backend_type,
            ingress_ts_ms=ingress_ts_ms,
            ticket_id=ticket_id,
        )

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Dict:
        return self.sessions.cancel_ticket(ticket_id, reason=reason)

    def release_backend(
        self,
        backend_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Dict:
        return self.sessions.release_backend(
            backend_pod=backend_pod,
            request_context=request_context,
        )

    def process_wait_queues(self) -> Dict:
        return self.sessions.process_wait_queues()

    def check_stale_allocations(self) -> Dict:
        return self.cleanup.check_stale_allocations()

    def get_assigned_request_context(self, backend_pod: str) -> Dict:
        return self.sessions.get_assigned_request_context(backend_pod)

    def get_pool_status(self) -> Dict:
        return self.status.get_pool_status()

    def get_queue_status(self, backend_type: Optional[str] = None) -> Dict:
        return self.status.get_queue_status(backend_type=backend_type)

    def get_ticket(self, ticket_id: str) -> Dict:
        return self.status.get_ticket(ticket_id)
