import logging
import threading
from typing import Dict, Optional

from config import settings
from config.settings import set_request_label

from .compute import (
    ComputeCleanup,
    ComputeManager,
    BufferCapacityReconciler,
    WarmBufferProvider,
)
from .infra import (
    ComputeAvailabilityWatcher,
    DeploymentPolicyWatcher,
    LeaseLeaderElector,
)
from .queue import ComputeQueues
from .status import ControllerStatus

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self):
        self.provider = WarmBufferProvider()
        self.queues = ComputeQueues()
        self.tickets = self.queues.tickets
        self.compute_manager = ComputeManager(self.provider, self.queues, self.tickets)
        self.cleanup = ComputeCleanup(self.provider, self.queues, self.compute_manager)
        self.capacity_reconciler = BufferCapacityReconciler(
            self.provider,
            self.queues,
            on_capacity_available=self.compute_manager.kick_wait_queue_worker,
            # Full sweep; it calls recover_journaled_orphans itself.
            on_periodic_cleanup=self.cleanup.check_stale_allocations,
        )
        self.deployment_watcher = DeploymentPolicyWatcher(
            apps_v1=self.provider.apps_v1,
            namespace=self.provider.namespace,
            on_policy_event=self.capacity_reconciler.on_deployment_event,
            label_selector=(
                f"{self.provider.LABEL_APP}={self.provider.APP_COMPUTE_POD}"
            ),
            timeout_seconds=settings.COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS,
            retry_seconds=settings.COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS,
        )
        self.status = ControllerStatus(
            self.provider,
            self.queues,
            self.tickets,
            capacity_reconciler=self.capacity_reconciler,
        )
        provider_uses_watch = getattr(self.provider, "uses_availability_watch", True)
        watch_enabled = provider_uses_watch and (
            settings.COMPUTE_AVAILABILITY_WATCH_ENABLED
            or self.capacity_reconciler is not None
        )
        self.compute_watcher = ComputeAvailabilityWatcher(
            v1=self.provider.v1,
            namespace=self.provider.namespace,
            label_selector=f"{self.provider.LABEL_APP}={self.provider.APP_COMPUTE_POD}",
            on_compute_available=self.compute_manager.notify_compute_available,
            on_buffer_event=self.capacity_reconciler.on_buffer_event,
            enabled=watch_enabled,
            availability_notifications_enabled=(
                settings.COMPUTE_AVAILABILITY_WATCH_ENABLED
            ),
            timeout_seconds=settings.COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS,
            retry_seconds=settings.COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS,
            app_label=self.provider.LABEL_APP,
            app_value=self.provider.APP_COMPUTE_POD,
            status_label=self.provider.LABEL_STATUS,
            available_status=self.provider.STATUS_AVAILABLE,
            compute_type_label=self.provider.LABEL_COMPUTE_TYPE,
        )
        self.leader_elector = None
        self.queue_worker_thread: Optional[threading.Thread] = None
        self.queue_worker_stop_event = threading.Event()
        # Cold start has no capacity reconciler to carry the periodic sweep, so
        # the leader runs it on a thread of its own.
        self.startup_completed = False
        self._initial_buffer_result: Optional[Dict] = None

    def health_check(self) -> str:
        return "Orchestrator healthy"

    def initialize_buffer(self) -> Dict:
        result = self.provider.initialize_buffer()
        self.compute_manager.refresh_compute_types(force=True)
        return result

    def start(self) -> Dict:
        if self.startup_completed:
            return self._initial_buffer_result or {"status": "success", "created": 0, "existing": 0}

        result = self.initialize_buffer()
        self._initial_buffer_result = result
        self._start_queue_worker()

        self.leader_elector = LeaseLeaderElector(
            on_started_leading=self._start_leader_services,
            on_stopped_leading=self._stop_leader_services,
        )
        self.capacity_reconciler.set_leadership_validator(
            self.leader_elector.has_valid_leadership
        )
        self.leader_elector.start()
        logger.info("Leader election initialized")

        self.startup_completed = True
        return result

    def stop(self) -> None:
        if self.leader_elector:
            self.leader_elector.stop()
        self._stop_leader_services()
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

    def _start_leader_services(self) -> None:
        self.capacity_reconciler.start()
        self.deployment_watcher.start()
        self.compute_watcher.start()

    def _stop_leader_services(self) -> None:
        self.deployment_watcher.stop()
        self.compute_watcher.stop()
        self.capacity_reconciler.stop()

    def execute_command(
        self,
        username: str,
        command: str,
        user_pod_ip: str,
        user_pod: str = "",
        compute_type: Optional[str] = None,
        request_at_ms: Optional[int] = None,
        ticket_id: Optional[str] = None,
    ) -> Dict:
        return self.compute_manager.execute_command(
            username=username,
            command=command,
            user_pod_ip=user_pod_ip,
            user_pod=user_pod,
            compute_type=compute_type,
            request_at_ms=request_at_ms,
            ticket_id=ticket_id,
        )

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Dict:
        return self.compute_manager.cancel_ticket(ticket_id, reason=reason)

    def release_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Dict:
        return self.compute_manager.release_compute_pod(
            compute_pod=compute_pod,
            request_context=request_context,
        )

    def process_wait_queues(self) -> Dict:
        return self.compute_manager.process_wait_queues()

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.compute_manager.get_assigned_request_context(compute_pod)

    def get_buffer_status(self) -> Dict:
        return self.status.get_buffer_status()

    def get_queue_status(self, compute_type: Optional[str] = None) -> Dict:
        return self.status.get_queue_status(compute_type=compute_type)

    def get_ticket(self, ticket_id: str) -> Dict:
        return self.status.get_ticket(ticket_id)

    def touch_ticket_poll(self, ticket_id: str) -> None:
        self.tickets.touch_poll(ticket_id)
