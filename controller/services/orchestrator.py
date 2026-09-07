import logging
import threading
from typing import Dict, Optional

from config import settings
from config.settings import set_request_label

from .compute import (
    ColdStartComputePool,
    ComputeCleanup,
    ComputeManager,
    PoolCapacityReconciler,
    WarmPodPool,
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
        if settings.COMPUTE_POOL_MODE == "cold_start":
            self.pool = ColdStartComputePool()
        else:
            self.pool = WarmPodPool()
        self.queues = ComputeQueues()
        self.tickets = self.queues.tickets
        self.compute_manager = ComputeManager(self.pool, self.queues, self.tickets)
        self.cleanup = ComputeCleanup(self.pool, self.queues, self.compute_manager)
        self.capacity_reconciler = None
        self.deployment_watcher = None
        if isinstance(self.pool, WarmPodPool):
            self.capacity_reconciler = PoolCapacityReconciler(
                self.pool,
                self.queues,
                on_capacity_available=self.compute_manager.kick_wait_queue_worker,
                on_periodic_cleanup=self.cleanup.recover_journaled_orphans,
            )
            self.deployment_watcher = DeploymentPolicyWatcher(
                apps_v1=self.pool.apps_v1,
                namespace=self.pool.namespace,
                on_policy_event=self.capacity_reconciler.on_deployment_event,
                label_selector=(
                    f"{self.pool.LABEL_APP}={self.pool.APP_WARM_POOL}"
                ),
                timeout_seconds=settings.COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS,
                retry_seconds=settings.COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS,
            )
        self.status = ControllerStatus(
            self.pool,
            self.queues,
            self.tickets,
            capacity_reconciler=self.capacity_reconciler,
        )
        pool_uses_watch = getattr(self.pool, "uses_availability_watch", True)
        watch_enabled = pool_uses_watch and (
            settings.COMPUTE_AVAILABILITY_WATCH_ENABLED
            or self.capacity_reconciler is not None
        )
        self.compute_watcher = ComputeAvailabilityWatcher(
            v1=self.pool.v1,
            namespace=self.pool.namespace,
            label_selector=f"{self.pool.LABEL_APP}={self.pool.APP_WARM_POOL}",
            on_compute_available=self.compute_manager.notify_compute_available,
            on_pool_event=(
                self.capacity_reconciler.on_pool_event
                if self.capacity_reconciler
                else None
            ),
            enabled=watch_enabled,
            availability_notifications_enabled=(
                settings.COMPUTE_AVAILABILITY_WATCH_ENABLED
            ),
            timeout_seconds=settings.COMPUTE_AVAILABILITY_WATCH_TIMEOUT_SECONDS,
            retry_seconds=settings.COMPUTE_AVAILABILITY_WATCH_RETRY_SECONDS,
            app_label=self.pool.LABEL_APP,
            app_value=self.pool.APP_WARM_POOL,
            status_label=self.pool.LABEL_STATUS,
            available_status=self.pool.STATUS_AVAILABLE,
            compute_type_label=self.pool.LABEL_COMPUTE_TYPE,
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
        self.compute_manager.refresh_compute_types(force=True)
        return result

    def start(self) -> Dict:
        if self.startup_completed:
            return self._initial_pool_result or {"status": "success", "created": 0, "existing": 0}

        result = self.initialize_pool()
        self._initial_pool_result = result
        self._start_queue_worker()

        self.leader_elector = LeaseLeaderElector(
            on_started_leading=self._start_leader_services,
            on_stopped_leading=self._stop_leader_services,
        )
        if self.capacity_reconciler:
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
        if self.capacity_reconciler:
            self.capacity_reconciler.start()
        if self.deployment_watcher:
            self.deployment_watcher.start()
        self.compute_watcher.start()

    def _stop_leader_services(self) -> None:
        if self.deployment_watcher:
            self.deployment_watcher.stop()
        self.compute_watcher.stop()
        if self.capacity_reconciler:
            self.capacity_reconciler.stop()

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
        return self.compute_manager.execute_command(
            username=username,
            command=command,
            user_pod_ip=user_pod_ip,
            user_pod=user_pod,
            compute_type=compute_type,
            ingress_ts_ms=ingress_ts_ms,
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

    def check_stale_allocations(self) -> Dict:
        return self.cleanup.check_stale_allocations()

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.compute_manager.get_assigned_request_context(compute_pod)

    def get_pool_status(self) -> Dict:
        return self.status.get_pool_status()

    def get_queue_status(self, compute_type: Optional[str] = None) -> Dict:
        return self.status.get_queue_status(compute_type=compute_type)

    def get_ticket(self, ticket_id: str) -> Dict:
        return self.status.get_ticket(ticket_id)

    def touch_ticket_poll(self, ticket_id: str) -> None:
        self.tickets.touch_poll(ticket_id)
