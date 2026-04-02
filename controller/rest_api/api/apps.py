from django.apps import AppConfig
import logging
import os
import threading

from config.settings import WAIT_QUEUE_WORKER_INTERVAL_SECONDS

logger = logging.getLogger(__name__)

orchestrator_instance = None
leader_elector_instance = None
leader_task_thread = None
leader_task_stop_event = threading.Event()
queue_worker_thread = None
queue_worker_stop_event = threading.Event()
startup_completed = False


def _leader_heartbeat_loop():
    identity = os.getenv("HOSTNAME", "controller-unknown")
    while not leader_task_stop_event.wait(5):
        logger.info("[LEADER] Heartbeat from %s", identity)


def _start_leader_task():
    global leader_task_thread

    if leader_task_thread and leader_task_thread.is_alive():
        return

    leader_task_stop_event.clear()
    leader_task_thread = threading.Thread(
        target=_leader_heartbeat_loop,
        name="leader-heartbeat",
        daemon=True,
    )
    leader_task_thread.start()
    logger.info("Leader-only heartbeat task started")


def _stop_leader_task():
    leader_task_stop_event.set()
    logger.info("Leader-only heartbeat task stopped")


def _queue_worker_loop():
    while not queue_worker_stop_event.wait(WAIT_QUEUE_WORKER_INTERVAL_SECONDS):
        instance = orchestrator_instance
        if instance is None:
            continue
        try:
            instance.process_wait_queues()
        except Exception:
            logger.exception("Queue worker iteration failed")


def _start_queue_worker():
    global queue_worker_thread

    if queue_worker_thread and queue_worker_thread.is_alive():
        return

    queue_worker_stop_event.clear()
    queue_worker_thread = threading.Thread(
        target=_queue_worker_loop,
        name="queue-worker",
        daemon=True,
    )
    queue_worker_thread.start()
    logger.info("Queue worker started")


class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"

    def ready(self):
        global orchestrator_instance, leader_elector_instance, startup_completed

        if os.environ.get("RUN_MAIN", None) != "true":
            return

        if startup_completed:
            return

        try:
            from services import Orchestrator, LeaseLeaderElector

            logger.info("Controller starting - initializing backend pool...")

            orchestrator_instance = Orchestrator()
            result = orchestrator_instance.initialize_pool()

            created = len(result.get("created", []))
            existing = len(result.get("existing", []))
            failed = len(result.get("failed", []))
            logger.info(
                "Pool init complete: %s created, %s existing, %s failed",
                created,
                existing,
                failed,
            )

            _start_queue_worker()

            leader_elector_instance = LeaseLeaderElector(
                on_started_leading=_start_leader_task,
                on_stopped_leading=_stop_leader_task,
            )
            leader_elector_instance.start()
            startup_completed = True
            logger.info("Leader election initialized")

        except Exception:
            logger.exception("Controller startup failed")
            raise
