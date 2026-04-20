import os

from django.apps import AppConfig
import logging
import threading

from config.settings import WAIT_QUEUE_WORKER_INTERVAL_SECONDS

logger = logging.getLogger(__name__)

orchestrator_instance = None
leader_elector_instance = None
queue_worker_thread = None
queue_worker_stop_event = threading.Event()
startup_completed = False


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

            leader_elector_instance = LeaseLeaderElector()
            leader_elector_instance.start()
            startup_completed = True
            logger.info("Leader election initialized")

        except Exception:
            logger.exception("Controller startup failed")
            raise
