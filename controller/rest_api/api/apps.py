import os

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

orchestrator_instance = None
leader_elector_instance = None
queue_worker_thread = None
queue_worker_stop_event = None
startup_completed = False


class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"

    def ready(self):
        global orchestrator_instance, leader_elector_instance
        global queue_worker_thread, queue_worker_stop_event, startup_completed

        if os.environ.get("RUN_MAIN", None) != "true":
            return

        if startup_completed:
            return

        try:
            from services.startup import start_runtime

            runtime = start_runtime()
            orchestrator_instance = runtime.orchestrator_instance
            leader_elector_instance = runtime.leader_elector_instance
            queue_worker_thread = runtime.queue_worker_thread
            queue_worker_stop_event = runtime.queue_worker_stop_event
            startup_completed = runtime.startup_completed

        except Exception:
            logger.exception("Controller startup failed")
            raise
