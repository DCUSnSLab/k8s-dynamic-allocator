import os

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)

orchestrator_instance = None
startup_completed = False


class ApiConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "api"

    def ready(self):
        global orchestrator_instance, startup_completed

        if os.environ.get("RUN_MAIN", None) != "true":
            return

        if startup_completed:
            return

        try:
            from services.startup import start_runtime

            runtime = start_runtime()
            orchestrator_instance = runtime.orchestrator_instance
            startup_completed = runtime.startup_completed

        except Exception:
            logger.exception("Controller startup failed")
            raise
