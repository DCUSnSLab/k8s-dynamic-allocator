import logging
import os

from django.apps import AppConfig

logger = logging.getLogger(__name__)

orchestrator_instance = None
startup_completed = False


class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'

    def ready(self):
        global orchestrator_instance, startup_completed

        if os.environ.get("RUN_MAIN", None) != "true":
            return

        if startup_completed:
            return

        try:
            from services.orchestrator import Orchestrator

            logger.info("Controller starting - initializing backend pool...")
            orchestrator = Orchestrator()
            result = orchestrator.start()
            orchestrator_instance = orchestrator
            startup_completed = orchestrator.startup_completed

            created = len(result.get("created", []))
            existing = len(result.get("existing", []))
            failed = len(result.get("failed", []))
            logger.info(
                "Pool init complete: %s created, %s existing, %s failed",
                created,
                existing,
                failed,
            )

        except Exception:
            logger.exception("Controller startup failed")
            raise
