from django.apps import AppConfig
import logging
import os
import threading

logger = logging.getLogger(__name__)

# apps.py에서 생성한 인스턴스를 views.py에서 import
orchestrator_instance = None
leader_elector_instance = None
leader_task_thread = None
leader_task_stop_event = threading.Event()
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


class ApiConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'api'

    def ready(self):
        """
        Called on app startup.
        Auto-initializes Backend Pool and starts leader election.
        """
        global orchestrator_instance, leader_elector_instance, startup_completed

        # Django 개발 서버 reloader 중복 실행 방지
        if os.environ.get('RUN_MAIN', None) != 'true':
            return

        if startup_completed:
            return

        try:
            from services import Orchestrator, LeaseLeaderElector

            logger.info("Controller starting - initializing backend pool...")

            orchestrator_instance = Orchestrator()
            result = orchestrator_instance.initialize_pool()

            created = len(result.get('created', []))
            existing = len(result.get('existing', []))
            failed = len(result.get('failed', []))
            logger.info(
                f"Pool init complete: {created} created, {existing} existing, {failed} failed"
            )

            leader_elector_instance = LeaseLeaderElector(
                on_started_leading=_start_leader_task,
                on_stopped_leading=_stop_leader_task,
            )
            leader_elector_instance.start()
            startup_completed = True
            logger.info("Leader election initialized")

        except Exception as e:
            logger.error(f"Pool init failed: {e}")
