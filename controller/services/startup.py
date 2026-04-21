"""Controller runtime startup helpers."""

import logging
import threading
from dataclasses import dataclass, field
from typing import Optional

from config.settings import WAIT_QUEUE_WORKER_INTERVAL_SECONDS

logger = logging.getLogger(__name__)


@dataclass
class ControllerRuntime:
    orchestrator_instance: object = None
    leader_elector_instance: object = None
    queue_worker_thread: Optional[threading.Thread] = None
    queue_worker_stop_event: threading.Event = field(default_factory=threading.Event)
    startup_completed: bool = False


runtime = ControllerRuntime()


def _queue_worker_loop() -> None:
    while not runtime.queue_worker_stop_event.wait(WAIT_QUEUE_WORKER_INTERVAL_SECONDS):
        instance = runtime.orchestrator_instance
        if instance is None:
            continue
        try:
            instance.process_wait_queues()
        except Exception:
            logger.exception("Queue worker iteration failed")


def _start_queue_worker() -> None:
    if runtime.queue_worker_thread and runtime.queue_worker_thread.is_alive():
        return

    runtime.queue_worker_stop_event.clear()
    runtime.queue_worker_thread = threading.Thread(
        target=_queue_worker_loop,
        name="queue-worker",
        daemon=True,
    )
    runtime.queue_worker_thread.start()
    logger.info("Queue worker started")


def start_runtime() -> ControllerRuntime:
    if runtime.startup_completed:
        return runtime

    from .leader import LeaseLeaderElector
    from .orchestrator import Orchestrator

    logger.info("Controller starting - initializing backend pool...")

    runtime.orchestrator_instance = Orchestrator()
    result = runtime.orchestrator_instance.initialize_pool()

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

    runtime.leader_elector_instance = LeaseLeaderElector()
    runtime.leader_elector_instance.start()
    runtime.startup_completed = True
    logger.info("Leader election initialized")
    return runtime
