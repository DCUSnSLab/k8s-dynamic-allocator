"""Leader-only backend availability watch."""

import logging
import threading
from datetime import datetime, timezone
from typing import Callable, Optional

from kubernetes import watch
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class BackendAvailabilityWatcher:
    """Watch backend pods and wake queue processing when a Ready backend appears."""

    def __init__(
        self,
        *,
        v1,
        namespace: str,
        label_selector: str,
        on_backend_available: Callable[[str, str, str, str], None],
        enabled: bool = True,
        timeout_seconds: int = 60,
        retry_seconds: float = 1.0,
        app_label: str = "app",
        app_value: str = "backend-pool",
        status_label: str = "pool-status",
        available_status: str = "available",
        backend_type_label: str = "backend-type",
    ):
        self.v1 = v1
        self.namespace = namespace
        self.label_selector = label_selector
        self.on_backend_available = on_backend_available
        self.enabled = enabled
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.retry_seconds = max(0.1, float(retry_seconds))
        self.app_label = app_label
        self.app_value = app_value
        self.status_label = status_label
        self.available_status = available_status
        self.backend_type_label = backend_type_label

        self._lock = threading.Lock()
        self._stop_event: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None
        self._watch: Optional[watch.Watch] = None

    def start(self) -> None:
        if not self.enabled:
            logger.info("[BackendWatchDisabled]")
            return

        with self._lock:
            if self._thread and self._thread.is_alive():
                if self._stop_event is None or not self._stop_event.is_set():
                    return

            stop_event = threading.Event()
            self._thread = threading.Thread(
                target=self._run,
                args=(stop_event,),
                name="backend-availability-watch",
                daemon=True,
            )
            self._stop_event = stop_event
            self._thread.start()

        logger.info(
            "[BackendWatchStarted] namespace=%s label_selector=%s",
            self.namespace,
            self.label_selector,
        )

    def stop(self) -> None:
        thread = None
        active_watch = None
        stop_event = None
        with self._lock:
            if not self._thread and not self._watch and not self._stop_event:
                return
            stop_event = self._stop_event
            if stop_event:
                stop_event.set()
            thread = self._thread
            active_watch = self._watch

        if active_watch:
            try:
                active_watch.stop()
            except Exception as exc:
                logger.debug("[BackendWatchStopSkipped] reason=%r", str(exc))

        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(2.0, self.retry_seconds + 1.0))

        with self._lock:
            if self._thread is thread and (not thread or not thread.is_alive()):
                self._thread = None
                if self._stop_event is stop_event:
                    self._stop_event = None
            if self._watch is active_watch:
                self._watch = None

        logger.info("[BackendWatchStopped]")

    def _run(self, stop_event: threading.Event) -> None:
        try:
            while not stop_event.is_set():
                active_watch = watch.Watch()
                with self._lock:
                    if self._stop_event is stop_event:
                        self._watch = active_watch

                try:
                    resource_version = self._process_snapshot()
                    stream_kwargs = {
                        "namespace": self.namespace,
                        "label_selector": self.label_selector,
                        "timeout_seconds": self.timeout_seconds,
                    }
                    if resource_version:
                        stream_kwargs["resource_version"] = resource_version
                    for event in active_watch.stream(
                        self.v1.list_namespaced_pod,
                        **stream_kwargs,
                    ):
                        if stop_event.is_set():
                            break
                        self._handle_event(event)
                except ApiException as exc:
                    if not stop_event.is_set():
                        logger.warning(
                            "[Warning] operation=backend_watch error_type=k8s_api status=%s reason=%r",
                            exc.status,
                            exc.reason,
                        )
                except Exception as exc:
                    if not stop_event.is_set():
                        logger.warning(
                            "[Warning] operation=backend_watch error_type=unexpected reason=%r",
                            str(exc),
                        )
                finally:
                    try:
                        active_watch.stop()
                    except Exception:
                        pass
                    with self._lock:
                        if self._watch is active_watch:
                            self._watch = None

                if not stop_event.is_set():
                    stop_event.wait(self.retry_seconds)
        finally:
            current_thread = threading.current_thread()
            with self._lock:
                if self._thread is current_thread:
                    self._thread = None
                if self._stop_event is stop_event:
                    self._stop_event = None

    def _process_snapshot(self) -> str:
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self.label_selector,
        )
        for pod in pods.items:
            backend_type, backend_pod = self._available_backend_info(pod)
            if backend_type:
                self._notify_backend_available(backend_type, backend_pod, source="snapshot")
        return getattr(getattr(pods, "metadata", None), "resource_version", "") or ""

    def _handle_event(self, event: dict) -> None:
        if event.get("type") not in {"ADDED", "MODIFIED"}:
            return
        pod = event.get("object")
        if pod is None:
            return
        self._notify_if_available(pod)

    def _notify_if_available(self, pod) -> None:
        backend_type, backend_pod = self._available_backend_info(pod)
        if not backend_type:
            return
        self._notify_backend_available(backend_type, backend_pod, source="watch")

    def _notify_backend_available(self, backend_type: str, backend_pod: str, source: str) -> None:
        observed_at = datetime.now(timezone.utc).isoformat()
        try:
            self.on_backend_available(backend_type, backend_pod, observed_at, source)
        except Exception as exc:
            logger.warning(
                "[Warning] operation=backend_watch_callback backend_type=%s backend_pod=%s reason=%r",
                backend_type,
                backend_pod,
                str(exc),
            )

    def _available_backend_info(self, pod) -> tuple[str, str]:
        metadata = getattr(pod, "metadata", None)
        status = getattr(pod, "status", None)
        if not metadata or not status:
            return "", ""
        if getattr(metadata, "deletion_timestamp", None):
            return "", ""

        labels = getattr(metadata, "labels", None) or {}
        if labels.get(self.app_label) != self.app_value:
            return "", ""
        if labels.get(self.status_label) != self.available_status:
            return "", ""
        backend_type = labels.get(self.backend_type_label) or ""
        if not backend_type:
            return "", ""
        if getattr(status, "phase", None) != "Running":
            return "", ""
        if not getattr(status, "pod_ip", None):
            return "", ""
        if not self._pod_is_ready(pod):
            return "", ""
        return backend_type, getattr(metadata, "name", "") or ""

    @staticmethod
    def _pod_is_ready(pod) -> bool:
        conditions = getattr(getattr(pod, "status", None), "conditions", None) or []
        for condition in conditions:
            if condition.type == "Ready":
                return condition.status == "True"
        return False
