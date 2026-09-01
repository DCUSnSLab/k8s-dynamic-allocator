"""Leader-only compute Deployment policy watch."""

import logging
import threading
from typing import Callable, Optional

from kubernetes import watch
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class DeploymentPolicyWatcher:
    """Watch warm-pool Deployments and report policy or lifecycle changes."""

    def __init__(
        self,
        *,
        apps_v1,
        namespace: str,
        on_policy_event: Optional[Callable[[str, object, str], None]] = None,
        label_selector: str = "app=warm-pod-pool",
        enabled: bool = True,
        timeout_seconds: int = 60,
        retry_seconds: float = 1.0,
    ):
        self.apps_v1 = apps_v1
        self.namespace = namespace
        self.on_policy_event = on_policy_event
        self.label_selector = label_selector
        self.enabled = enabled
        self.timeout_seconds = max(1, int(timeout_seconds))
        self.retry_seconds = max(0.1, float(retry_seconds))

        self._lock = threading.Lock()
        self._stop_event: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None
        self._watch: Optional[watch.Watch] = None

    def start(self) -> None:
        if not self.enabled:
            logger.info("[DeploymentPolicyWatchDisabled]")
            return

        with self._lock:
            if self._thread and self._thread.is_alive():
                if self._stop_event is None or not self._stop_event.is_set():
                    return

            stop_event = threading.Event()
            self._thread = threading.Thread(
                target=self._run,
                args=(stop_event,),
                name="deployment-policy-watch",
                daemon=True,
            )
            self._stop_event = stop_event
            self._thread.start()

        logger.info(
            "[DeploymentPolicyWatchStarted] namespace=%s label_selector=%s",
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
                logger.debug("[DeploymentPolicyWatchStopSkipped] reason=%r", str(exc))

        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(2.0, self.retry_seconds + 1.0))

        with self._lock:
            if self._thread is thread and (not thread or not thread.is_alive()):
                self._thread = None
                if self._stop_event is stop_event:
                    self._stop_event = None
            if self._watch is active_watch:
                self._watch = None

        logger.info("[DeploymentPolicyWatchStopped]")

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
                        self.apps_v1.list_namespaced_deployment,
                        **stream_kwargs,
                    ):
                        if stop_event.is_set():
                            break
                        self._handle_event(event)
                except ApiException as exc:
                    if not stop_event.is_set():
                        logger.warning(
                            "[Warning] operation=deployment_policy_watch "
                            "error_type=k8s_api status=%s reason=%r",
                            exc.status,
                            exc.reason,
                        )
                except Exception as exc:
                    if not stop_event.is_set():
                        logger.warning(
                            "[Warning] operation=deployment_policy_watch "
                            "error_type=unexpected reason=%r",
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
        deployments = self.apps_v1.list_namespaced_deployment(
            namespace=self.namespace,
            label_selector=self.label_selector,
        )
        for deployment in deployments.items:
            self._notify_policy_event("SYNC", deployment, source="snapshot")
        return (
            getattr(getattr(deployments, "metadata", None), "resource_version", "")
            or ""
        )

    def _handle_event(self, event: dict) -> None:
        event_type = event.get("type")
        if event_type not in {"ADDED", "MODIFIED", "DELETED"}:
            return
        deployment = event.get("object")
        if deployment is None:
            return
        self._notify_policy_event(event_type, deployment, source="watch")

    def _notify_policy_event(self, event_type: str, deployment, source: str) -> None:
        if self.on_policy_event is None:
            return
        try:
            self.on_policy_event(event_type, deployment, source)
        except Exception as exc:
            logger.warning(
                "[Warning] operation=deployment_policy_watch_callback "
                "event_type=%s deployment=%s reason=%r",
                event_type,
                getattr(getattr(deployment, "metadata", None), "name", "") or "",
                str(exc),
            )
