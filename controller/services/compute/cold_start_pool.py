"""Cold-start compute pod manager.

Creates one standalone compute pod per claimed ticket and deletes it on release.
"""

import copy
import glob
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import yaml
from kubernetes.client.rest import ApiException

from config import settings

from ..infra.kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)


MANIFESTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "manifests",
    "cold_start",
)


class ColdStartComputePool(KubernetesClient):
    """Create request-scoped compute pods instead of maintaining a warm pool."""

    allocation_mode = "cold_start"
    uses_availability_watch = False

    LABEL_APP = "app"
    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "pool-status"
    LABEL_USER = "assigned-user"

    APP_WARM_POOL = "warm-pod-pool"

    STATUS_AVAILABLE = "available"
    STATUS_ASSIGNED = "assigned"

    def __init__(self):
        super().__init__()
        self._templates_by_type: Dict[str, Dict] = {}

    def initialize_pool(self, *, log_existing: bool = True) -> Dict:
        """Load and validate cold-start pod templates without creating pods."""
        results = {"created": [], "existing": [], "failed": [], "templates": []}
        self._templates_by_type = {}

        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        if not yaml_files:
            logger.warning(
                "[Warning] operation=cold_start_manifest_discovery manifest_dir=%s reason=%r",
                MANIFESTS_DIR,
                "no manifest files found",
            )
            return results

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, encoding="utf-8") as handle:
                    spec = yaml.safe_load(handle)
                compute_type = self._validate_pod_template(spec)
                self._templates_by_type[compute_type] = spec
                results["templates"].append({"file": os.path.basename(yaml_file), "compute_type": compute_type})
            except Exception as exc:
                logger.error(
                    "[Failed] operation=cold_start_manifest_load manifest=%s reason=%r",
                    os.path.basename(yaml_file),
                    str(exc),
                )
                results["failed"].append({"file": os.path.basename(yaml_file), "error": str(exc)})

        if results["templates"] and log_existing:
            logger.info("Cold-start pod templates loaded: %s", results["templates"])
        return results

    def create_pod_for_ticket(self, ticket: Dict) -> str:
        """Create a standalone compute pod for one allocating ticket."""
        compute_type = self._normalize_compute_type(ticket.get("compute_type"))
        template = self._template_for_compute_type(compute_type)
        pod_name = self._pod_name_for_ticket(ticket, compute_type)
        user_pod = (ticket.get("user_pod") or "unknown").strip() or "unknown"

        body = copy.deepcopy(template)
        metadata = body.setdefault("metadata", {})
        metadata["name"] = pod_name
        metadata.pop("generateName", None)
        metadata.pop("ownerReferences", None)
        metadata["namespace"] = self.namespace

        labels = metadata.setdefault("labels", {})
        labels[self.LABEL_APP] = self.APP_WARM_POOL
        labels[self.LABEL_COMPUTE_TYPE] = compute_type
        labels[self.LABEL_STATUS] = self.STATUS_ASSIGNED
        labels[self.LABEL_USER] = user_pod

        try:
            self.v1.create_namespaced_pod(namespace=self.namespace, body=body)
            return pod_name
        except ApiException as exc:
            if exc.status == 409:
                raise RuntimeError(f"Cold-start compute pod already exists: {pod_name}") from exc
            raise

    def wait_pod_ready(self, pod_name: str):
        timeout = max(1.0, float(settings.COLD_START_POD_READY_TIMEOUT_SECONDS))
        poll = max(0.2, float(settings.COLD_START_POD_READY_POLL_SECONDS))
        deadline = time.monotonic() + timeout
        last_phase = ""

        while time.monotonic() < deadline:
            try:
                pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            except ApiException as exc:
                if exc.status == 404:
                    time.sleep(poll)
                    continue
                raise

            last_phase = getattr(pod.status, "phase", "") or ""
            if self._pod_is_ready(pod):
                return pod
            if last_phase in {"Failed", "Succeeded"}:
                raise RuntimeError(f"Cold-start compute pod ended before Ready: {pod_name} phase={last_phase}")
            time.sleep(poll)

        raise TimeoutError(f"Timed out waiting for cold-start compute pod Ready: {pod_name} phase={last_phase}")

    def get_available_pod(self, compute_type: Optional[str] = None) -> Optional[str]:
        return None

    def get_available_pods(
        self,
        compute_type: Optional[str] = None,
        exclude: Optional[set] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        return []

    def get_pod_ready_at(self, pod_name: str):
        try:
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
        return self._pod_ready_at(pod)

    def release_pod(self, pod_name: str) -> bool:
        try:
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=0,
            )
            logger.debug("[ComputeDeleted] compute_pod=%s mode=cold_start", pod_name)
            return True
        except ApiException as exc:
            if exc.status in (404, 409):
                logger.debug("[ComputeDeleted] compute_pod=%s status=already_deleted_or_terminating", pod_name)
                return False
            raise

    def list_pool_status(self, compute_type: Optional[str] = None) -> List[Dict]:
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self._compute_selector(compute_type=compute_type),
        )

        status_list = []
        for pod in pods.items:
            labels = pod.metadata.labels or {}
            status_list.append(
                {
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,
                    "app": labels.get(self.LABEL_APP, "unknown"),
                    "compute_type": labels.get(self.LABEL_COMPUTE_TYPE, "unknown"),
                    "pool_status": labels.get(self.LABEL_STATUS, "unknown"),
                    "assigned_user": labels.get(self.LABEL_USER, ""),
                    "ready": self._pod_is_ready(pod),
                    "ip": pod.status.pod_ip,
                }
            )
        return status_list

    def _template_for_compute_type(self, compute_type: str) -> Dict:
        if compute_type not in self._templates_by_type:
            self.initialize_pool(log_existing=False)
        template = self._templates_by_type.get(compute_type)
        if not template:
            raise ValueError(f"No cold-start compute pod template for compute_type={compute_type}")
        return template

    def _validate_pod_template(self, spec: Dict) -> str:
        if not isinstance(spec, dict):
            raise ValueError("Invalid manifest: expected mapping")
        if spec.get("kind") != "Pod":
            raise ValueError("Cold-start compute manifest must be kind=Pod")

        labels = spec.get("metadata", {}).get("labels", {})
        if labels.get(self.LABEL_APP) != self.APP_WARM_POOL:
            raise ValueError("Cold-start compute pod template must set app=warm-pod-pool")

        compute_type = labels.get(self.LABEL_COMPUTE_TYPE)
        if not compute_type:
            raise ValueError("Cold-start compute pod template must set compute-type")

        if labels.get(self.LABEL_STATUS) != self.STATUS_ASSIGNED:
            raise ValueError("Cold-start compute pod template must set pool-status=assigned")

        if self.LABEL_USER not in labels:
            raise ValueError("Cold-start compute pod template must define assigned-user")

        containers = spec.get("spec", {}).get("containers") or []
        if not containers:
            raise ValueError("Cold-start compute pod template must define containers")
        return self._normalize_compute_type(compute_type)

    def _compute_selector(self, compute_type: Optional[str] = None) -> str:
        parts = [f"{self.LABEL_APP}={self.APP_WARM_POOL}"]
        if compute_type:
            parts.append(f"{self.LABEL_COMPUTE_TYPE}={self._normalize_compute_type(compute_type)}")
        return ",".join(parts)

    def _pod_name_for_ticket(self, ticket: Dict, compute_type: str) -> str:
        ticket_id = str(ticket.get("ticket_id") or "")
        ticket_short = (ticket.get("ticket_short") or ticket_id[:10] or "unknown").strip().lower()
        ticket_short = re.sub(r"[^a-z0-9-]", "-", ticket_short).strip("-") or "unknown"
        try:
            retry_count = int(ticket.get("retry_count") or 0)
        except (TypeError, ValueError):
            retry_count = 0
        if retry_count > 0:
            ticket_short = f"{ticket_short}-{retry_count}"
        compute_type_value = re.sub(r"[^a-z0-9-]", "-", compute_type.lower()).strip("-") or "general"
        prefix = f"compute-{compute_type_value}-cold-"
        max_suffix = max(1, 63 - len(prefix))
        return f"{prefix}{ticket_short[:max_suffix]}".strip("-")

    def _normalize_compute_type(self, compute_type: Optional[str]) -> str:
        value = (compute_type or settings.DEFAULT_COMPUTE_TYPE or "general").strip().lower()
        return value or "general"

    @staticmethod
    def _pod_is_ready(pod) -> bool:
        if getattr(pod.metadata, "deletion_timestamp", None):
            return False
        if getattr(pod.status, "phase", None) != "Running":
            return False
        conditions = getattr(pod.status, "conditions", None) or []
        for condition in conditions:
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    @staticmethod
    def _pod_ready_at(pod):
        conditions = getattr(pod.status, "conditions", None) or []
        for condition in conditions:
            if condition.type == "Ready" and condition.status == "True":
                return getattr(condition, "last_transition_time", None)
        return datetime.now(timezone.utc)
