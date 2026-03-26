"""
Backend pool manager.

- Creates backend Deployments from manifests
- Allocates warm backend pods to frontend pods
- Uses pool-status to control warm-pool Deployment membership
"""

import glob
import logging
import os
import yaml
from typing import Dict, List, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from .kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)


MANIFESTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "manifests")


class PodConflictError(Exception):
    """Raised when another controller replica already took the same pod."""


class BackendPool(KubernetesClient):
    """
    Backend pod pool manager.

    `app` identifies backend pods managed by this controller.
    `pool-status` is part of the Deployment selector, so changing it from
    available -> assigned removes a pod from warm-pool membership while keeping
    the backend identity labels intact.
    """

    LABEL_APP = "app"
    LABEL_BACKEND_TYPE = "backend-type"
    LABEL_STATUS = "pool-status"
    LABEL_FRONTEND = "assigned-frontend"

    APP_WARM_POOL = "backend-pool"

    STATUS_AVAILABLE = "available"
    STATUS_ASSIGNED = "assigned"

    _cached_owner_ref = None
    _owner_ref_resolved = False

    def __init__(self):
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self.owner_ref = self._get_owner_deployment()

    def _warm_pool_selector(self, status: Optional[str] = None) -> str:
        parts = [
            f"{self.LABEL_APP}={self.APP_WARM_POOL}",
            self.LABEL_BACKEND_TYPE,
        ]
        if status:
            parts.append(f"{self.LABEL_STATUS}={status}")
        return ",".join(parts)

    def _backend_selector(self) -> str:
        return ",".join(
            [
                f"{self.LABEL_APP}={self.APP_WARM_POOL}",
                self.LABEL_BACKEND_TYPE,
            ]
        )

    def _validate_backend_manifest(self, spec: Dict) -> str:
        selector_labels = (
            spec.get("spec", {})
            .get("selector", {})
            .get("matchLabels", {})
        )
        template_labels = (
            spec.get("spec", {})
            .get("template", {})
            .get("metadata", {})
            .get("labels", {})
        )

        selector_app = selector_labels.get(self.LABEL_APP)
        template_app = template_labels.get(self.LABEL_APP)
        selector_backend_type = selector_labels.get(self.LABEL_BACKEND_TYPE)
        template_backend_type = template_labels.get(self.LABEL_BACKEND_TYPE)
        selector_status = selector_labels.get(self.LABEL_STATUS)
        template_status = template_labels.get(self.LABEL_STATUS)

        if selector_app != self.APP_WARM_POOL or template_app != self.APP_WARM_POOL:
            raise ValueError(
                "Every backend manifest must set app=backend-pool in both "
                "spec.selector.matchLabels and spec.template.metadata.labels"
            )

        if not selector_backend_type or not template_backend_type:
            raise ValueError(
                "Every backend manifest must define backend-type in both "
                "spec.selector.matchLabels and spec.template.metadata.labels"
            )

        if selector_backend_type != template_backend_type:
            raise ValueError(
                "backend-type must match between selector labels and template labels"
            )

        if selector_status != self.STATUS_AVAILABLE:
            raise ValueError(
                "Every backend manifest must set pool-status=available in "
                "spec.selector.matchLabels"
            )

        if template_status != self.STATUS_AVAILABLE:
            raise ValueError(
                "Every backend manifest must set pool-status=available in "
                "spec.template.metadata.labels"
            )

        if self.LABEL_FRONTEND not in template_labels:
            raise ValueError(
                "Every backend manifest must define assigned-frontend in "
                "spec.template.metadata.labels"
            )

        return template_backend_type

    def initialize_pool(self) -> Dict:
        """
        Create backend Deployments defined in the manifests directory.
        Safe to call multiple times because existing Deployments are skipped.
        """
        results = {"created": [], "existing": [], "failed": []}

        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        if not yaml_files:
            logger.warning("No manifest files found in %s", MANIFESTS_DIR)
            return results

        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    spec = yaml.safe_load(f)

                name = spec["metadata"]["name"]
                backend_type = self._validate_backend_manifest(spec)

                if self.owner_ref:
                    spec.setdefault("metadata", {})["ownerReferences"] = [self.owner_ref]

                self.apps_v1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=spec,
                )
                logger.info("Deployment created: %s (backend_type=%s)", name, backend_type)
                results["created"].append(name)

            except ApiException as e:
                if e.status == 409:
                    logger.info("Deployment exists: %s", name)
                    results["existing"].append(name)
                else:
                    logger.error(
                        "Deployment creation failed (%s): %s",
                        os.path.basename(yaml_file),
                        e,
                    )
                    results["failed"].append(
                        {"file": os.path.basename(yaml_file), "error": str(e)}
                    )
            except Exception as e:
                logger.error("Manifest load failed (%s): %s", os.path.basename(yaml_file), e)
                results["failed"].append(
                    {"file": os.path.basename(yaml_file), "error": str(e)}
                )

        return results

    def _get_owner_deployment(self) -> Optional[Dict]:
        """
        Resolve the controller Deployment that owns this controller pod.
        Cached so the lookup only happens once per process.
        """
        if BackendPool._owner_ref_resolved:
            return BackendPool._cached_owner_ref

        try:
            pod_name = os.getenv("HOSTNAME")
            if not pod_name:
                logger.warning("HOSTNAME not set, skipping OwnerReference")
                BackendPool._owner_ref_resolved = True
                return None

            pod = self.v1.read_namespaced_pod(pod_name, self.namespace)
            if not pod.metadata.owner_references:
                logger.warning("Pod has no ownerReferences")
                BackendPool._owner_ref_resolved = True
                return None

            rs_ref = pod.metadata.owner_references[0]
            rs = self.apps_v1.read_namespaced_replica_set(rs_ref.name, self.namespace)
            if not rs.metadata.owner_references:
                logger.warning("ReplicaSet has no ownerReferences")
                BackendPool._owner_ref_resolved = True
                return None

            deploy_ref = rs.metadata.owner_references[0]
            owner_ref = {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": deploy_ref.name,
                "uid": deploy_ref.uid,
                "blockOwnerDeletion": True,
            }

            logger.info("OwnerRef resolved: %s (uid=%s)", deploy_ref.name, deploy_ref.uid)
            BackendPool._cached_owner_ref = owner_ref
            BackendPool._owner_ref_resolved = True
            return owner_ref

        except Exception as e:
            logger.warning("Failed to get owner deployment: %s", e)
            BackendPool._owner_ref_resolved = True
            return None

    def get_available_pod(self) -> Optional[str]:
        """Return one running warm backend pod that is currently available."""
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self._warm_pool_selector(status=self.STATUS_AVAILABLE),
        )

        running_pods = [pod for pod in pods.items if pod.status.phase == "Running"]
        if not running_pods:
            return None

        return running_pods[0].metadata.name

    def assign_pod(self, pod_name: str, frontend_pod: str) -> None:
        """
        Mark an available warm pod as assigned.

        Because pool-status=available is part of the Deployment selector,
        changing it to assigned removes the pod from warm-pool membership and
        lets the Deployment backfill a new warm pod.
        """
        try:
            patch = [
                {"op": "test", "path": "/metadata/labels/app", "value": self.APP_WARM_POOL},
                {
                    "op": "test",
                    "path": "/metadata/labels/pool-status",
                    "value": self.STATUS_AVAILABLE,
                },
                {
                    "op": "replace",
                    "path": "/metadata/labels/pool-status",
                    "value": self.STATUS_ASSIGNED,
                },
                {
                    "op": "replace",
                    "path": "/metadata/labels/assigned-frontend",
                    "value": frontend_pod,
                },
            ]
            self.v1.api_client.call_api(
                "/api/v1/namespaces/{namespace}/pods/{name}",
                "PATCH",
                path_params={"namespace": self.namespace, "name": pod_name},
                body=patch,
                header_params={"Content-Type": "application/json-patch+json"},
                response_type="V1Pod",
                auth_settings=["BearerToken"],
                _return_http_data_only=True,
                _preload_content=True,
            )
        except ApiException as e:
            if e.status == 422:
                raise PodConflictError(f"{pod_name} already taken")
            raise

    def release_pod(self, pod_name: str) -> None:
        """
        Return a backend pod to warm-pool membership by restoring
        pool-status=available.

        This keeps the current release semantics intact. Later, when scale
        logic is added, this release path can be changed to delete completed
        running pods instead of restoring them.
        """
        patch = {
            "metadata": {
                "labels": {
                    self.LABEL_STATUS: self.STATUS_AVAILABLE,
                    self.LABEL_FRONTEND: "",
                }
            }
        }
        self.v1.patch_namespaced_pod(pod_name, self.namespace, patch)

    def list_pool_status(self) -> List[Dict]:
        """
        List backend pods managed by this controller.
        """
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self._backend_selector(),
        )

        status_list = []
        for pod in pods.items:
            labels = pod.metadata.labels or {}
            status_list.append(
                {
                    "name": pod.metadata.name,
                    "phase": pod.status.phase,
                    "app": labels.get(self.LABEL_APP, "unknown"),
                    "backend_type": labels.get(self.LABEL_BACKEND_TYPE, "unknown"),
                    "pool_status": labels.get(self.LABEL_STATUS, "unknown"),
                    "assigned_frontend": labels.get(self.LABEL_FRONTEND, ""),
                    "ip": pod.status.pod_ip,
                }
            )

        return status_list
