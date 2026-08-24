"""
Compute pool manager

- Creates compute Deployments from manifests
- Allocates warm compute pods to user pods
- Uses pool-status to control warm-pool Deployment membership
"""

import glob
import logging
import os
import yaml
from typing import Dict, List, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from ..infra.kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)


MANIFESTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "manifests")


class PodConflictError(Exception):
    """Raised when another controller replica already took the same pod."""


class WarmPodPool(KubernetesClient):
    """
    Compute pod pool manager.

    `app` identifies compute pods managed by this controller.
    `pool-status` is part of the Deployment selector, so changing it from
    available -> assigned removes a pod from warm-pool membership while keeping
    the compute identity labels intact. Released assigned pods are deleted so
    the Deployment can backfill a new Ready warm pod.
    """

    LABEL_APP = "app"
    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "pool-status"
    LABEL_USER = "assigned-user"

    APP_WARM_POOL = "warm-pod-pool"

    STATUS_AVAILABLE = "available"
    STATUS_ASSIGNED = "assigned"

    ANNOTATION_POOL_AVAILABLE_MIN = "k8s-dynamic-allocator/pool-available-min"
    ANNOTATION_POOL_TOTAL_MAX = "k8s-dynamic-allocator/pool-total-max"
    ANNOTATION_ALLOCATION_TICKET = "k8s-dynamic-allocator/allocation-ticket-id"
    ANNOTATION_ALLOCATION_CLAIM = "k8s-dynamic-allocator/allocation-claim-token"

    _cached_owner_ref = None
    _owner_ref_resolved = False

    def __init__(self):
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self.owner_ref = self._get_owner_deployment()

    def _warm_pod_pool_selector(
        self,
        status: Optional[str] = None,
        compute_type: Optional[str] = None,
    ) -> str:
        parts = [f"{self.LABEL_APP}={self.APP_WARM_POOL}"]
        if compute_type:
            parts.append(f"{self.LABEL_COMPUTE_TYPE}={compute_type}")
        if status:
            parts.append(f"{self.LABEL_STATUS}={status}")
        return ",".join(parts)

    def _compute_selector(self, compute_type: Optional[str] = None) -> str:
        parts = [f"{self.LABEL_APP}={self.APP_WARM_POOL}"]
        if compute_type:
            parts.append(f"{self.LABEL_COMPUTE_TYPE}={compute_type}")
        return ",".join(parts)

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
        return None

    def _validate_compute_manifest(self, spec: Dict) -> str:
        metadata = spec.get("metadata", {})
        deployment_labels = metadata.get("labels", {})
        annotations = metadata.get("annotations", {})
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
        selector_compute_type = selector_labels.get(self.LABEL_COMPUTE_TYPE)
        template_compute_type = template_labels.get(self.LABEL_COMPUTE_TYPE)
        selector_status = selector_labels.get(self.LABEL_STATUS)
        template_status = template_labels.get(self.LABEL_STATUS)
        deployment_app = deployment_labels.get(self.LABEL_APP)
        deployment_compute_type = deployment_labels.get(self.LABEL_COMPUTE_TYPE)

        if selector_app != self.APP_WARM_POOL or template_app != self.APP_WARM_POOL:
            raise ValueError(
                "Every compute manifest must set app=warm-pod-pool in both "
                "spec.selector.matchLabels and spec.template.metadata.labels"
            )

        if not selector_compute_type or not template_compute_type:
            raise ValueError(
                "Every compute manifest must define compute-type in both "
                "spec.selector.matchLabels and spec.template.metadata.labels"
            )

        if selector_compute_type != template_compute_type:
            raise ValueError(
                "compute-type must match between selector labels and template labels"
            )

        if selector_status != self.STATUS_AVAILABLE:
            raise ValueError(
                "Every compute manifest must set pool-status=available in "
                "spec.selector.matchLabels"
            )

        if template_status != self.STATUS_AVAILABLE:
            raise ValueError(
                "Every compute manifest must set pool-status=available in "
                "spec.template.metadata.labels"
            )

        if deployment_app != self.APP_WARM_POOL:
            raise ValueError(
                "Every compute manifest must set app=warm-pod-pool in metadata.labels"
            )

        if deployment_compute_type != template_compute_type:
            raise ValueError(
                "compute-type must match between Deployment metadata and Pod template labels"
            )

        if self.LABEL_USER not in template_labels:
            raise ValueError(
                "Every compute manifest must define assigned-user in "
                "spec.template.metadata.labels"
            )

        self._parse_policy_values(annotations)
        return template_compute_type

    @classmethod
    def _parse_policy_values(cls, annotations: Dict) -> tuple[int, int]:
        annotations = annotations or {}
        raw_r = annotations.get(cls.ANNOTATION_POOL_AVAILABLE_MIN)
        raw_n = annotations.get(cls.ANNOTATION_POOL_TOTAL_MAX)
        if raw_r is None or raw_n is None:
            raise ValueError(
                "Pool policy annotations are required: "
                f"{cls.ANNOTATION_POOL_AVAILABLE_MIN}, "
                f"{cls.ANNOTATION_POOL_TOTAL_MAX}"
            )

        try:
            pool_available_min = int(str(raw_r).strip())
            pool_total_max = int(str(raw_n).strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("Pool policy annotations must be integers") from exc

        if (
            pool_available_min < 0
            or pool_total_max < 0
            or pool_available_min > pool_total_max
        ):
            raise ValueError("Pool policy must satisfy 0 <= R <= N")

        return pool_available_min, pool_total_max

    def parse_deployment_policy(self, deployment) -> Dict:
        """Parse and validate R/N from a warm-pool Deployment."""
        metadata = getattr(deployment, "metadata", None)
        if metadata is None:
            raise ValueError("Deployment metadata is missing")

        labels = getattr(metadata, "labels", None) or {}
        annotations = getattr(metadata, "annotations", None) or {}
        deployment_name = (getattr(metadata, "name", None) or "").strip()
        compute_type = (labels.get(self.LABEL_COMPUTE_TYPE) or "").strip().lower()

        if labels.get(self.LABEL_APP) != self.APP_WARM_POOL:
            raise ValueError("Deployment must set app=warm-pod-pool")
        if not deployment_name:
            raise ValueError("Deployment name is missing")
        if not compute_type:
            raise ValueError("Deployment metadata.labels.compute-type is missing")

        spec = getattr(deployment, "spec", None)
        selector_labels = (
            getattr(getattr(spec, "selector", None), "match_labels", None) or {}
        )
        template = getattr(spec, "template", None)
        template_labels = (
            getattr(getattr(template, "metadata", None), "labels", None) or {}
        )
        if (
            selector_labels.get(self.LABEL_APP) != self.APP_WARM_POOL
            or selector_labels.get(self.LABEL_COMPUTE_TYPE) != compute_type
            or selector_labels.get(self.LABEL_STATUS) != self.STATUS_AVAILABLE
            or template_labels.get(self.LABEL_APP) != self.APP_WARM_POOL
            or template_labels.get(self.LABEL_COMPUTE_TYPE) != compute_type
            or template_labels.get(self.LABEL_STATUS) != self.STATUS_AVAILABLE
            or self.LABEL_USER not in template_labels
        ):
            raise ValueError(
                "Deployment selector/template labels do not match the warm-pool policy"
            )

        pool_available_min, pool_total_max = self._parse_policy_values(annotations)
        return {
            "compute_type": compute_type,
            "deployment_name": deployment_name,
            "R": pool_available_min,
            "N": pool_total_max,
            "resource_version": (
                getattr(metadata, "resource_version", None) or ""
            ),
        }

    def list_pool_deployments(self) -> List:
        deployments = self.apps_v1.list_namespaced_deployment(
            namespace=self.namespace,
            label_selector=f"{self.LABEL_APP}={self.APP_WARM_POOL}",
            _request_timeout=self.api_request_timeout,
        )
        return list(deployments.items)

    def initialize_pool(self, *, log_existing: bool = True) -> Dict:
        """
        Create compute Deployments defined in the manifests directory.
        Safe to call multiple times because existing Deployments are skipped.
        """
        results = {"created": [], "existing": [], "failed": []}
        existing_deployments = {}

        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        if not yaml_files:
            logger.warning("[Warning] operation=manifest_discovery manifest_dir=%s reason=%r", MANIFESTS_DIR, "no manifest files found")
            return results

        try:
            existing_deployments = {
                deployment.metadata.name: deployment
                for deployment in self.apps_v1.list_namespaced_deployment(
                    namespace=self.namespace,
                    _request_timeout=self.api_request_timeout,
                ).items
            }
        except Exception as exc:
            logger.debug("[DeploymentPrefetchSkipped] reason=%r", str(exc))

        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    spec = yaml.safe_load(f)

                if not isinstance(spec, dict) or "metadata" not in spec:
                    raise ValueError("Invalid manifest: missing metadata")
                name = spec["metadata"].get("name")
                if not name:
                    raise ValueError("Invalid manifest: missing metadata.name")
                compute_type = self._validate_compute_manifest(spec)

                if name in existing_deployments:
                    existing = existing_deployments[name]
                    if existing is not None:
                        self._migrate_legacy_deployment_metadata(
                            existing,
                            spec,
                        )
                    if log_existing:
                        logger.debug("Deployment exists: %s", name)
                    results["existing"].append(name)
                    continue

                if self.owner_ref:
                    spec.setdefault("metadata", {})["ownerReferences"] = [self.owner_ref]

                self.apps_v1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=spec,
                    _request_timeout=self.api_request_timeout,
                )
                logger.info("Deployment created: %s (compute_type=%s)", name, compute_type)
                results["created"].append(name)
                existing_deployments[name] = None

            except ApiException as e:
                if e.status == 409:
                    if log_existing:
                        logger.debug("Deployment exists: %s", name)
                    results["existing"].append(name)
                else:
                    logger.error(
                        "[Failed] operation=deployment_create manifest=%s reason=%r",
                        os.path.basename(yaml_file),
                        str(e),
                    )
                    results["failed"].append(
                        {"file": os.path.basename(yaml_file), "error": str(e)}
                    )
            except Exception as e:
                logger.error("[Failed] operation=manifest_load manifest=%s reason=%r", os.path.basename(yaml_file), str(e))
                results["failed"].append(
                    {"file": os.path.basename(yaml_file), "error": str(e)}
                )

        return results

    def _migrate_legacy_deployment_metadata(self, deployment, manifest: Dict) -> bool:
        """
        Add R/N metadata once for Deployments created by the pre-policy version.

        A Deployment is considered legacy only when both policy annotations and
        both top-level identity labels are absent. Partially missing or invalid
        administrator configuration is intentionally left untouched so the
        fail-closed policy can report it.
        """
        metadata = getattr(deployment, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        annotations = getattr(metadata, "annotations", None) or {}
        legacy = (
            self.LABEL_APP not in labels
            and self.LABEL_COMPUTE_TYPE not in labels
            and self.ANNOTATION_POOL_AVAILABLE_MIN not in annotations
            and self.ANNOTATION_POOL_TOTAL_MAX not in annotations
        )
        if not legacy:
            return False

        manifest_metadata = manifest.get("metadata", {})
        manifest_labels = manifest_metadata.get("labels", {})
        manifest_annotations = manifest_metadata.get("annotations", {})
        expected_compute_type = manifest_labels[self.LABEL_COMPUTE_TYPE]

        live_spec = getattr(deployment, "spec", None)
        live_selector = (
            getattr(getattr(live_spec, "selector", None), "match_labels", None)
            or {}
        )
        live_template = getattr(live_spec, "template", None)
        live_template_labels = (
            getattr(getattr(live_template, "metadata", None), "labels", None)
            or {}
        )
        live_identity_matches = (
            live_selector.get(self.LABEL_APP) == self.APP_WARM_POOL
            and live_selector.get(self.LABEL_COMPUTE_TYPE)
            == expected_compute_type
            and live_selector.get(self.LABEL_STATUS) == self.STATUS_AVAILABLE
            and live_template_labels.get(self.LABEL_APP)
            == self.APP_WARM_POOL
            and live_template_labels.get(self.LABEL_COMPUTE_TYPE)
            == expected_compute_type
            and live_template_labels.get(self.LABEL_STATUS)
            == self.STATUS_AVAILABLE
            and self.LABEL_USER in live_template_labels
        )
        if not live_identity_matches:
            logger.error(
                "[PoolPolicyMigrationBlocked] deployment=%s reason=%r",
                getattr(metadata, "name", "") or "",
                "live selector/template does not match the warm-pool manifest",
            )
            return False

        metadata_patch = {
            "labels": {
                self.LABEL_APP: manifest_labels[self.LABEL_APP],
                self.LABEL_COMPUTE_TYPE: manifest_labels[
                    self.LABEL_COMPUTE_TYPE
                ],
            },
            "annotations": {
                self.ANNOTATION_POOL_AVAILABLE_MIN: manifest_annotations[
                    self.ANNOTATION_POOL_AVAILABLE_MIN
                ],
                self.ANNOTATION_POOL_TOTAL_MAX: manifest_annotations[
                    self.ANNOTATION_POOL_TOTAL_MAX
                ],
            },
        }
        resource_version = getattr(metadata, "resource_version", None)
        if resource_version:
            metadata_patch["resourceVersion"] = resource_version
        body = {"metadata": metadata_patch}
        self.apps_v1.patch_namespaced_deployment(
            name=metadata.name,
            namespace=self.namespace,
            body=body,
            _request_timeout=self.api_request_timeout,
        )
        logger.info(
            "[PoolPolicyMigrated] deployment=%s R=%s N=%s",
            metadata.name,
            body["metadata"]["annotations"][
                self.ANNOTATION_POOL_AVAILABLE_MIN
            ],
            body["metadata"]["annotations"][self.ANNOTATION_POOL_TOTAL_MAX],
        )
        return True

    def _get_owner_deployment(self) -> Optional[Dict]:
        """
        Resolve the controller Deployment that owns this controller pod.
        Cached so the lookup only happens once per process.
        """
        if WarmPodPool._owner_ref_resolved:
            return WarmPodPool._cached_owner_ref

        try:
            pod_name = os.getenv("HOSTNAME")
            if not pod_name:
                logger.warning("[Warning] operation=owner_ref reason=%r", "HOSTNAME not set")
                WarmPodPool._owner_ref_resolved = True
                return None

            pod = self.v1.read_namespaced_pod(
                pod_name,
                self.namespace,
                _request_timeout=self.api_request_timeout,
            )
            if not pod.metadata.owner_references:
                logger.warning("[Warning] operation=owner_ref pod=%s reason=%r", pod_name, "pod has no ownerReferences")
                WarmPodPool._owner_ref_resolved = True
                return None

            rs_ref = pod.metadata.owner_references[0]
            rs = self.apps_v1.read_namespaced_replica_set(
                rs_ref.name,
                self.namespace,
                _request_timeout=self.api_request_timeout,
            )
            if not rs.metadata.owner_references:
                logger.warning("[Warning] operation=owner_ref replicaset=%s reason=%r", rs_ref.name, "replicaset has no ownerReferences")
                WarmPodPool._owner_ref_resolved = True
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
            WarmPodPool._cached_owner_ref = owner_ref
            WarmPodPool._owner_ref_resolved = True
            return owner_ref

        except Exception as e:
            logger.warning("[Warning] operation=owner_ref reason=%r", str(e))
            WarmPodPool._owner_ref_resolved = True
            return None

    def get_available_pod(self, compute_type: Optional[str] = None) -> Optional[str]:
        """Return one Ready warm compute pod that is currently available."""
        names = self.get_available_pods(compute_type=compute_type, limit=1)
        return names[0] if names else None

    def get_pod_ready_at(self, pod_name: str):
        try:
            pod = self.v1.read_namespaced_pod(
                pod_name,
                self.namespace,
                _request_timeout=self.api_request_timeout,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
        return self._pod_ready_at(pod)

    def get_available_pods(
        self,
        compute_type: Optional[str] = None,
        exclude: Optional[set] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        """Return Ready warm compute pod names that are currently available.

        `exclude` filters out pods already reserved in the current pass because
        the apiserver watch cache can briefly report a freshly-patched pod
        as still available, so callers tracking in-flight reservations pass
        them here to avoid double-selection.
        """
        snapshot = self.list_pool_snapshot(compute_type=compute_type)
        exclude_set = exclude or set()
        names: List[str] = []
        for candidate in snapshot["available_candidates"]:
            name = candidate["name"]
            if name in exclude_set:
                continue
            names.append(name)
            if limit is not None and len(names) >= limit:
                break
        return names

    @staticmethod
    def _has_controller_owner(pod, kind: str) -> bool:
        owner_references = (
            getattr(getattr(pod, "metadata", None), "owner_references", None) or []
        )
        for owner in owner_references:
            if getattr(owner, "kind", None) != kind:
                continue
            if getattr(owner, "controller", None) is False:
                continue
            return True
        return False

    def list_pool_snapshot(self, compute_type: Optional[str] = None) -> Dict:
        """
        Return one API-list based snapshot for allocation and capacity control.

        Pool_Total counts non-terminating available + assigned pods. Creating
        Pods are included even when they are Pending or NotReady.
        """
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self._compute_selector(compute_type=compute_type),
            _request_timeout=self.api_request_timeout,
        )

        pool_total = 0
        pool_available = 0
        pool_assigned = 0
        terminating = 0
        ready_available = 0
        assigned_with_replicaset_owner = 0
        available_candidates = []
        pod_items = []

        for pod in pods.items:
            metadata = getattr(pod, "metadata", None)
            status = getattr(pod, "status", None)
            labels = getattr(metadata, "labels", None) or {}
            pool_status = labels.get(self.LABEL_STATUS)
            deletion_timestamp = getattr(metadata, "deletion_timestamp", None)
            counted = (
                deletion_timestamp is None
                and pool_status in {self.STATUS_AVAILABLE, self.STATUS_ASSIGNED}
            )

            if deletion_timestamp is not None:
                terminating += 1
            elif pool_status == self.STATUS_AVAILABLE:
                pool_available += 1
                pool_total += 1
            elif pool_status == self.STATUS_ASSIGNED:
                pool_assigned += 1
                pool_total += 1
                if self._has_controller_owner(pod, "ReplicaSet"):
                    assigned_with_replicaset_owner += 1

            ready = self._pod_is_ready(pod)
            if (
                counted
                and pool_status == self.STATUS_AVAILABLE
                and ready
                and getattr(status, "pod_ip", None)
            ):
                ready_available += 1
                available_candidates.append(
                    {
                        "name": getattr(metadata, "name", "") or "",
                        "ip": getattr(status, "pod_ip", "") or "",
                        "ready_at": self._pod_ready_at(pod),
                        "creation_timestamp": getattr(
                            metadata, "creation_timestamp", None
                        ),
                        "resource_version": getattr(
                            metadata, "resource_version", None
                        )
                        or "",
                        "annotations": dict(
                            getattr(metadata, "annotations", None) or {}
                        ),
                    }
                )

            annotations = getattr(metadata, "annotations", None) or {}
            pod_items.append(
                {
                    "name": getattr(metadata, "name", "") or "",
                    "phase": getattr(status, "phase", None) or "Unknown",
                    "compute_type": labels.get(self.LABEL_COMPUTE_TYPE, "unknown"),
                    "pool_status": pool_status or "unknown",
                    "assigned_user": labels.get(self.LABEL_USER, ""),
                    "ready": ready,
                    "ip": getattr(status, "pod_ip", None),
                    "terminating": deletion_timestamp is not None,
                    "counted_in_pool_total": counted,
                    "replicaset_owned": self._has_controller_owner(pod, "ReplicaSet"),
                    "allocation_ticket_id": annotations.get(
                        self.ANNOTATION_ALLOCATION_TICKET,
                        "",
                    ),
                    "allocation_claim_token": annotations.get(
                        self.ANNOTATION_ALLOCATION_CLAIM,
                        "",
                    ),
                }
            )

        available_candidates.sort(
            key=lambda item: (
                str(item.get("creation_timestamp") or ""),
                item.get("name") or "",
            )
        )
        return {
            "compute_type": (compute_type or "").strip().lower(),
            "pool_total": pool_total,
            "pool_available": pool_available,
            "pool_assigned": pool_assigned,
            "ready_available": ready_available,
            "terminating": terminating,
            "physical_total": len(pods.items),
            "assigned_with_replicaset_owner": assigned_with_replicaset_owner,
            "available_candidates": available_candidates,
            "pods": pod_items,
        }

    def find_reserved_pod(
        self,
        ticket_id: str,
        claim_token: str = "",
        compute_type: Optional[str] = None,
    ) -> Optional[str]:
        """Find the Pod journaled by an allocating ticket after a crash."""
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value:
            return None
        for pod in self.list_pool_snapshot(compute_type)["pods"]:
            if pod.get("terminating"):
                continue
            if pod.get("pool_status") != self.STATUS_ASSIGNED:
                continue
            if pod.get("allocation_ticket_id") != ticket_id_value:
                continue
            recorded_claim = pod.get("allocation_claim_token") or ""
            if claim_token and recorded_claim != claim_token:
                continue
            return pod.get("name") or None
        return None

    def read_deployment_replicas(self, deployment_name: str) -> int:
        scale = self.apps_v1.read_namespaced_deployment_scale(
            name=deployment_name,
            namespace=self.namespace,
            _request_timeout=self.api_request_timeout,
        )
        return int(getattr(getattr(scale, "spec", None), "replicas", 0) or 0)

    def patch_deployment_replicas(
        self,
        deployment_name: str,
        replicas: int,
    ) -> int:
        desired = max(0, int(replicas))
        scale = self.apps_v1.patch_namespaced_deployment_scale(
            name=deployment_name,
            namespace=self.namespace,
            body={"spec": {"replicas": desired}},
            _request_timeout=self.api_request_timeout,
        )
        return int(getattr(getattr(scale, "spec", None), "replicas", desired) or 0)

    def assign_pod(
        self,
        pod_name: str,
        user_pod: str,
        expected_resource_version: Optional[str] = None,
        ticket_id: str = "",
        claim_token: str = "",
        expected_annotations: Optional[Dict] = None,
    ) -> None:
        """
        Mark an available warm pod as assigned.

        Because pool-status=available is part of the Deployment selector,
        changing it to assigned removes the pod from warm-pool membership and
        lets the Deployment backfill a new warm pod.
        """
        try:
            patch = []
            if expected_resource_version:
                patch.append(
                    {
                        "op": "test",
                        "path": "/metadata/resourceVersion",
                        "value": expected_resource_version,
                    }
                )
            if ticket_id:
                annotations = dict(expected_annotations or {})
                annotations[self.ANNOTATION_ALLOCATION_TICKET] = ticket_id
                annotations[self.ANNOTATION_ALLOCATION_CLAIM] = claim_token or ""
                patch.append(
                    {
                        "op": "add",
                        "path": "/metadata/annotations",
                        "value": annotations,
                    }
                )
            patch.extend([
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
                    "path": "/metadata/labels/assigned-user",
                    "value": user_pod,
                },
            ])
            self.v1.api_client.call_api(
                "/api/v1/namespaces/{namespace}/pods/{name}",
                "PATCH",
                path_params={"namespace": self.namespace, "name": pod_name},
                body=patch,
                header_params={"Content-Type": "application/json-patch+json"},
                auth_settings=["BearerToken"],
                _return_http_data_only=True,
                _preload_content=True,
                _request_timeout=self.api_request_timeout,
            )
        except ApiException as e:
            if e.status in (404, 409, 422):
                raise PodConflictError(
                    f"{pod_name} is no longer assignable"
                ) from e
            raise

    def release_pod(self, pod_name: str) -> bool:
        """
        Delete an assigned compute pod so the Deployment can backfill a new
        warm pod. This path is idempotent so duplicate release notifications
        or concurrent cleanup attempts do not fail the controller.
        """
        try:
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=0,
                _request_timeout=self.api_request_timeout,
            )
            logger.debug("[ComputeDeleted] compute_pod=%s", pod_name)
            return True
        except ApiException as e:
            if e.status in (404, 409):
                logger.debug("[ComputeDeleted] compute_pod=%s status=already_deleted_or_terminating", pod_name)
                return False
            raise

    def list_pool_status(self, compute_type: Optional[str] = None) -> List[Dict]:
        """
        List compute pods managed by this controller.
        """
        snapshot = self.list_pool_snapshot(compute_type=compute_type)
        return [
            {
                **pod,
                "app": self.APP_WARM_POOL,
            }
            for pod in snapshot["pods"]
        ]
