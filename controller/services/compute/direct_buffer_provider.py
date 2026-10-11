"""cold start 의 파드 공급자. 요청마다 파드를 만들고 반납 때 지운다.

제안(warm buffer)의 공급자에서 "미리 만들어 둔 파드" 를 뺀 것이다. 이 arm 이 만드는
파드는 모두 assigned(요청에 묶인 파드)다. idle 은 다른 arm 이 남긴 파드를 알아보고
지우기 위해서만 읽는다. Deployment 는 쓰지 않는다.
"""

import copy
import glob
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import urllib3
import yaml
from kubernetes import client
from kubernetes.client.rest import ApiException

from config import settings

from ..infra.kubernetes_client import KubernetesClient
from .manifest_images import override_compute_agent_image

try:
    from .. import timing
except ImportError:  # 단위 테스트는 이 파일 하나만 따로 읽어 패키지가 없다
    class timing:  # noqa: N801 - 모듈 대역. 계측이 꺼진 것과 같게 동작한다.
        ENABLED = False

        @staticmethod
        def timed(kind):
            return lambda fn: fn

logger = logging.getLogger(__name__)


class AllocationClaimLost(RuntimeError):
    """파드가 뜨는 동안 티켓이 할당 소유권을 잃었다."""


MANIFESTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "manifests",
    "direct_buffer",
)


class DirectBufferProvider(KubernetesClient):
    allocation_mode = "cold_start"
    # 리더의 남은 idle 정리가 파드 이벤트로 깨어난다.
    uses_availability_watch = True

    LABEL_APP = "app"
    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "compute-status"
    LABEL_USER = "assigned-user"

    APP_COMPUTE_POD = "compute-pod"

    # 다른 arm(warm buffer)이 미리 만든 파드. 이 arm 은 만들지도 내주지도 않고 지운다.
    STATUS_IDLE = "idle"
    STATUS_ASSIGNED = "assigned"
    # compute watcher 와 상태 보고가 쓰는 이름. 이 arm 에는 할당 가능한 파드가 없다.
    STATUS_AVAILABLE = STATUS_IDLE

    ANNOTATION_ALLOCATION_TICKET = "k8s-dynamic-allocator/allocation-ticket-id"
    ANNOTATION_ALLOCATION_CLAIM = "k8s-dynamic-allocator/allocation-claim-token"
    # 파드를 만든 경로. 이 arm 은 늘 request 다. 분석 도구가 arm 과 상관없이 같은
    # 어노테이션으로 "미리 만든 파드를 썼나" 를 가르므로 남긴다.
    ANNOTATION_ORIGIN = "k8s-dynamic-allocator/origin"
    # 파드를 만든 템플릿(이미지 포함)의 세대. 기록용이다(arm 과 상관없이 같은 형식).
    ANNOTATION_TEMPLATE_HASH = "k8s-dynamic-allocator/template-hash"
    ORIGIN_REQUEST = "request"

    def __init__(self):
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self._templates_by_type: Dict[str, Dict] = {}
        self._template_hash_by_type: Dict[str, str] = {}

    # ------------------------------------------------------------------ 설정

    def initialize_buffer(self, *, log_existing: bool = True) -> Dict:
        """파드 템플릿을 읽고 검증한다. 파드는 만들지 않는다(요청이 올 때 할당이 만든다)."""
        results = {"created": [], "existing": [], "failed": [], "templates": []}
        self._templates_by_type = {}
        self._template_hash_by_type = {}

        if log_existing:
            capacity = self.read_capacity()
            if capacity is None:
                logger.error(
                    "[Failed] operation=direct_buffer_capacity reason=%r",
                    "no capacity (BUFFER_CAPACITY) configured; no compute pod will be created",
                )
            logger.info("Cold start policy: N=%s", capacity)

        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        if not yaml_files:
            logger.warning(
                "[Warning] operation=direct_buffer_manifest_discovery manifest_dir=%s reason=%r",
                MANIFESTS_DIR,
                "no manifest files found",
            )
            return results

        for yaml_file in yaml_files:
            try:
                with open(yaml_file, encoding="utf-8") as handle:
                    spec = yaml.safe_load(handle)
                override_compute_agent_image(spec, settings.COMPUTE_POD_IMAGE)
                compute_type = self._validate_pod_template(spec)
                self._templates_by_type[compute_type] = spec
                self._template_hash_by_type[compute_type] = self._hash_template(spec)
                results["templates"].append(
                    {"file": os.path.basename(yaml_file), "compute_type": compute_type}
                )
            except Exception as exc:
                logger.error(
                    "[Failed] operation=direct_buffer_manifest_load manifest=%s reason=%r",
                    os.path.basename(yaml_file),
                    str(exc),
                )
                results["failed"].append({"file": os.path.basename(yaml_file), "error": str(exc)})

        if results["templates"] and log_existing:
            logger.info("Cold start pod templates loaded: %s", results["templates"])
        return results

    @staticmethod
    def _parse_count(raw: str, name: str) -> Optional[int]:
        if raw is None or str(raw).strip() == "":
            return None
        try:
            return max(0, int(str(raw).strip()))
        except (TypeError, ValueError):
            logger.warning(
                "[Warning] operation=direct_buffer_policy name=%s reason=%r",
                name,
                f"not an integer: {raw!r}",
            )
            return None

    def read_capacity(self, compute_type: Optional[str] = None) -> Optional[int]:
        """N. None 이면 설정이 없는 것이고, 할당은 파드를 하나도 만들지 않는다
        (상한 없는 run 이 조용히 생기지 않게. 시작 로그에 오류가 남는다)."""
        del compute_type
        return self._parse_count(settings.WARM_BUFFER_CAPACITY, "N")

    # ------------------------------------------------------------------ 목록

    @timing.timed("k8s.list_snapshot")
    def snapshot(self, compute_type: Optional[str] = None) -> Dict:
        """한 번의 LIST 로 버퍼 상태를 만든다.

        LIST 는 apiserver 의 watch 캐시가 일관 읽기로 응답한다. 그래서 매 패스 읽어도
        etcd 부담이 작다.
        """
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=self._compute_selector(compute_type=compute_type),
            _request_timeout=self.api_request_timeout,
        )
        items = [self._pod_info(pod) for pod in pods.items]
        live = [p for p in items if not p["terminating"]]
        idle = [p for p in live if p["buffer_status"] == self.STATUS_IDLE]
        return {
            "pods": items,
            # 자리를 쥔 파드. 지우는 중인 파드는 자리를 돌려주는 중이라 세지 않는다
            # (cold start 의 N 정의와 같다).
            "active_names": {p["name"] for p in live},
            "idle": idle,
            "idle_ready": [p for p in idle if p["ready"]],
            "assigned": sum(1 for p in live if p["buffer_status"] == self.STATUS_ASSIGNED),
        }

    def _pod_info(self, pod) -> Dict:
        metadata = pod.metadata
        labels = metadata.labels or {}
        annotations = metadata.annotations or {}
        status = pod.status
        ready = self._pod_is_ready(pod)
        return {
            "name": metadata.name,
            "uid": metadata.uid,
            "resource_version": metadata.resource_version,
            "created": metadata.creation_timestamp,
            "annotations": dict(annotations),
            "app": labels.get(self.LABEL_APP, "unknown"),
            "compute_type": labels.get(self.LABEL_COMPUTE_TYPE, "unknown"),
            "buffer_status": labels.get(self.LABEL_STATUS, "unknown"),
            "assigned_user": labels.get(self.LABEL_USER, ""),
            "origin": annotations.get(self.ANNOTATION_ORIGIN, ""),
            "template_hash": annotations.get(self.ANNOTATION_TEMPLATE_HASH, ""),
            "allocation_ticket_id": annotations.get(self.ANNOTATION_ALLOCATION_TICKET, ""),
            "allocation_claim_token": annotations.get(self.ANNOTATION_ALLOCATION_CLAIM, ""),
            "phase": getattr(status, "phase", None),
            "ready": ready,
            "ready_at": self._pod_ready_at(pod) if ready else None,
            "not_ready_since": self._pod_not_ready_since(pod),
            "terminating": getattr(metadata, "deletion_timestamp", None) is not None,
            "ip": getattr(status, "pod_ip", None),
            "node": getattr(pod.spec, "node_name", None),
        }

    def list_buffer_status(self, compute_type: Optional[str] = None) -> List[Dict]:
        return self.snapshot(compute_type=compute_type)["pods"]

    def find_reserved_pod(
        self,
        ticket_id: str,
        claim_token: str = "",
        compute_type: Optional[str] = None,
    ) -> Optional[str]:
        """할당 도중 죽은 티켓이 표시해 둔 파드를 찾는다(stale 복구용)."""
        ticket_id_value = (ticket_id or "").strip()
        if not ticket_id_value:
            return None
        for pod in self.snapshot(compute_type)["pods"]:
            if pod["terminating"] or pod["buffer_status"] != self.STATUS_ASSIGNED:
                continue
            if pod["allocation_ticket_id"] != ticket_id_value:
                continue
            if claim_token and pod["allocation_claim_token"] != claim_token:
                continue
            return pod["name"]
        return None

    # ------------------------------------------------------------------ 생성

    # 파드 이름은 arm 과 상관없이 같은 규칙이다: compute-<타입>-<hex 10>. 어느 경로로
    # 만들었는지는 어노테이션 origin 이 기록한다(분석용). 이름으로 구분하지 않는다.

    @staticmethod
    def _pod_name(compute_type: str, suffix: str) -> str:
        type_value = re.sub(r"[^a-z0-9-]", "-", compute_type.lower()).strip("-") or "general"
        return f"compute-{type_value}-{suffix}"

    def request_pod_name(self, ticket: Dict) -> str:
        """요청 하나에 묶어 만들 파드의 이름. 만들기 전에 티켓과 생성 중 목록에 올린다.

        할당 락 안의 예약과 생성 스레드가 같은 이름을 알아야 하므로 티켓과 재시도 횟수에서
        정해진다(해시). 재시도마다 이름이 달라 앞 시도의 파드와 겹치지 않는다.
        """
        compute_type = self._normalize_compute_type(ticket.get("compute_type"))
        try:
            retry_count = int(ticket.get("retry_count") or 0)
        except (TypeError, ValueError):
            retry_count = 0
        digest = hashlib.sha256(f"{ticket.get('ticket_id') or ''}:{retry_count}".encode("utf-8")).hexdigest()
        return self._pod_name(compute_type, digest[:10])

    @timing.timed("k8s.create_pod")
    def create_pod_for_ticket(self, ticket: Dict, pod_name: Optional[str] = None) -> str:
        """요청 하나에 묶인 파드를 만든다(cold start 와 같은 경로)."""
        compute_type = self._normalize_compute_type(ticket.get("compute_type"))
        pod_name = pod_name or self.request_pod_name(ticket)
        user_pod = (ticket.get("user_pod") or "unknown").strip() or "unknown"
        body = self._pod_body(
            compute_type,
            pod_name,
            status=self.STATUS_ASSIGNED,
            user=user_pod,
            annotations={
                self.ANNOTATION_ORIGIN: self.ORIGIN_REQUEST,
                self.ANNOTATION_ALLOCATION_TICKET: str(ticket.get("ticket_id") or ""),
                self.ANNOTATION_ALLOCATION_CLAIM: str(ticket.get("claim_token") or ""),
            },
        )
        try:
            self.v1.create_namespaced_pod(
                namespace=self.namespace,
                body=body,
                _request_timeout=self.api_request_timeout,
            )
            return pod_name
        except ApiException as exc:
            if exc.status == 409:
                raise RuntimeError(f"Compute pod already exists: {pod_name}") from exc
            raise

    def _pod_body(self, compute_type: str, pod_name: str, *, status: str, user: str, annotations: Dict) -> Dict:
        template = self._template_for_compute_type(compute_type)
        body = copy.deepcopy(template)
        metadata = body.setdefault("metadata", {})
        metadata["name"] = pod_name
        metadata.pop("generateName", None)
        metadata.pop("ownerReferences", None)
        metadata["namespace"] = self.namespace
        labels = metadata.setdefault("labels", {})
        labels[self.LABEL_APP] = self.APP_COMPUTE_POD
        labels[self.LABEL_COMPUTE_TYPE] = compute_type
        labels[self.LABEL_STATUS] = status
        labels[self.LABEL_USER] = user
        merged = dict(metadata.get("annotations") or {})
        merged.update(annotations)
        merged[self.ANNOTATION_TEMPLATE_HASH] = self.template_hash(compute_type)
        metadata["annotations"] = merged
        return body

    # ------------------------------------------------------------------ 삭제

    @timing.timed("k8s.delete_pod")
    def delete_idle_pod(self, pod_name: str, uid: str, expected_resource_version: str) -> bool:
        """남은 idle 파드를 지운다. 읽은 뒤 바뀌었으면 지우지 않고 False.

        삭제 전제 조건(uid + resourceVersion)이라 그사이 다른 쪽이 바꾼 파드는 지우지 않는다.
        """
        try:
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=0,
                body=client.V1DeleteOptions(
                    grace_period_seconds=0,
                    preconditions=client.V1Preconditions(
                        uid=uid,
                        resource_version=expected_resource_version,
                    ),
                ),
                _request_timeout=self.api_request_timeout,
            )
            return True
        except ApiException as exc:
            if exc.status in (404, 409):
                return False
            raise

    @timing.timed("k8s.delete_pod")
    def release_pod(self, pod_name: str) -> bool:
        """할당된 파드를 지운다. 이미 없거나 지우는 중이면 False."""
        if not pod_name:
            return False
        try:
            self.v1.delete_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                grace_period_seconds=0,
                _request_timeout=self.api_request_timeout,
            )
            logger.debug("[ComputeDeleted] compute_pod=%s", pod_name)
            return True
        except ApiException as exc:
            if exc.status in (404, 409):
                logger.debug("[ComputeDeleted] compute_pod=%s status=already_deleted_or_terminating", pod_name)
                return False
            raise

    # ------------------------------------------------------------------ Ready 대기

    def wait_pod_ready(
        self,
        pod_name: str,
        keep_claim: Optional[Callable[[], bool]] = None,
    ):
        """파드가 Ready 가 될 때까지 기다린다.

        이미지가 느리면 티켓의 allocating TTL 보다 오래 걸릴 수 있다. keep_claim 이
        기다리는 동안 claim 을 연장하고, 티켓이 소유권을 잃으면 멈춘다.

        404 는 생성 직후 한때만 "아직 안 보임" 으로 본다. 한 번 본 파드가 404 면 지워진
        것이다 - 우리 삭제는 grace 0 이라 Terminating
        단계 없이 바로 사라지므로, 그때 기다리면 Ready 대기 상한(300초)을 통째로 쓴다.
        읽기 실패(timeout, 5xx, 429)는 상한 안에서 다시 읽는다. 혼잡한 apiserver 에서
        한 번의 실패로 할당 시도를 버리면 재시도 예산이 줄고 파드를 새로 만든다.
        """
        timeout = max(1.0, float(settings.COLD_START_POD_READY_TIMEOUT_SECONDS))
        poll = max(0.2, float(settings.COLD_START_POD_READY_POLL_SECONDS))
        renew_interval = max(1.0, float(settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS) / 3.0)
        deadline = time.monotonic() + timeout
        next_renew = time.monotonic() + renew_interval
        last_phase = ""
        seen = False

        while time.monotonic() < deadline:
            if keep_claim is not None and time.monotonic() >= next_renew:
                if not keep_claim():
                    raise AllocationClaimLost(
                        f"Ticket no longer owns the allocation while waiting for {pod_name}"
                    )
                next_renew = time.monotonic() + renew_interval
            try:
                pod = self.v1.read_namespaced_pod(
                    name=pod_name,
                    namespace=self.namespace,
                    _request_timeout=self.api_request_timeout,
                )
            except ApiException as exc:
                if exc.status == 404:
                    if seen:
                        raise RuntimeError(f"Compute pod was deleted before Ready: {pod_name}")
                    time.sleep(poll)
                    continue
                if exc.status == 429 or (exc.status or 0) >= 500:
                    logger.debug("[ComputeReadyPollRetry] compute_pod=%s status=%s", pod_name, exc.status)
                    time.sleep(poll)
                    continue
                raise
            except (urllib3.exceptions.HTTPError, OSError) as exc:
                logger.debug("[ComputeReadyPollRetry] compute_pod=%s reason=%r", pod_name, str(exc))
                time.sleep(poll)
                continue

            seen = True
            if getattr(pod.metadata, "deletion_timestamp", None) is not None:
                raise RuntimeError(f"Compute pod is being deleted before Ready: {pod_name}")
            last_phase = getattr(pod.status, "phase", "") or ""
            if self._pod_is_ready(pod):
                return pod
            if last_phase in {"Failed", "Succeeded"}:
                raise RuntimeError(f"Compute pod ended before Ready: {pod_name} phase={last_phase}")
            time.sleep(poll)

        raise TimeoutError(f"Timed out waiting for compute pod Ready: {pod_name} phase={last_phase}")

    # ------------------------------------------------------------------ 기타

    def drain_buffer_deployments(self) -> int:
        """다른 arm(develop, reuse)의 compute Deployment 가 있으면 0 으로 내린다.

        이 arm 은 Deployment 를 쓰지 않으므로 배포와 실험 스크립트가 지운다. 그래도 arm
        전환 때 이전 컨트롤러가 몇 초 더 살아 다시 만들 수 있어, 리더가 주기적으로
        확인해 그 파드가 노드 자원과 N 을 쥐지 못하게 한다. 지우는 것은 배포 쪽 일이다.
        """
        scaled = 0
        deployments = self.apps_v1.list_namespaced_deployment(
            namespace=self.namespace,
            label_selector=f"{self.LABEL_APP}={self.APP_COMPUTE_POD}",
            _request_timeout=self.api_request_timeout,
        )
        for deployment in deployments.items:
            name = deployment.metadata.name
            if (getattr(deployment.spec, "replicas", 0) or 0) == 0:
                continue
            try:
                self.apps_v1.patch_namespaced_deployment_scale(
                    name=name,
                    namespace=self.namespace,
                    body={"spec": {"replicas": 0}},
                    _request_timeout=self.api_request_timeout,
                )
            except ApiException as exc:
                logger.warning(
                    "[Warning] operation=direct_buffer_drain_deployment deployment=%s reason=%r",
                    name,
                    str(exc),
                )
                continue
            scaled += 1
            logger.info("[Drained] deployment=%s replicas=0 reason=direct_buffer_mode", name)
        return scaled

    @staticmethod
    def _hash_template(spec: Dict) -> str:
        return hashlib.sha256(json.dumps(spec, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]

    def template_hash(self, compute_type: str) -> str:
        """지금 템플릿(이미지 덮어쓰기 포함)의 세대. 파드에 붙은 값과 다르면 옛 세대다."""
        compute_type_value = self._normalize_compute_type(compute_type)
        if compute_type_value not in self._template_hash_by_type:
            self._template_for_compute_type(compute_type_value)
        return self._template_hash_by_type.get(compute_type_value, "")

    def _template_for_compute_type(self, compute_type: str) -> Dict:
        if compute_type not in self._templates_by_type:
            self.initialize_buffer(log_existing=False)
        template = self._templates_by_type.get(compute_type)
        if not template:
            raise ValueError(f"No compute pod template for compute_type={compute_type}")
        return template

    def _validate_pod_template(self, spec: Dict) -> str:
        if not isinstance(spec, dict):
            raise ValueError("Invalid manifest: expected mapping")
        if spec.get("kind") != "Pod":
            raise ValueError("Compute manifest must be kind=Pod")
        labels = spec.get("metadata", {}).get("labels", {})
        if labels.get(self.LABEL_APP) != self.APP_COMPUTE_POD:
            raise ValueError("Compute pod template must set app=compute-pod")
        compute_type = labels.get(self.LABEL_COMPUTE_TYPE)
        if not compute_type:
            raise ValueError("Compute pod template must set compute-type")
        if self.LABEL_USER not in labels or self.LABEL_STATUS not in labels:
            raise ValueError("Compute pod template must define compute-status and assigned-user")
        containers = spec.get("spec", {}).get("containers") or []
        if not containers:
            raise ValueError("Compute pod template must define containers")
        return self._normalize_compute_type(compute_type)

    def _compute_selector(self, compute_type: Optional[str] = None) -> str:
        parts = [f"{self.LABEL_APP}={self.APP_COMPUTE_POD}"]
        if compute_type:
            parts.append(f"{self.LABEL_COMPUTE_TYPE}={self._normalize_compute_type(compute_type)}")
        return ",".join(parts)

    def _normalize_compute_type(self, compute_type: Optional[str]) -> str:
        value = (compute_type or settings.DEFAULT_COMPUTE_TYPE or "general").strip().lower()
        return value or "general"

    @staticmethod
    def _pod_is_ready(pod) -> bool:
        if getattr(pod.metadata, "deletion_timestamp", None):
            return False
        if getattr(pod.status, "phase", None) != "Running":
            return False
        for condition in getattr(pod.status, "conditions", None) or []:
            if condition.type == "Ready":
                return condition.status == "True"
        return False

    @staticmethod
    def _pod_ready_at(pod):
        for condition in getattr(pod.status, "conditions", None) or []:
            if condition.type == "Ready" and condition.status == "True":
                return getattr(condition, "last_transition_time", None)
        return datetime.now(timezone.utc)
