"""Deployment 없이 컨트롤러가 직접 관리하는 웜 버퍼의 파드 공급자.

cold start 공급자(요청마다 파드를 만들고 반납 때 지운다)에 "미리 만들어 둔 파드"
하나를 더한 것이다. 파드는 두 상태다.

  idle      미리 만든 파드. 부팅 중이거나 Ready 로 대기한다. 아직 요청이 없다.
  assigned  요청에 묶인 파드. 미리 만든 파드를 할당했거나 요청 때 새로 만들었다.

ReplicaSet 이 없으므로 replicas·게이트·축소 게이트가 없고, 어떤 파드를 지울지는
항상 컨트롤러가 고른다. idle 라벨 값은 일부러 "available" 이 아니다 - 같은
네임스페이스에 남아 있는 compute-general Deployment(셀렉터 compute-status=available)의
ReplicaSet 이 주인 없는 파드로 보고 입양해 지우지 못하게 하기 위해서다.
"""

import copy
import glob
import logging
import os
import re
import time
import uuid
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
    allocation_mode = "warm_buffer_direct"
    # 리더의 버퍼 조정이 파드 이벤트로 깨어나고, 생성 시간 표본도 이벤트에서 얻는다.
    uses_availability_watch = True

    LABEL_APP = "app"
    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "compute-status"
    LABEL_USER = "assigned-user"

    APP_COMPUTE_POD = "compute-pod"

    STATUS_IDLE = "idle"
    STATUS_ASSIGNED = "assigned"
    # compute watcher 와 상태 보고가 쓰는 이름. 이 arm 에서 "할당 가능" 은 idle 이다.
    STATUS_AVAILABLE = STATUS_IDLE

    ANNOTATION_ALLOCATION_TICKET = "k8s-dynamic-allocator/allocation-ticket-id"
    ANNOTATION_ALLOCATION_CLAIM = "k8s-dynamic-allocator/allocation-claim-token"
    # idle 로 만든 파드인지 요청 때 만든 파드인지. 분석에서 "미리 만든 파드를 썼나" 를
    # 가른다. 할당해도 바뀌지 않는다.
    ANNOTATION_ORIGIN = "k8s-dynamic-allocator/origin"
    ORIGIN_IDLE = "idle"
    ORIGIN_REQUEST = "request"

    def __init__(self):
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self._templates_by_type: Dict[str, Dict] = {}

    # ------------------------------------------------------------------ 설정

    def initialize_buffer(self, *, log_existing: bool = True) -> Dict:
        """파드 템플릿을 읽고 검증한다. 파드는 만들지 않는다(리더의 조정이 만든다)."""
        results = {"created": [], "existing": [], "failed": [], "templates": []}
        self._templates_by_type = {}

        if log_existing:
            capacity = self.read_capacity()
            reserve = self.read_reserve()
            if capacity is None:
                logger.error(
                    "[Failed] operation=direct_buffer_capacity reason=%r",
                    "no capacity (BUFFER_CAPACITY) configured; no compute pod will be created",
                )
            logger.info("Direct warm buffer policy: R=%s N=%s", reserve, capacity)

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
            logger.info("Direct warm buffer pod templates loaded: %s", results["templates"])
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
        """N. None 이면 설정이 없는 것이고, 할당과 버퍼 조정은 파드를 하나도 만들지 않는다
        (상한 없는 run 이 조용히 생기지 않게. 시작 로그에 오류가 남는다)."""
        del compute_type
        return self._parse_count(settings.WARM_BUFFER_CAPACITY, "N")

    def read_reserve(self, compute_type: Optional[str] = None) -> int:
        """R. 비었거나 잘못되면 0 - 미리 만들지 않는 cold start 와 같아진다."""
        del compute_type
        return self._parse_count(settings.WARM_BUFFER_RESERVE, "R") or 0

    # ------------------------------------------------------------------ 목록

    @timing.timed("k8s.list_snapshot")
    def snapshot(self, compute_type: Optional[str] = None) -> Dict:
        """한 번의 LIST 로 버퍼 상태를 만든다.

        LIST 는 apiserver 의 watch 캐시가 최신 상태로 응답한다(일관 읽기, 10/11 실측:
        캐시 응답 = 전체, etcd 로 빠짐 0). 그래서 매 패스 읽어도 etcd 부담이 작다.
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
            "allocation_ticket_id": annotations.get(self.ANNOTATION_ALLOCATION_TICKET, ""),
            "allocation_claim_token": annotations.get(self.ANNOTATION_ALLOCATION_CLAIM, ""),
            "phase": getattr(status, "phase", None),
            "ready": self._pod_is_ready(pod),
            "ready_at": self._pod_ready_at(pod) if self._pod_is_ready(pod) else None,
            "not_ready_since": self._pod_not_ready_since(pod),
            "terminating": getattr(metadata, "deletion_timestamp", None) is not None,
            "ip": getattr(status, "pod_ip", None),
            "node": getattr(pod.spec, "node_name", None),
        }

    def list_buffer_status(self, compute_type: Optional[str] = None) -> List[Dict]:
        return self.snapshot(compute_type=compute_type)["pods"]

    def active_pod_names(self, compute_type: Optional[str] = None) -> set:
        return self.snapshot(compute_type=compute_type)["active_names"]

    def count_active_pods(self, compute_type: Optional[str] = None) -> int:
        return len(self.active_pod_names(compute_type=compute_type))

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

    IDLE_NAME_PATTERN = re.compile(r"^compute-[a-z0-9-]+-warm-[0-9a-f]{10}$")

    def new_idle_name(self, compute_type: str) -> str:
        """미리 만들 파드의 이름. 요청 전에 정해 N 계산의 "생성 중" 목록에 먼저 올린다.

        무작위 꼬리를 쓴다. 순번은 초기화 때 Redis 와 함께 지워져, 앞 run 의 지워지는
        중인 파드와 이름이 겹칠 수 있다.
        """
        type_value = re.sub(r"[^a-z0-9-]", "-", compute_type.lower()).strip("-") or "general"
        return f"compute-{type_value}-warm-{uuid.uuid4().hex[:10]}"

    def is_idle_name(self, pod_name: str) -> bool:
        """생성 중 목록의 이름이 미리 만드는 파드인지(아니면 요청 파드)."""
        return bool(self.IDLE_NAME_PATTERN.match(pod_name or ""))

    def request_pod_name(self, ticket: Dict) -> str:
        """요청 하나에 묶어 만들 파드의 이름. 만들기 전에 티켓과 생성 중 목록에 올린다."""
        compute_type = self._normalize_compute_type(ticket.get("compute_type"))
        return self._pod_name_for_ticket(ticket, compute_type)

    @timing.timed("k8s.create_pod")
    def create_idle_pod(self, compute_type: str, pod_name: str) -> None:
        """요청 없이 파드를 하나 만든다. 409(같은 이름 존재)는 예외로 올린다."""
        body = self._pod_body(
            compute_type,
            pod_name,
            status=self.STATUS_IDLE,
            user="",
            annotations={self.ANNOTATION_ORIGIN: self.ORIGIN_IDLE},
        )
        self.v1.create_namespaced_pod(
            namespace=self.namespace,
            body=body,
            _request_timeout=self.api_request_timeout,
        )

    @timing.timed("k8s.create_pod")
    def create_pod_for_ticket(self, ticket: Dict, pod_name: Optional[str] = None) -> str:
        """요청 하나에 묶인 파드를 만든다(cold start 와 같은 경로)."""
        compute_type = self._normalize_compute_type(ticket.get("compute_type"))
        pod_name = pod_name or self._pod_name_for_ticket(ticket, compute_type)
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
        metadata["annotations"] = merged
        return body

    # ------------------------------------------------------------------ 할당·삭제

    @timing.timed("k8s.patch_assign")
    def claim_idle_pod(
        self,
        pod_name: str,
        expected_resource_version: str,
        user_pod: str,
        ticket_id: str,
        claim_token: str,
        expected_annotations: Optional[Dict] = None,
    ) -> bool:
        """idle 파드를 요청에 묶는다. 읽은 뒤 바뀌었으면(삭제 시작 포함) 아무것도 안 하고 False.

        resourceVersion 을 test 하므로, 그사이 다른 복제본이 할당했거나 리더가 지우기
        시작했으면(deletionTimestamp 가 붙으면 버전이 바뀐다) 실패한다.
        """
        if not pod_name or not expected_resource_version:
            return False
        annotations = dict(expected_annotations or {})
        annotations[self.ANNOTATION_ALLOCATION_TICKET] = ticket_id or ""
        annotations[self.ANNOTATION_ALLOCATION_CLAIM] = claim_token or ""
        patch = [
            {"op": "test", "path": "/metadata/resourceVersion", "value": expected_resource_version},
            {"op": "test", "path": "/metadata/labels/app", "value": self.APP_COMPUTE_POD},
            {"op": "test", "path": "/metadata/labels/compute-status", "value": self.STATUS_IDLE},
            {"op": "add", "path": "/metadata/annotations", "value": annotations},
            {"op": "replace", "path": "/metadata/labels/compute-status", "value": self.STATUS_ASSIGNED},
            {"op": "replace", "path": "/metadata/labels/assigned-user", "value": user_pod or "unknown"},
        ]
        try:
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
        except ApiException as exc:
            if exc.status in (404, 409, 422):
                return False
            raise
        return True

    @timing.timed("k8s.delete_pod")
    def delete_idle_pod(self, pod_name: str, uid: str, expected_resource_version: str) -> bool:
        """idle 파드를 지운다. 읽은 뒤 바뀌었으면(그사이 할당됐으면) 지우지 않고 False.

        삭제 전제 조건(uid + resourceVersion)이라 할당과 같은 파드를 두고 겹쳐도
        할당된 파드를 지우는 일이 없다.
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
        must_exist: bool = False,
    ):
        """파드가 Ready 가 될 때까지 기다린다.

        이미지가 느리면 티켓의 allocating TTL 보다 오래 걸릴 수 있다. keep_claim 이
        기다리는 동안 claim 을 연장하고, 티켓이 소유권을 잃으면 멈춘다.

        404 는 생성 직후 한때만 "아직 안 보임" 으로 본다. 한 번 본 파드(또는 이미 있던
        idle, must_exist)가 404 면 지워진 것이다 - 우리 삭제는 grace 0 이라 Terminating
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
        seen = bool(must_exist)

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

    def get_pod_ready_at(self, pod_name: str):
        try:
            pod = self.v1.read_namespaced_pod(
                name=pod_name,
                namespace=self.namespace,
                _request_timeout=self.api_request_timeout,
            )
        except ApiException as exc:
            if exc.status == 404:
                return None
            raise
        return self._pod_ready_at(pod)

    # ------------------------------------------------------------------ 기타

    def drain_buffer_deployments(self) -> int:
        """남아 있는 Deployment 버퍼(develop 의 warm_buffer arm)를 0 으로 내린다.

        이 arm 은 그 파드를 할당하지 않는다. 그대로 두면 노드 자원을 쥔 채 이 run 의
        점유량에 섞인다. 모드 전환 때 이전 리더가 몇 초 더 살아 다시 올릴 수 있어
        리더 조정이 주기적으로 다시 확인한다(cold start 와 같은 이유).
        Deployment 자체는 지우지 않는다 - develop arm 으로 돌아갈 때 그대로 쓴다.
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
        prefix = f"compute-{compute_type_value}-req-"
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
