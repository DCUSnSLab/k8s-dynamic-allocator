"""Deployment 없이 컨트롤러가 직접 관리하는 웜 버퍼의 파드 공급자(재사용 arm).

반납된 파드는 정리(scrub)한 뒤 버퍼 목표 안이면 idle 로 되돌린다(return_pod). 나머지는
제안 arm 과 같다.

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
import hashlib
import json
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


class PodReturnConflict(RuntimeError):
    """되돌리려던 파드가 더는 그 티켓에 묶여 있지 않다(아무것도 바꾸지 않았다)."""


MANIFESTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "manifests",
    "direct_buffer",
)


class DirectBufferProvider(KubernetesClient):
    # 상태 API 와 배포 smoke 가 이 값으로 arm 을 확인한다. 브랜치마다 settings 의 상수 하나만
    # 고치면 되게 거기서 읽는다(여기에 따로 적었다가 어긋나 배포 smoke 가 실패한 적이 있다).
    allocation_mode = settings.COMPUTE_ALLOCATION_MODE
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
    # 파드를 만든 템플릿(이미지 포함)의 세대. 배포로 템플릿이 바뀌면 옛 세대 idle 은
    # 할당하지 않고 교체한다. 요청에 묶인 파드는 건드리지 않는다.
    ANNOTATION_TEMPLATE_HASH = "k8s-dynamic-allocator/template-hash"
    # 재사용 횟수와 마지막으로 idle 로 되돌린 시각. 되돌린 파드는 첫 부팅 때부터 계속
    # Ready 라서, 대기하던 요청이 본 "쓸 수 있게 된 시각" 은 되돌린 시각이다.
    ANNOTATION_REUSE_COUNT = "k8s-dynamic-allocator/reuse-count"
    ANNOTATION_AVAILABLE_SINCE = "k8s-dynamic-allocator/available-since"
    ORIGIN_IDLE = "idle"
    ORIGIN_REQUEST = "request"

    def __init__(self):
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self._templates_by_type: Dict[str, Dict] = {}
        self._template_hash_by_type: Dict[str, str] = {}

    # ------------------------------------------------------------------ 설정

    def initialize_buffer(self, *, log_existing: bool = True) -> Dict:
        """파드 템플릿을 읽고 검증한다. 파드는 만들지 않는다(리더의 조정이 만든다)."""
        results = {"created": [], "existing": [], "failed": [], "templates": []}
        self._templates_by_type = {}
        self._template_hash_by_type = {}

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
        """R(버퍼의 바닥). 비었거나 잘못되면 0. 동적 R 이라 0 이어도 몰림 뒤에는 버퍼를 둔다."""
        del compute_type
        return self._parse_count(settings.WARM_BUFFER_RESERVE, "R") or 0

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
            # 할당 가능해진 시각. 되돌린 파드는 첫 부팅 때부터 Ready 라서 되돌린 시각이다.
            "available_at": self._pod_available_at(pod) if ready else None,
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

    # 파드 이름은 미리 만든 것과 요청 때 만든 것이 같은 규칙이다: compute-<타입>-<hex 10>.
    # 어느 경로로 만들었는지는 어노테이션 origin 이 기록한다(분석용). 이름으로 구분하지
    # 않는다 - 측정과 운영이 같은 코드를 쓴다.

    @staticmethod
    def _pod_name(compute_type: str, suffix: str) -> str:
        type_value = re.sub(r"[^a-z0-9-]", "-", compute_type.lower()).strip("-") or "general"
        return f"compute-{type_value}-{suffix}"

    def new_idle_name(self, compute_type: str) -> str:
        """미리 만들 파드의 이름. 요청 전에 정해 N 계산의 "생성 중" 목록에 먼저 올린다.

        무작위 꼬리를 쓴다. 순번은 초기화 때 Redis 와 함께 지워져, 앞 run 의 지워지는
        중인 파드와 이름이 겹칠 수 있다.
        """
        return self._pod_name(compute_type, uuid.uuid4().hex[:10])

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

    # ------------------------------------------------------------------ 할당·삭제·되돌리기

    @classmethod
    def _pod_available_at(cls, pod):
        """Ready 인 파드가 마지막으로 할당 가능해진 시각(Ready 시각과 되돌린 시각 중 늦은 쪽)."""
        ready_at = cls._pod_ready_at(pod)
        annotations = getattr(pod.metadata, "annotations", None) or {}
        raw = (annotations.get(cls.ANNOTATION_AVAILABLE_SINCE) or "").strip()
        if not raw:
            return ready_at
        try:
            available_since = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return ready_at
        if available_since.tzinfo is None:
            available_since = available_since.replace(tzinfo=timezone.utc)
        if ready_at is None:
            return available_since
        try:
            return max(ready_at, available_since)
        except TypeError:
            # 시간대 없는 Ready 시각과는 비교할 수 없다. 값 하나로 목록 전체를 버리지 않는다.
            return ready_at

    @staticmethod
    def _annotation_path(key: str) -> str:
        # JSON Pointer 이스케이프(RFC 6901): 어노테이션 키에 "/" 가 있다.
        return "/metadata/annotations/" + key.replace("~", "~0").replace("/", "~1")

    @timing.timed("k8s.patch_return")
    def return_pod(self, pod_name: str, ticket_id: str, reuse_count: int, available_since: str) -> None:
        """정리한 assigned 파드를 idle 로 되돌린다. 그사이 다른 티켓으로 넘어갔으면 실패한다.

        티켓 어노테이션을 test 하므로 늦은 반납이 다음 사용자의 파드를 되돌리지 않는다.
        resourceVersion 은 test 하지 않는다 - 정리하는 동안에도 kubelet 이 상태를 계속
        써서 이유 없이 실패한다. 할당(claim)은 resourceVersion 을 test 하므로, 되돌린 뒤의
        할당은 이 변경을 본 목록으로만 이뤄진다. 라벨 값은 idle 이다(available 이 아니다 -
        남은 Deployment 의 ReplicaSet 이 입양하지 못하게).
        """
        ticket_path = self._annotation_path(self.ANNOTATION_ALLOCATION_TICKET)
        patch = [
            {"op": "test", "path": "/metadata/labels/compute-status", "value": self.STATUS_ASSIGNED},
            {"op": "test", "path": ticket_path, "value": ticket_id},
            {"op": "replace", "path": "/metadata/labels/compute-status", "value": self.STATUS_IDLE},
            {"op": "replace", "path": "/metadata/labels/assigned-user", "value": ""},
            # 할당은 티켓과 claim 어노테이션을 늘 함께 쓴다.
            {"op": "remove", "path": ticket_path},
            {"op": "remove", "path": self._annotation_path(self.ANNOTATION_ALLOCATION_CLAIM)},
            {"op": "add", "path": self._annotation_path(self.ANNOTATION_REUSE_COUNT), "value": str(reuse_count)},
            {"op": "add", "path": self._annotation_path(self.ANNOTATION_AVAILABLE_SINCE), "value": available_since},
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
                raise PodReturnConflict(f"{pod_name} is no longer assigned to ticket {ticket_id}") from exc
            raise

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

    def delete_idle_pod(self, pod_name: str, uid: str, expected_resource_version: str) -> bool:
        return self.delete_pod_if_unchanged(pod_name, uid, expected_resource_version)

    @timing.timed("k8s.delete_pod")
    def delete_pod_if_unchanged(self, pod_name: str, uid: str, expected_resource_version: str) -> bool:
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
