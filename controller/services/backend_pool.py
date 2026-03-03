"""
Backend Pool Manager

- K8s Deployment 기반 Backend Pool Pod 상태 식별
- K8s Label 변경을 통한 특정 Frontend 대상 가용(Available) Pod 점유 할당 및 해제 매니징
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
    """Pod 할당 충돌 (422) — 다른 Controller가 이미 할당함"""
    pass


class BackendPool(KubernetesClient):
    """
    Backend Pod Pool 상태 및 할당 매니저
    
    - manifests/ 디렉토리 템플릿 로드 (초기 구동 1회)
    - Label Selector 기반 가동 가능 Pod 스케줄링 대행
    - 동시성 Race Condition 제어 (JSON Patch Test 활용)
    """
    
    # Pool Pod Label 상수
    LABEL_APP = "backend-pool"
    LABEL_STATUS = "pool-status"
    LABEL_FRONTEND = "assigned-frontend"
    
    # Pool 상태 값
    STATUS_AVAILABLE = "available"
    STATUS_ASSIGNED = "assigned"
    
    def __init__(self):
        """Backend Pool 초기화"""
        super().__init__()
        self.apps_v1 = client.AppsV1Api()
        self.owner_ref = self._get_owner_deployment()
    
    def initialize_pool(self) -> Dict:
        """
        manifests/ 하위 모든 YAML을 스캔하여 Deployment 생성
        (시작 시 1회성 — 로그 유지)
        """
        results = {"created": [], "existing": [], "failed": []}
        
        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        
        if not yaml_files:
            logger.warning(f"No manifest files found in {MANIFESTS_DIR}")
            return results
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    spec = yaml.safe_load(f)
                
                name = spec["metadata"]["name"]
                
                # OwnerReference 주입 (Controller Deployment 삭제 시 같이 삭제)
                if self.owner_ref:
                    spec.setdefault("metadata", {})["ownerReferences"] = [self.owner_ref]
                
                self.apps_v1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=spec
                )
                logger.info(f"Deployment created: {name}")
                results["created"].append(name)
                
            except ApiException as e:
                if e.status == 409:
                    logger.info(f"Deployment exists: {name}")
                    results["existing"].append(name)
                else:
                    logger.error(f"Deployment creation failed ({os.path.basename(yaml_file)}): {e}")
                    results["failed"].append({"file": os.path.basename(yaml_file), "error": str(e)})
            except Exception as e:
                logger.error(f"Manifest load failed ({os.path.basename(yaml_file)}): {e}")
                results["failed"].append({"file": os.path.basename(yaml_file), "error": str(e)})
        
        return results
    
    # 클래스 레벨 캐시 — 프로세스당 1회만 조회
    _cached_owner_ref = None
    _owner_ref_resolved = False

    def _get_owner_deployment(self) -> Optional[Dict]:
        """
        Pod → ReplicaSet → Deployment 소유 체인 추적
        (프로세스당 1회만 실행, 결과 캐시)
        """
        if BackendPool._owner_ref_resolved:
            return BackendPool._cached_owner_ref

        try:
            pod_name = os.getenv("HOSTNAME")
            if not pod_name:
                logger.warning("HOSTNAME not set, skipping OwnerReference")
                BackendPool._owner_ref_resolved = True
                return None
            
            # Pod → ReplicaSet
            pod = self.v1.read_namespaced_pod(pod_name, self.namespace)
            if not pod.metadata.owner_references:
                logger.warning("Pod has no ownerReferences")
                BackendPool._owner_ref_resolved = True
                return None
            
            rs_ref = pod.metadata.owner_references[0]
            rs = self.apps_v1.read_namespaced_replica_set(rs_ref.name, self.namespace)
            
            # ReplicaSet → Deployment
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
            
            logger.info(f"OwnerRef resolved: {deploy_ref.name} (uid={deploy_ref.uid})")
            BackendPool._cached_owner_ref = owner_ref
            BackendPool._owner_ref_resolved = True
            return owner_ref
            
        except Exception as e:
            logger.warning(f"Failed to get owner deployment: {e}")
            BackendPool._owner_ref_resolved = True
            return None
    
    def get_available_pod(self) -> Optional[str]:
        """
        사용 가능한 Pool Pod 조회
        
        Returns:
            Optional[str]: Pod 이름 (없으면 None)
        Raises:
            ApiException: K8s API 에러
        """
        label_selector = f"app={self.LABEL_APP},{self.LABEL_STATUS}={self.STATUS_AVAILABLE}"
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=label_selector
        )
        
        running_pods = [
            pod for pod in pods.items
            if pod.status.phase == "Running"
        ]
        
        if not running_pods:
            return None
        
        return running_pods[0].metadata.name
    
    def assign_pod(self, pod_name: str, frontend_pod: str) -> None:
        """
        Pod를 Frontend에 할당 (JSON Patch + test로 경쟁 방지)
        
        Raises:
            PodConflictError: 422 — 이미 다른 Controller가 할당
            ApiException: 그 외 K8s API 에러
        """
        try:
            patch = [
                {"op": "test", "path": "/metadata/labels/pool-status", "value": self.STATUS_AVAILABLE},
                {"op": "replace", "path": "/metadata/labels/pool-status", "value": self.STATUS_ASSIGNED},
                {"op": "replace", "path": "/metadata/labels/assigned-frontend", "value": frontend_pod}
            ]
            self.v1.api_client.call_api(
                '/api/v1/namespaces/{namespace}/pods/{name}',
                'PATCH',
                path_params={'namespace': self.namespace, 'name': pod_name},
                body=patch,
                header_params={'Content-Type': 'application/json-patch+json'},
                response_type='V1Pod',
                auth_settings=['BearerToken'],
                _return_http_data_only=True,
                _preload_content=True
            )
        except ApiException as e:
            if e.status == 422:
                raise PodConflictError(f"{pod_name} already taken")
            raise
    
    def release_pod(self, pod_name: str) -> None:
        """
        Pod 할당 해제 (Label 업데이트)
        
        Raises:
            ApiException: K8s API 에러
        """
        patch = {
            "metadata": {
                "labels": {
                    self.LABEL_STATUS: self.STATUS_AVAILABLE,
                    self.LABEL_FRONTEND: ""
                }
            }
        }
        self.v1.patch_namespaced_pod(pod_name, self.namespace, patch)
    
    def list_pool_status(self) -> List[Dict]:
        """
        Pool 전체 상태 조회
        
        Raises:
            ApiException: K8s API 에러
        """
        label_selector = f"app={self.LABEL_APP}"
        pods = self.v1.list_namespaced_pod(
            namespace=self.namespace,
            label_selector=label_selector
        )
        
        status_list = []
        for pod in pods.items:
            status_list.append({
                "name": pod.metadata.name,
                "phase": pod.status.phase,
                "backend_type": pod.metadata.labels.get("backend-type", "unknown"),
                "pool_status": pod.metadata.labels.get(self.LABEL_STATUS, "unknown"),
                "assigned_frontend": pod.metadata.labels.get(self.LABEL_FRONTEND, ""),
                "ip": pod.status.pod_ip
            })
        
        return status_list
