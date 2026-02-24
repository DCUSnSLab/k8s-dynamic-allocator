"""
Backend Pod Pool Manager

Deployment 기반 Backend Pool 관리 클래스
manifests/ 디렉토리의 YAML 파일을 자동 스캔하여 Deployment 생성 및 관리
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


class BackendPool(KubernetesClient):
    """
    Deployment 기반 Backend Pod Pool 관리 클래스
    
    - manifests/*.yaml 자동 스캔하여 Deployment 생성
    - 사용 가능한 Pod 조회 (Label 기반)
    - Pod 할당/해제 (Label 업데이트)
    - ReplicaSet이 Pod 자동 복구 담당
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
    
    def initialize_pool(self) -> Dict:
        """
        manifests/ 하위 모든 YAML을 스캔하여 Deployment 생성
        
        이미 존재하는 Deployment는 건너뛰고, 없는 것만 생성
        
        Returns:
            Dict: 초기화 결과
        """
        results = {"created": [], "existing": [], "failed": []}
        
        yaml_files = glob.glob(os.path.join(MANIFESTS_DIR, "*.yaml"))
        
        if not yaml_files:
            logger.warning(f"No YAML files found in {MANIFESTS_DIR}")
            return results
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file) as f:
                    spec = yaml.safe_load(f)
                
                name = spec["metadata"]["name"]
                
                self.apps_v1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=spec
                )
                logger.info(f"Created deployment: {name}")
                results["created"].append(name)
                
            except ApiException as e:
                if e.status == 409:
                    logger.info(f"Deployment {name} already exists")
                    results["existing"].append(name)
                else:
                    logger.error(f"Failed to create deployment from {yaml_file}: {e}")
                    results["failed"].append({"file": os.path.basename(yaml_file), "error": str(e)})
            except Exception as e:
                logger.error(f"Failed to load {yaml_file}: {e}")
                results["failed"].append({"file": os.path.basename(yaml_file), "error": str(e)})
        
        return results
    
    def get_available_pod(self) -> Optional[str]:
        """
        사용 가능한 Pool Pod 조회
        
        Label selector를 사용하여 available 상태인 Pod 중 하나 반환
        
        Returns:
            Optional[str]: 사용 가능한 Pod 이름 (없으면 None)
        """
        try:
            label_selector = f"app={self.LABEL_APP},{self.LABEL_STATUS}={self.STATUS_AVAILABLE}"
            pods = self.v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector
            )
            
            # Running 상태인 Pod만 필터링
            running_pods = [
                pod for pod in pods.items
                if pod.status.phase == "Running"
            ]
            
            if not running_pods:
                logger.warning("No available pool pods found")
                return None
            
            # [임시] 첫 번째 Pod 반환
            selected_pod = running_pods[0].metadata.name
            logger.info(f"Selected available pod: {selected_pod}")
            return selected_pod
            
        except ApiException as e:
            logger.error(f"Failed to list available pods: {e}")
            return None
    
    def assign_pod(self, pod_name: str, frontend_pod: str) -> bool:
        """
        Pod를 Frontend에 할당 (Label 업데이트)
        
        Args:
            pod_name: 할당할 Pool Pod 이름
            frontend_pod: 연결할 Frontend Pod 이름
            
        Returns:
            bool: 할당 성공 여부
        """
        try:
            patch = {
                "metadata": {
                    "labels": {
                        self.LABEL_STATUS: self.STATUS_ASSIGNED,
                        self.LABEL_FRONTEND: frontend_pod
                    }
                }
            }
            self.v1.patch_namespaced_pod(pod_name, self.namespace, patch)
            logger.info(f"Assigned pod {pod_name} to frontend {frontend_pod}")
            return True
        except ApiException as e:
            logger.error(f"Failed to assign pod {pod_name}: {e}")
            return False
    
    def release_pod(self, pod_name: str) -> bool:
        """
        Pod 할당 해제 (Label 업데이트)
        
        Args:
            pod_name: 해제할 Pool Pod 이름
            
        Returns:
            bool: 해제 성공 여부
        """
        try:
            patch = {
                "metadata": {
                    "labels": {
                        self.LABEL_STATUS: self.STATUS_AVAILABLE,
                        self.LABEL_FRONTEND: ""
                    }
                }
            }
            self.v1.patch_namespaced_pod(pod_name, self.namespace, patch)
            logger.info(f"Released pod {pod_name}")
            return True
        except ApiException as e:
            logger.error(f"Failed to release pod {pod_name}: {e}")
            return False
    
    def list_pool_status(self) -> List[Dict]:
        """
        Pool 전체 상태 조회
        
        Returns:
            List[Dict]: Pool Pod 상태 목록
        """
        try:
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
        except ApiException as e:
            logger.error(f"Failed to list pool status: {e}")
            return []
