"""
Backend Pod Pool Manager

Backend Pod Pool을 생성, 관리, 할당하는 클래스
미리 생성된 Pool에서 사용 가능한 Pod를 선택하여 할당
"""

import logging
from typing import Dict, List, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from .kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)


POOL_SIZE = 3
POOL_PREFIX = "backend-pool"
BACKEND_IMAGE = "harbor.cu.ac.kr/k8s_dynamic_allocator/backend:interactive"


class BackendPool(KubernetesClient):
    """
    Backend Pod Pool 관리 클래스
    
    - Pool Pod 생성/삭제
    - 사용 가능한 Pod 조회 (Label 기반)
    - Pod 할당/해제 (Label 업데이트)
    
    향후 확장:
    - Frontend 수에 따른 Pool 크기 동적 조절
    - 작업 유형별 Pod 선택 알고리즘
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
        self.pool_size = POOL_SIZE
        self.pool_prefix = POOL_PREFIX
        self.backend_image = BACKEND_IMAGE
    
    def initialize_pool(self) -> Dict:
        """
        Pool Pod 초기화 (N개 미리 생성)
        
        Returns:
            Dict: 초기화 결과
        """
        results = {"created": [], "existing": [], "failed": []}
        
        for i in range(self.pool_size):
            pod_name = f"{self.pool_prefix}-{i + 1}"
            
            if self.pod_exists(pod_name):
                logger.info(f"Pool pod {pod_name} already exists")
                results["existing"].append(pod_name)
                continue
            
            try:
                pod_spec = self._create_pool_pod_spec(i)
                self.v1.create_namespaced_pod(
                    namespace=self.namespace,
                    body=pod_spec
                )
                logger.info(f"Created pool pod: {pod_name}")
                results["created"].append(pod_name)
            except ApiException as e:
                logger.error(f"Failed to create pool pod {pod_name}: {e}")
                results["failed"].append({"name": pod_name, "error": str(e)})
        
        return results
    
    def get_available_pod(self) -> Optional[str]:
        """
        사용 가능한 Pool Pod 조회
        
        Label selector를 사용하여 available 상태인 Pod 중 하나 반환
        
        향후 확장: 작업 유형에 따른 적합한 Pod 선택 알고리즘 추가
        
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
                    "pool_status": pod.metadata.labels.get(self.LABEL_STATUS, "unknown"),
                    "assigned_frontend": pod.metadata.labels.get(self.LABEL_FRONTEND, ""),
                    "ip": pod.status.pod_ip
                })
            
            return status_list
        except ApiException as e:
            logger.error(f"Failed to list pool status: {e}")
            return []
    
    def _create_pool_pod_spec(self, index: int) -> client.V1Pod:
        """
        Pool Pod 스펙 생성
        
        Args:
            index: Pod 인덱스
            
        Returns:
            V1Pod: Kubernetes Pod 스펙
        """
        pod_name = f"{self.pool_prefix}-{index}"
        
        container = client.V1Container(
            name="backend-agent",
            image=self.backend_image,
            command=["uvicorn", "agent:app", "--host", "0.0.0.0", "--port", "8080"],
            ports=[
                client.V1ContainerPort(container_port=8080, name="agent")
            ],
            resources=client.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"}
            ),
            security_context=client.V1SecurityContext(
                privileged=True
            ),
            volume_mounts=[
                client.V1VolumeMount(
                    name="ssh-private-key",
                    mount_path="/etc/ssh-key",
                    read_only=True
                )
            ]
        )
        
        ssh_key_volume = client.V1Volume(
            name="ssh-private-key",
            secret=client.V1SecretVolumeSource(
                secret_name="backend-ssh-key",
                default_mode=0o600
            )
        )

        metadata = client.V1ObjectMeta(
            name=pod_name,
            labels={
                "app": self.LABEL_APP,
                self.LABEL_STATUS: self.STATUS_AVAILABLE,
                self.LABEL_FRONTEND: "",
                "created-by": "controller"
            }
        )
        
        spec = client.V1PodSpec(
            containers=[container],
            volumes=[ssh_key_volume],
            restart_policy="Always"
        )
        
        return client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=metadata,
            spec=spec
        )
