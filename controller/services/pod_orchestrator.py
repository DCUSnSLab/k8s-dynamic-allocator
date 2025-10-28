"""
Backend Pod Orchestrator

Kubernetes API를 사용하여 Backend Pod를 생성, 관리, 삭제하는 클래스
"""

import logging
from datetime import datetime
from typing import Dict, Optional
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class BackendOrchestrator:
    """
    Backend Pod 생성 및 관리를 담당하는 Orchestrator 클래스
    """
    
    def __init__(self):
        """
        Kubernetes 클라이언트 초기화
        클러스터 내부에서 실행 시 InClusterConfig 사용
        로컬 개발 시 ~/.kube/config 사용
        """
        try:
            # 클러스터 내부에서 실행 중인 경우
            config.load_incluster_config()
            logger.info("Kubernetes InClusterConfig loaded successfully")
        except config.ConfigException:
            # 로컬 개발 환경인 경우
            try:
                config.load_kube_config()
                logger.info("Kubernetes KubeConfig loaded successfully")
            except config.ConfigException as e:
                logger.error(f"Failed to load Kubernetes config: {e}")
                raise
        
        self.v1 = client.CoreV1Api()
        self.namespace = "swlabpods"

    def health_check(self):
        return "import successfully"
        
    def create_backend_pod(self, username: str, command: str) -> Dict:
        """
        사용자 요청에 따라 Backend Pod 생성
        
        Args:
            username: 사용자명 (Pod 이름에 사용)
            command: 실행할 명령어
            
        Returns:
            Dict: 생성 결과
                {
                    "status": "success" | "error",
                    "message": str,
                    "pod_name": str,
                    "namespace": str,
                    "created_at": str
                }
        """
        try:
            # Pod 이름 생성 (backend-username)
            pod_name = f"backend-{username}".lower()
            
            logger.info(f"Creating backend pod: {pod_name}")
            
            # Pod 스펙 정의
            pod_spec = self._create_pod_spec(pod_name, username, command)
            
            # Pod 생성
            response = self.v1.create_namespaced_pod(
                namespace=self.namespace,
                body=pod_spec
            )
            
            logger.info(f"Pod {pod_name} created successfully")
            
            return {
                "status": "success",
                "message": f"Backend Pod가 성공적으로 생성되었습니다",
                "pod_name": pod_name,
                "namespace": self.namespace,
                "command": command,
                "created_at": datetime.now().isoformat(),
                "uid": response.metadata.uid
            }
            
        except ApiException as e:
            logger.error(f"Kubernetes API error: {e}")
            return {
                "status": "error",
                "message": f"Pod 생성 실패: {e.reason}",
                "details": str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"예상치 못한 에러: {str(e)}"
            }
    
    def _create_pod_spec(self, pod_name: str, username: str, command: str) -> client.V1Pod:
        """
        Pod 스펙 생성
        
        Args:
            pod_name: Pod 이름
            username: 사용자명
            command: 실행할 명령어
            
        Returns:
            V1Pod: Kubernetes Pod 스펙
        """
        # Container 정의
        container = client.V1Container(
            name="backend",
            image="ubuntu:22.04",  # 기본 이미지 (나중에 커스텀 이미지로 변경 가능)
            command=["/bin/bash", "-c"],
            args=[command],
            resources=client.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"}
            )
        )
        
        # Pod 메타데이터
        metadata = client.V1ObjectMeta(
            name=pod_name,
            labels={
                "app": "backend",
                "username": username,
                "created-by": "controller"
            }
        )
        
        # Pod 스펙
        spec = client.V1PodSpec(
            containers=[container],
            restart_policy="Never"  # 작업 완료 후 재시작하지 않음
        )
        
        # Pod 객체 생성
        pod = client.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=metadata,
            spec=spec
        )
        
        return pod
