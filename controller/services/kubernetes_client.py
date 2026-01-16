"""
Kubernetes Client Base

Kubernetes API 연결 및 공통 Pod 조회 기능을 제공하는 기본 클래스
"""

import logging
import os
from abc import ABC
from typing import Optional

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class KubernetesClient(ABC):
    """
    Kubernetes API 클라이언트 기본 클래스
    
    다른 서비스 클래스들이 상속받아 K8s API와 통신할 때 사용
    """
    
    def __init__(self, namespace: str = None):
        """
        Kubernetes 클라이언트 초기화
        
        클러스터 내부에서 실행 시 InClusterConfig 사용
        로컬 개발 시 ~/.kube/config 사용
        
        Args:
            namespace: 작업할 네임스페이스 (기본값: 환경변수 또는 "swlabpods")
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
        self.namespace = namespace or os.getenv("K8S_NAMESPACE", "swlabpods")
    
    def get_pod_ip(self, pod_name: str) -> Optional[str]:
        """
        Pod 이름으로 IP 주소 조회
        
        Args:
            pod_name: Pod 이름
            
        Returns:
            Optional[str]: Pod IP 주소 (없으면 None)
        """
        try:
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return pod.status.pod_ip
        except ApiException as e:
            logger.error(f"Failed to get Pod IP for {pod_name}: {e}")
            return None
    
    def get_pod_status(self, pod_name: str) -> Optional[str]:
        """
        Pod 상태 조회
        
        Args:
            pod_name: Pod 이름
            
        Returns:
            Optional[str]: Pod 상태 (Running, Pending, Succeeded, Failed 등)
        """
        try:
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return pod.status.phase
        except ApiException as e:
            logger.error(f"Failed to get Pod status for {pod_name}: {e}")
            return None
    
    def pod_exists(self, pod_name: str) -> bool:
        """
        Pod 존재 여부 확인
        
        Args:
            pod_name: Pod 이름
            
        Returns:
            bool: Pod 존재 여부
        """
        try:
            self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            logger.error(f"Failed to check Pod existence for {pod_name}: {e}")
            return False
