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

# K8s config는 프로세스당 1회만 로드
_config_loaded = False


class KubernetesClient(ABC):
    """
    Kubernetes API 클라이언트 기본 클래스
    
    하위 레이어: 에러 시 exception raise (로그 없음)
    config 로드는 프로세스당 1회만 실행 + 로그
    """
    
    def __init__(self, namespace: str = None):
        global _config_loaded
        
        if not _config_loaded:
            try:
                config.load_incluster_config()
                logger.info("Kubernetes InClusterConfig loaded successfully")
            except config.ConfigException:
                try:
                    config.load_kube_config()
                    logger.info("Kubernetes KubeConfig loaded successfully")
                except config.ConfigException as e:
                    raise RuntimeError(f"Failed to load Kubernetes config: {e}")
            _config_loaded = True
        
        self.v1 = client.CoreV1Api()
        self.namespace = namespace or os.getenv("K8S_NAMESPACE", "swlabpods")
    
    def get_pod_ip(self, pod_name: str) -> Optional[str]:
        """
        Pod IP 조회
        
        Raises:
            ApiException: K8s API 에러 (404 제외 — None 반환)
        """
        try:
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return pod.status.pod_ip
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def get_pod_status(self, pod_name: str) -> Optional[str]:
        """
        Pod 상태 조회 (Running, Pending 등)
        
        Returns:
            Optional[str]: Pod 상태 (존재하지 않으면 None)
        """
        try:
            pod = self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return pod.status.phase
        except ApiException as e:
            if e.status == 404:
                return None
            raise
    
    def pod_exists(self, pod_name: str) -> bool:
        """Pod 존재 여부 확인"""
        try:
            self.v1.read_namespaced_pod(name=pod_name, namespace=self.namespace)
            return True
        except ApiException as e:
            if e.status == 404:
                return False
            raise
