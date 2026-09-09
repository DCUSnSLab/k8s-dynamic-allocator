"""
Kubernetes Client Base

K8s API 연결 및 공통 Pod 단일 상태 조회 인터페이스 제공
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
    Kubernetes API 클라이언트 공통 베이스 클래스

    - InCluster/KubeConfig 인증 과정 통합 (프로세스 최초 1회 로드)
    - Pod IP 및 구동 Status 획득 공통 로직 내장
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
        self.namespace = namespace or os.getenv("K8S_NAMESPACE") or os.getenv("DEFAULT_NAMESPACE", "swlabpods")
        request_timeout = float(
            os.getenv("K8S_API_REQUEST_TIMEOUT_SECONDS", "5")
        )
        self.api_request_timeout = (2.0, max(1.0, request_timeout))
    
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
        Pod 현재 가동 상태 판별 (Running, Pending 등 명시)
        
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

    @staticmethod
    def _pod_not_ready_since(pod):
        """Ready였다가 False로 떨어진 시각 (그 외에는 None).

        아직 한 번도 Ready가 된 적 없는 Pod는 Ready=False 전이 시각이 startTime과
        같고 기동 중에도 갱신되지 않는다. 그래서 시각을 그대로 쓰면 "고장난 뒤
        경과 시간"이 아니라 Pod 나이가 되고, 기동이 느린 Pod가 회수 대상이 된다.
        Pod 오브젝트가 이미 들고 있는 값이라 별도 조회는 필요 없다.
        """
        status = getattr(pod, "status", None)
        start_time = getattr(status, "start_time", None)
        if start_time is None:
            return None

        for condition in getattr(status, "conditions", None) or []:
            if condition.type != "Ready" or condition.status == "True":
                continue
            not_ready_since = getattr(condition, "last_transition_time", None)
            if not_ready_since is not None and not_ready_since > start_time:
                return not_ready_since
            return None
        return None
