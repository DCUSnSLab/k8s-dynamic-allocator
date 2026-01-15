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
        
    def create_backend_pod(self, username: str, command: str, frontend_pod: str = "") -> Dict:
        """
        사용자 요청에 따라 Backend Pod 생성
        
        Args:
            username: 사용자명 (Pod 이름에 사용)
            command: 실행할 명령어
            frontend_pod: Frontend Pod 이름 (SSHFS 마운트 대상)
            
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
            # Frontend Pod IP 조회
            frontend_ip = self.get_pod_ip(frontend_pod) if frontend_pod else ""
            if frontend_pod and not frontend_ip:
                return {
                    "status": "error",
                    "message": f"Frontend Pod '{frontend_pod}'의 IP를 찾을 수 없습니다"
                }
            
            logger.info(f"Frontend Pod IP: {frontend_ip}")
            
            # Pod 이름 생성 (backend-username)
            pod_name = f"backend-{username}".lower()
            
            logger.info(f"Creating backend pod: {pod_name}")
            
            # Pod 스펙 정의
            pod_spec = self._create_pod_spec(pod_name, username, command, frontend_ip)
            
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
                "frontend_pod": frontend_pod,
                "frontend_ip": frontend_ip,
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
    
    def _create_pod_spec(self, pod_name: str, username: str, command: str, frontend_ip: str = "") -> client.V1Pod:
        """
        Pod 스펙 생성
        
        Args:
            pod_name: Pod 이름
            username: 사용자명
            command: 실행할 명령어
            frontend_ip: Frontend Pod IP (SSHFS 마운트용)
            
        Returns:
            V1Pod: Kubernetes Pod 스펙
        """
        # SSHFS 마운트 스크립트 생성
        mount_script = f"""
# SSH 키 설정
mkdir -p /root/.ssh
cp /etc/ssh-key/id_rsa /root/.ssh/id_rsa
chmod 600 /root/.ssh/id_rsa

# SSHFS 마운트
mkdir -p /mnt/frontend
sshfs -o IdentityFile=/root/.ssh/id_rsa -o StrictHostKeyChecking=no \\
  dcuuser@{frontend_ip}:/home/dcuuser /mnt/frontend

# 사용자 명령 실행
cd /mnt/frontend
{command}

# 결과 저장 후 종료
"""
        
        # Container 정의 (privileged 모드 + SSH 키 마운트)
        container = client.V1Container(
            name="backend",
            image="harbor.cu.ac.kr/k8s_dynamic_allocator/backend:latest",
            command=["/bin/bash", "-c"],
            args=[mount_script],
            resources=client.V1ResourceRequirements(
                requests={"cpu": "100m", "memory": "128Mi"},
                limits={"cpu": "500m", "memory": "512Mi"}
            ),
            security_context=client.V1SecurityContext(
                privileged=True  # FUSE 마운트에 필요
            ),
            volume_mounts=[
                client.V1VolumeMount(
                    name="ssh-private-key",
                    mount_path="/etc/ssh-key",
                    read_only=True
                )
            ]
        )
        
        # SSH 개인키 볼륨 (Secret)
        ssh_key_volume = client.V1Volume(
            name="ssh-private-key",
            secret=client.V1SecretVolumeSource(
                secret_name="backend-ssh-key",
                default_mode=0o600
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
            volumes=[ssh_key_volume],
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

