"""
Orchestrator

Backend Pool을 조율하는 메인 Orchestrator 클래스
kubectl exec 기반으로 마운트/실행 관리
"""

import logging
import subprocess
from datetime import datetime
from typing import Dict

from .backend_pool import BackendPool

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Backend Pod 조율 Orchestrator
    
    - Pool에서 사용 가능한 Pod 선택
    - kubectl exec로 마운트/언마운트
    - Interactive 세션 관리
    """
    
    def __init__(self):
        """Orchestrator 초기화"""
        self.pool = BackendPool()
    
    def health_check(self) -> str:
        """헬스 체크"""
        return "Orchestrator healthy"
    
    def initialize_pool(self) -> Dict:
        """
        Backend Pool 초기화
        
        Returns:
            Dict: 초기화 결과
        """
        logger.info("Initializing Backend Pool...")
        result = self.pool.initialize_pool()
        logger.info(f"Pool initialization result: {result}")
        return result
    
    def interactive_start(self, frontend_pod: str) -> Dict:
        """
        Interactive 세션 시작
        
        1. Pool에서 사용 가능한 Pod 선택
        2. Pod를 Frontend에 할당
        3. kubectl exec로 SSHFS 마운트 (루트 전체)
        
        Args:
            frontend_pod: Frontend Pod 이름
            
        Returns:
            Dict: backend_pod, pod_ip 포함
        """
        try:
            logger.info(f"Interactive start: frontend={frontend_pod}")
            
            # 1. Frontend Pod IP 조회
            frontend_ip = self.pool.get_pod_ip(frontend_pod)
            if not frontend_ip:
                return {
                    "status": "error",
                    "message": f"Frontend Pod '{frontend_pod}'의 IP를 찾을 수 없습니다"
                }
            
            # 2. Pool에서 사용 가능한 Pod 선택
            backend_pod = self.pool.get_available_pod()
            if not backend_pod:
                return {
                    "status": "error",
                    "message": "사용 가능한 Backend Pod가 없습니다"
                }
            
            # 3. Pod 할당 (Label 업데이트)
            if not self.pool.assign_pod(backend_pod, frontend_pod):
                return {
                    "status": "error",
                    "message": f"Backend Pod '{backend_pod}' 할당 실패"
                }
            
            # 4. Backend Pod IP 조회
            backend_ip = self.pool.get_pod_ip(backend_pod)
            if not backend_ip:
                self.pool.release_pod(backend_pod)
                return {
                    "status": "error",
                    "message": f"Backend Pod '{backend_pod}'의 IP를 찾을 수 없습니다"
                }
            
            # 5. kubectl exec로 SSHFS 마운트
            mount_result = self._mount_frontend(backend_pod, frontend_ip)
            if not mount_result["success"]:
                self.pool.release_pod(backend_pod)
                return {
                    "status": "error",
                    "message": f"마운트 실패: {mount_result['error']}"
                }
            
            logger.info(f"Interactive session started: {backend_pod} -> {frontend_pod}")
            
            return {
                "status": "success",
                "message": "Interactive 세션이 시작되었습니다",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "frontend_pod": frontend_pod,
                "frontend_ip": frontend_ip,
                "started_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in interactive_start: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"예상치 못한 에러: {str(e)}"
            }
    
    def interactive_end(self, backend_pod: str) -> Dict:
        """
        Interactive 세션 종료
        
        1. kubectl exec로 SSHFS 언마운트
        2. Pool로 반환
        
        Args:
            backend_pod: Backend Pod 이름
            
        Returns:
            Dict: 해제 결과
        """
        try:
            logger.info(f"Interactive end: backend={backend_pod}")
            
            # 1. kubectl exec로 언마운트
            self._unmount_frontend(backend_pod)
            
            # 2. Pool로 반환
            if self.pool.release_pod(backend_pod):
                return {
                    "status": "success",
                    "message": f"Backend Pod '{backend_pod}'가 Pool로 반환되었습니다"
                }
            else:
                return {
                    "status": "error",
                    "message": f"Backend Pod '{backend_pod}' 해제 실패"
                }
                
        except Exception as e:
            logger.error(f"Error in interactive_end: {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e)
            }
    
    def _mount_frontend(self, backend_pod: str, frontend_ip: str) -> Dict:
        """
        kubectl exec로 SSHFS 마운트
        
        Frontend 루트 전체를 /mnt/rootfs에 마운트
        """
        namespace = self.pool.namespace
        mount_cmd = (
            f"mkdir -p /mnt/rootfs && "
            f"sshfs -o StrictHostKeyChecking=no,allow_other "
            f"dcuuser@{frontend_ip}:/ /mnt/rootfs"
        )
        
        try:
            result = subprocess.run(
                ["kubectl", "exec", "-n", namespace, backend_pod, "--", "sh", "-c", mount_cmd],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info(f"Mount successful: {backend_pod} -> {frontend_ip}")
                return {"success": True}
            else:
                logger.error(f"Mount failed: {result.stderr}")
                return {"success": False, "error": result.stderr}
                
        except subprocess.TimeoutExpired:
            return {"success": False, "error": "Mount timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _unmount_frontend(self, backend_pod: str) -> bool:
        """
        kubectl exec로 SSHFS 언마운트
        """
        namespace = self.pool.namespace
        unmount_cmd = "fusermount -uz /mnt/rootfs || umount -l /mnt/rootfs || true"
        
        try:
            subprocess.run(
                ["kubectl", "exec", "-n", namespace, backend_pod, "--", "sh", "-c", unmount_cmd],
                capture_output=True,
                timeout=10
            )
            logger.info(f"Unmount completed: {backend_pod}")
            return True
        except Exception as e:
            logger.error(f"Unmount error: {e}")
            return False
    
    def get_pool_status(self) -> Dict:
        """
        Pool 전체 상태 조회
        
        Returns:
            Dict: Pool 상태
        """
        pool_list = self.pool.list_pool_status()
        
        available_count = sum(1 for p in pool_list if p["pool_status"] == "available")
        assigned_count = sum(1 for p in pool_list if p["pool_status"] == "assigned")
        
        return {
            "total": len(pool_list),
            "available": available_count,
            "assigned": assigned_count,
            "pods": pool_list
        }
    
    def release_backend(self, backend_pod: str) -> Dict:
        """
        Backend Pod 할당 해제 (interactive_end와 동일)
        """
        return self.interactive_end(backend_pod)
