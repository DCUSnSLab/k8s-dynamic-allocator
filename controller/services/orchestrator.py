"""
Orchestrator

- Backend Pool(K8s Deployment/Pods) 단위와 Backend Agent 연동 통제 최상위 클래스
- Frontend 요청 수신, 가용 Backend 할당(Allocation) 연계
- 선택된 Pod 내 Agent API 마운트 실행 및 Release(해제) 로직 오케스트레이션 전담
"""

import logging
from datetime import datetime
from typing import Dict

import httpx
from kubernetes.client.rest import ApiException

from .backend_pool import BackendPool, PodConflictError
from .backend_agent import BackendAgent

logger = logging.getLogger(__name__)


class Orchestrator:
    """Backend Pod 조율 Orchestrator"""
    
    MAX_RETRIES = 3
    
    def __init__(self):
        self.pool = BackendPool()
    
    def health_check(self) -> str:
        return "Orchestrator healthy"
    
    def initialize_pool(self) -> Dict:
        """Backend Pool 초기화 (시작 시 1회)"""
        return self.pool.initialize_pool()
    
    def execute_command(self, username: str, command: str, frontend_pod: str) -> Dict:
        """
        Frontend 요청 기반 Backend Pod 할당 및 터미널 명령어(SSHFS) 마운트 트리거(Trigger)
        
        - 과정:
          1. 요청 주체(Frontend Pod) IP 상태 점검
          2. 가용(Available) Backend Pod 탐색 및 Label 점유 (충돌 발생 시 Retry)
          3. 연결된 Backend Pod의 HTTP Agent API 연동 (Mount 비동기 요청 전송)
        """
        backend_pod = None
        
        try:
            # 1. Frontend Pod IP
            frontend_ip = self.pool.get_pod_ip(frontend_pod)
            if not frontend_ip:
                logger.error(f"[FAILED] Execute: Frontend pod IP not found ({frontend_pod})")
                return {
                    "status": "error",
                    "message": f"Frontend pod IP not found: {frontend_pod}"
                }
            
            # 2. Pod 선택 + 할당 (경쟁 시 재시도)
            for attempt in range(self.MAX_RETRIES):
                backend_pod = self.pool.get_available_pod()
                if not backend_pod:
                    logger.error("[FAILED] Execute: No available backend pods")
                    return {
                        "status": "error",
                        "message": "No available backend pods"
                    }
                try:
                    self.pool.assign_pod(backend_pod, frontend_pod)
                    break
                except PodConflictError:
                    logger.warning(f"{backend_pod} already assigned, retrying...")
                    backend_pod = None
                    continue
            
            if not backend_pod:
                logger.error(f"[FAILED] Execute: Assignment contention after {self.MAX_RETRIES} retries")
                return {
                    "status": "error",
                    "message": "Backend pod assignment failed due to contention"
                }
            
            # 3. Backend Pod IP
            backend_ip = self.pool.get_pod_ip(backend_pod)
            if not backend_ip:
                self.pool.release_pod(backend_pod)
                logger.error(f"[FAILED] Execute: Backend pod IP not found ({backend_pod})")
                return {
                    "status": "error",
                    "message": f"Backend pod IP not found: {backend_pod}"
                }
            
            # 4. Agent mount
            agent = BackendAgent(backend_ip)
            try:
                agent.mount(frontend_ip, command)
            finally:
                agent.close()
            
            logger.info(f"[SUCCESS] Execute: {frontend_pod} <-> {backend_pod} ({backend_ip})")
            
            return {
                "status": "success",
                "message": "Command submitted to backend",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "tcp_port": 8081,
                "frontend_pod": frontend_pod,
                "frontend_ip": frontend_ip,
                "command": command,
                "submitted_at": datetime.now().isoformat()
            }
            
        except httpx.HTTPError as e:
            if backend_pod:
                try:
                    self.pool.release_pod(backend_pod)
                except Exception as ex:
                    logger.warning(f"Failed to release pod {backend_pod} during fallback: {ex}")
            logger.error(f"[FAILED] Execute: Agent communication error ({e})")
            return {
                "status": "error",
                "message": f"Agent communication error: {str(e)}"
            }
        except ApiException as e:
            logger.error(f"[FAILED] Execute: K8s API error ({e.status} {e.reason})")
            return {
                "status": "error",
                "message": f"K8s API error: {e.reason}"
            }
        except Exception as e:
            logger.error(f"[FAILED] Execute: Unexpected error ({e})")
            return {
                "status": "error",
                "message": f"Unexpected error: {str(e)}"
            }
    
    def get_pool_status(self) -> Dict:
        """Pool 전체 상태 조회"""
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
        """Backend Pod 할당 해제"""
        try:
            # 해제 전 연결 정보 조회
            pod = self.pool.v1.read_namespaced_pod(backend_pod, self.pool.namespace)
            frontend = pod.metadata.labels.get(self.pool.LABEL_FRONTEND, "unknown")

            backend_ip = self.pool.get_pod_ip(backend_pod)
            if backend_ip:
                agent = BackendAgent(backend_ip)
                try:
                    agent.unmount()
                finally:
                    agent.close()
            
            self.pool.release_pod(backend_pod)
            logger.info(f"[SUCCESS] Released: {frontend} <-> {backend_pod} ({backend_ip})")
            return {
                "status": "success",
                "message": f"Released: {backend_pod}"
            }
        except Exception as e:
            logger.error(f"[FAILED] Release: {backend_pod} ({e})")
            return {
                "status": "error",
                "message": str(e)
            }

    def check_stale_allocations(self) -> Dict:
        """
        비정상 할당 감지 및 자동 해제 (가비지 컬렉터 역할)
        
        - 대상: K8s 내 할당 매칭된 Frontend Pod가 없거나 Running 상태가 아닌 경우
        - 조치: 대상 Backend Pod 연결 강제 해제 및 Assigned(할당됨) Label 거두기
        """
        pool_list = self.pool.list_pool_status()
        assigned = [p for p in pool_list if p["pool_status"] == "assigned"]

        released = []
        errors = []

        for pod_info in assigned:
            frontend_pod = pod_info.get("assigned_frontend", "")
            backend_pod = pod_info["name"]

            if not frontend_pod:
                continue

            frontend_status = self.pool.get_pod_status(frontend_pod)

            if frontend_status is None:
                logger.warning(
                    f"Frontend '{frontend_pod}' not found, releasing '{backend_pod}'"
                )
                result = self.release_backend(backend_pod)
                if result["status"] == "success":
                    released.append(backend_pod)
                else:
                    errors.append({"pod": backend_pod, "error": result["message"]})

            elif frontend_status != "Running":
                logger.warning(
                    f"Frontend '{frontend_pod}' is {frontend_status}, "
                    f"releasing '{backend_pod}'"
                )
                result = self.release_backend(backend_pod)
                if result["status"] == "success":
                    released.append(backend_pod)
                else:
                    errors.append({"pod": backend_pod, "error": result["message"]})

        return {
            "checked": len(assigned),
            "released": released,
            "errors": errors,
        }
