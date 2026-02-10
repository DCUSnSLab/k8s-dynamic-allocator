"""
Orchestrator

Backend Pool과 Agent를 조율하는 메인 Orchestrator 클래스
모든 Backend 관련 요청의 진입점
"""

import logging
from datetime import datetime
from typing import Dict

from .backend_pool import BackendPool
from .backend_agent import BackendAgent

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Backend Pod 조율 Orchestrator
    
    - Pool에서 사용 가능한 Pod 선택
    - Agent에 마운트/실행 요청
    - 작업 상태 관리
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
        
        Controller 시작 시 호출하여 Pool Pod 생성
        
        Returns:
            Dict: 초기화 결과
        """
        logger.info("Initializing Backend Pool...")
        result = self.pool.initialize_pool()
        logger.info(f"Pool initialization result: {result}")
        return result
    
    def execute_command(self, username: str, command: str, frontend_pod: str) -> Dict:
        """
        Backend Pod에서 명령 실행
        
        1. Pool에서 사용 가능한 Pod 선택
        2. Pod를 Frontend에 할당
        3. Agent에 마운트 및 실행 요청
        
        Args:
            username: 사용자명
            command: 실행할 명령어
            frontend_pod: Frontend Pod 이름
            
        Returns:
            Dict: 실행 결과
        """
        try:
            logger.info(f"Execute request: user={username}, command={command}, frontend={frontend_pod}")
            
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
                    "message": "사용 가능한 Backend Pod가 없습니다. 잠시 후 다시 시도해주세요."
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
                # 할당 롤백
                self.pool.release_pod(backend_pod)
                return {
                    "status": "error",
                    "message": f"Backend Pod '{backend_pod}'의 IP를 찾을 수 없습니다"
                }
            
            # 5. Agent 클라이언트 생성 및 마운트 요청
            agent = BackendAgent(backend_ip)
            try:
                agent_response = agent.mount(frontend_ip, command)
            finally:
                agent.close()
            
            if agent_response.get("status") == "error":
                # 할당 롤백
                self.pool.release_pod(backend_pod)
                return {
                    "status": "error",
                    "message": f"Agent 요청 실패: {agent_response.get('message')}"
                }
            
            logger.info(f"Command submitted to backend {backend_pod}")
            
            return {
                "status": "success",
                "message": "Backend Pod에 명령이 전달되었습니다",
                "backend_pod": backend_pod,
                "backend_ip": backend_ip,
                "tcp_port": 8081,
                "frontend_pod": frontend_pod,
                "frontend_ip": frontend_ip,
                "command": command,
                "submitted_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Unexpected error in execute_command: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"예상치 못한 에러: {str(e)}"
            }
    
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
        Backend Pod 할당 해제
        
        작업 완료 후 호출하여 Pod를 Pool로 반환
        
        Args:
            backend_pod: Backend Pod 이름
            
        Returns:
            Dict: 해제 결과
        """
        try:
            # Pod IP 조회하여 Agent에 unmount 요청
            backend_ip = self.pool.get_pod_ip(backend_pod)
            if backend_ip:
                agent = BackendAgent(backend_ip)
                try:
                    agent.unmount()
                finally:
                    agent.close()
            
            # Pool로 반환
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
            logger.error(f"Error releasing backend {backend_pod}: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
