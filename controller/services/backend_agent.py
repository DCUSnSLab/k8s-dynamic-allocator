"""
Backend Agent Client

Backend Pod 내 Agent와 HTTP 통신하는 클라이언트 클래스
"""

import logging
from typing import Dict

import httpx

logger = logging.getLogger(__name__)


AGENT_PORT = 8080
AGENT_TIMEOUT = 30.0  # 초


class BackendAgent:
    """
    Backend Agent HTTP 클라이언트
    
    특정 Backend Pod의 Agent와 통신
    - 마운트 요청
    - 상태 조회
    - 마운트 해제
    """
    
    def __init__(self, pod_ip: str):
        """
        Agent 클라이언트 초기화
        
        Args:
            pod_ip: Backend Pod IP
        """
        self.pod_ip = pod_ip
        self.base_url = f"http://{pod_ip}:{AGENT_PORT}"
        self.client = httpx.Client(timeout=AGENT_TIMEOUT)
    
    def close(self):
        """클라이언트 종료"""
        self.client.close()
    
    def mount(self, frontend_ip: str, command: str) -> Dict:
        """
        Agent에 마운트 및 명령 실행 요청
        
        Args:
            frontend_ip: Frontend Pod IP (SSHFS 마운트 대상)
            command: 실행할 명령어
            
        Returns:
            Dict: Agent 응답 {"status": "accepted"} 또는 에러
        """
        url = f"{self.base_url}/mount"
        payload = {
            "frontend_ip": frontend_ip,
            "command": command
        }
        
        try:
            response = self.client.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            logger.info(f"Mount request sent to {self.pod_ip}: {result}")
            return result
        except httpx.HTTPError as e:
            logger.error(f"Failed to send mount request to {self.pod_ip}: {e}")
            return {"status": "error", "message": str(e)}
    
    def get_status(self) -> Dict:
        """
        Agent 상태 조회
        
        Returns:
            Dict: Agent 상태 {"status": "idle|running|completed", ...}
        """
        url = f"{self.base_url}/status"
        
        try:
            response = self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"Failed to get status from {self.pod_ip}: {e}")
            return {"status": "error", "message": str(e)}
    
    def unmount(self) -> Dict:
        """
        Agent에 마운트 해제 요청
        
        Returns:
            Dict: Agent 응답
        """
        url = f"{self.base_url}/unmount"
        
        try:
            response = self.client.post(url)
            response.raise_for_status()
            result = response.json()
            logger.info(f"Unmount request sent to {self.pod_ip}: {result}")
            return result
        except httpx.HTTPError as e:
            logger.error(f"Failed to send unmount request to {self.pod_ip}: {e}")
            return {"status": "error", "message": str(e)}
