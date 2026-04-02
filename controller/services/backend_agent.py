"""
Backend Agent Client

Backend Pod 내 Agent와 HTTP 통신하는 클라이언트 클래스
"""

import os
from typing import Dict

import httpx


AGENT_PORT = 8080


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


DEFAULT_AGENT_TIMEOUT = _env_float("BACKEND_AGENT_TIMEOUT_SECONDS", 30.0)
MOUNT_TIMEOUT = _env_float("BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)
UNMOUNT_TIMEOUT = _env_float("BACKEND_AGENT_UNMOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)


class BackendAgent:
    """
    Backend Agent 통신용 HTTP 클라이언트
    
    - 특정 Backend Pod IP 타겟 HTTP(/mount, /unmount) 통신
    - 명령어 터미널 진입 전 SSHFS 환경 격리 세팅 및 자원 종료(Lifecycle) 제어
    """
    
    def __init__(self, pod_ip: str):
        self.pod_ip = pod_ip
        self.base_url = f"http://{pod_ip}:{AGENT_PORT}"
        self.client = httpx.Client(timeout=None)
    
    def close(self):
        self.client.close()
    
    def mount(self, frontend_ip: str, command: str, frontend_pod: str = None, timeout: float = MOUNT_TIMEOUT) -> Dict:
        """
        Agent에 마운트 및 명령 실행 요청
        
        Raises:
            httpx.HTTPError: HTTP 통신 에러
        """
        url = f"{self.base_url}/mount"
        payload = {
            "frontend_ip": frontend_ip,
            "command": command
        }
        if frontend_pod:
            payload["frontend_pod"] = frontend_pod
            
        response = self.client.post(url, json=payload, timeout=timeout)
        response.raise_for_status()
        return response.json()

    
    def unmount(self, timeout: float = UNMOUNT_TIMEOUT) -> Dict:
        """
        Agent에 마운트 해제 요청
        
        Raises:
            httpx.HTTPError: HTTP 통신 에러
        """
        url = f"{self.base_url}/unmount"
        
        response = self.client.post(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
