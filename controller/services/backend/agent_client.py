"""
Backend Agent Client

Backend Pod 내 Agent와 HTTP 통신하는 클라이언트 클래스
"""

import os
from types import TracebackType
from typing import Dict, Optional, Type

import httpx


AGENT_PORT = int(os.getenv("BACKEND_AGENT_PORT", "8080"))


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


DEFAULT_AGENT_TIMEOUT = _env_float("BACKEND_AGENT_TIMEOUT_SECONDS", 30.0)
MOUNT_TIMEOUT = _env_float("BACKEND_AGENT_MOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)
UNMOUNT_TIMEOUT = _env_float("BACKEND_AGENT_UNMOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)


class BackendAgentError(Exception):
    """
    Backend agent HTTP 통신 실패를 나타내는 도메인 예외.
    HTTP 구현 세부사항(httpx)은 이 모듈에 갇혀 있고,
    상위 레이어에는 'BackendAgentError'라는 도메인 예외만 노출
    """


class BackendAgent:
    """Backend Pod Agent HTTP 클라이언트 (/mount, /unmount)"""

    def __init__(self, pod_ip: str) -> None:
        self.pod_ip = pod_ip
        self.base_url = f"http://{pod_ip}:{AGENT_PORT}"
        self.client = httpx.Client(timeout=DEFAULT_AGENT_TIMEOUT)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "BackendAgent":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    def mount(
        self,
        frontend_ip: str,
        command: str,
        frontend_pod: Optional[str] = None,
        timeout: float = MOUNT_TIMEOUT,
    ) -> Dict:
        payload: Dict = {
            "frontend_ip": frontend_ip,
            "command": command,
        }
        if frontend_pod:
            payload["frontend_pod"] = frontend_pod

        try:
            response = self.client.post(f"{self.base_url}/mount", json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise BackendAgentError(f"mount failed: {exc}") from exc

    def unmount(self, timeout: float = UNMOUNT_TIMEOUT) -> Dict:
        try:
            response = self.client.post(f"{self.base_url}/unmount", timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise BackendAgentError(f"unmount failed: {exc}") from exc
