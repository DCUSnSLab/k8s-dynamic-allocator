"""
Compute Agent Client

Compute Pod 내 Agent와 HTTP 통신하는 클라이언트 클래스
"""

import os
from types import TracebackType
from typing import Dict, Optional, Type

import httpx


AGENT_PORT = int(os.getenv("COMPUTE_AGENT_PORT", "8080"))


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


DEFAULT_AGENT_TIMEOUT = _env_float("COMPUTE_AGENT_TIMEOUT_SECONDS", 30.0)
MOUNT_TIMEOUT = _env_float("COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)
UNMOUNT_TIMEOUT = _env_float("COMPUTE_AGENT_UNMOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)


class ComputeAgentError(Exception):
    """
    Compute agent HTTP 통신 실패를 나타내는 도메인 예외.
    HTTP 구현 세부사항(httpx)은 이 모듈에 갇혀 있고,
    상위 레이어에는 'ComputeAgentError'라는 도메인 예외만 노출
    """


class ComputeAgent:
    """Compute Pod Agent HTTP 클라이언트 (/mount, /unmount)"""

    def __init__(self, pod_ip: str) -> None:
        self.pod_ip = pod_ip
        self.base_url = f"http://{pod_ip}:{AGENT_PORT}"
        self.client = httpx.Client(timeout=DEFAULT_AGENT_TIMEOUT)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "ComputeAgent":
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
        user_pod_ip: str,
        command: str,
        user_pod: Optional[str] = None,
        timeout: float = MOUNT_TIMEOUT,
    ) -> Dict:
        payload: Dict = {
            "user_pod_ip": user_pod_ip,
            "command": command,
        }
        if user_pod:
            payload["user_pod"] = user_pod

        try:
            response = self.client.post(f"{self.base_url}/mount", json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise ComputeAgentError(f"mount failed: {exc}") from exc

    def unmount(self, timeout: float = UNMOUNT_TIMEOUT) -> Dict:
        try:
            response = self.client.post(f"{self.base_url}/unmount", timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise ComputeAgentError(f"unmount failed: {exc}") from exc
