"""HTTP client for compute-agent mount, unmount, and scrub endpoints."""

import os
from types import TracebackType
from typing import Dict, Optional, Type

import httpx


AGENT_PORT = int(os.getenv("COMPUTE_AGENT_PORT") or os.getenv("COMPUTE_AGENT_HTTP_PORT", "8080"))


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


DEFAULT_AGENT_TIMEOUT = _env_float("COMPUTE_AGENT_TIMEOUT_SECONDS", 30.0)
MOUNT_TIMEOUT = _env_float("COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)
UNMOUNT_TIMEOUT = _env_float("COMPUTE_AGENT_UNMOUNT_TIMEOUT_SECONDS", DEFAULT_AGENT_TIMEOUT)
# How long the controller waits on /scrub. Must outlast the agent's own kill
# deadline (COMPUTE_AGENT_SCRUB_TIMEOUT_SECONDS, 10 s, read by the agent), or a
# slow scrub that still succeeds reads as a failure here and the Pod is deleted
# instead of returned.
SCRUB_HTTP_TIMEOUT = _env_float("COMPUTE_AGENT_SCRUB_HTTP_TIMEOUT_SECONDS", 20.0)


class ComputeAgentError(Exception):
    """Raised when compute-agent HTTP communication fails."""


class AgentTicketMismatch(ComputeAgentError):
    """The agent serves another ticket than the one named, and touched nothing."""

    def __init__(self, message: str, current_ticket_id: str = "") -> None:
        super().__init__(message)
        self.current_ticket_id = current_ticket_id


def _raise_if_ticket_mismatch(response, operation: str) -> None:
    # Told apart from other failures: the Pod is not the caller's any more, so
    # it must be left alone rather than deleted as a broken one.
    if response.status_code != 409:
        return
    try:
        body = response.json()
    except ValueError:
        return
    if isinstance(body, dict) and body.get("reason") == "ticket_mismatch":
        current = str(body.get("current_ticket_id") or "")
        raise AgentTicketMismatch(
            f"{operation} refused: agent serves ticket {current or 'unknown'}",
            current,
        )


class ComputeAgent:
    """Compute-agent HTTP client for /mount, /unmount, and /scrub."""

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
        ticket_id: Optional[str] = None,
    ) -> Dict:
        payload: Dict = {
            "user_pod_ip": user_pod_ip,
            "command": command,
        }
        if user_pod:
            payload["user_pod"] = user_pod
        # A reused Pod outlives its user; the ticket lets the agent tell this
        # user's connections and fallback release apart from the previous one's.
        if ticket_id:
            payload["ticket_id"] = ticket_id

        try:
            response = self.client.post(f"{self.base_url}/mount", json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise ComputeAgentError(f"mount failed: {exc}") from exc

    def unmount(self, timeout: float = UNMOUNT_TIMEOUT, ticket_id: Optional[str] = None) -> Dict:
        """Raises AgentTicketMismatch if the agent serves a ticket other than ticket_id."""
        payload = {"ticket_id": ticket_id} if ticket_id else None
        try:
            response = self.client.post(f"{self.base_url}/unmount", json=payload, timeout=timeout)
            _raise_if_ticket_mismatch(response, "unmount")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            raise ComputeAgentError(f"unmount failed: {exc}") from exc

    def scrub(self, timeout: float = SCRUB_HTTP_TIMEOUT, ticket_id: Optional[str] = None) -> Dict:
        """Wipe the last session from the Pod so it can serve another user.

        The agent answers 200 even when the scrub fails, so the body decides:
        anything short of status=success raises, and the caller must not reuse
        the Pod. AgentTicketMismatch means the agent serves a ticket other than
        ticket_id and scrubbed nothing.
        """
        payload = {"ticket_id": ticket_id} if ticket_id else None
        try:
            response = self.client.post(f"{self.base_url}/scrub", json=payload, timeout=timeout)
            _raise_if_ticket_mismatch(response, "scrub")
            response.raise_for_status()
            body = response.json()
        except httpx.HTTPError as exc:
            raise ComputeAgentError(f"scrub failed: {exc}") from exc
        except ValueError as exc:
            raise ComputeAgentError(f"scrub failed: unreadable response: {exc}") from exc

        if not isinstance(body, dict) or body.get("status") != "success":
            details = body if isinstance(body, dict) else {}
            raise ComputeAgentError(
                f"scrub failed: status={details.get('status') or 'missing'} "
                f"message={details.get('message') or ''!r} "
                f"checks={details.get('checks') or {}}"
            )
        return body
