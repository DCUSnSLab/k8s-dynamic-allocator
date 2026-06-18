from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any

import asyncssh

from .config import (
    COMMAND_TIMEOUT_SECONDS,
    CONNECT_TIMEOUT_SECONDS,
    PER_USER_MAX_INFLIGHT,
    PTY_HEIGHT,
    PTY_TERM_TYPE,
    PTY_WIDTH,
    SimulatorConfig,
)


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TICKET_RE = re.compile(r"\[INFO\]\s+Ticket queued:\s*(?P<ticket_id>[0-9a-fA-F]{32})")
ALLOC_RE = re.compile(
    r"\[INFO\]\s+Compute pod allocated:\s*(?P<compute_pod>\S+) \((?P<compute_pod_ip>[^)]+)\)"
)


@dataclass
class CommandResult:
    exit_status: int | None
    stdout: str
    stderr: str
    elapsed_ms: float
    per_user_queue_delay_ms: float
    ticket_id: str | None
    compute_pod: str | None
    compute_pod_ip: str | None
    timed_out: bool = False
    error: str | None = None


def strip_ansi(value: str) -> str:
    return ANSI_RE.sub("", value)


def parse_run_output(stdout: str, stderr: str) -> dict[str, str | None]:
    text = strip_ansi("\n".join([stdout or "", stderr or ""]))
    ticket_match = TICKET_RE.search(text)
    alloc_match = ALLOC_RE.search(text)
    return {
        "ticket_id": ticket_match.group("ticket_id") if ticket_match else None,
        "compute_pod": alloc_match.group("compute_pod") if alloc_match else None,
        "compute_pod_ip": alloc_match.group("compute_pod_ip") if alloc_match else None,
    }


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


class SSHUserSession:
    def __init__(self, config: SimulatorConfig, username: str) -> None:
        self._config = config
        self.username = username
        self._connection: asyncssh.SSHClientConnection | None = None
        self._connect_lock = asyncio.Lock()
        self._run_lock = asyncio.Semaphore(PER_USER_MAX_INFLIGHT)

    async def connect(self) -> None:
        if self._connection is not None:
            return
        async with self._connect_lock:
            if self._connection is not None:
                return
            self._connection = await asyncssh.connect(
                self._config.ssh.host,
                port=self._config.ssh.port,
                username=self.username,
                password=self._config.password_for(self.username),
                known_hosts=None,
                login_timeout=CONNECT_TIMEOUT_SECONDS,
            )

    async def warmup(self) -> CommandResult:
        return await self.run_remote("pwd", timeout=COMMAND_TIMEOUT_SECONDS)

    async def run_remote(self, remote_command: str, timeout: float) -> CommandResult:
        lock_wait_started = time.monotonic()
        async with self._run_lock:
            per_user_queue_delay_ms = (time.monotonic() - lock_wait_started) * 1000.0
            started = time.monotonic()
            try:
                await self.connect()
                if self._connection is None:
                    raise RuntimeError("SSH connection was not established")
                result = await self._connection.run(
                    remote_command,
                    check=False,
                    timeout=timeout,
                    term_type=PTY_TERM_TYPE,
                    term_size=(
                        PTY_WIDTH,
                        PTY_HEIGHT,
                    ),
                )
                elapsed_ms = (time.monotonic() - started) * 1000.0
                stdout = _to_text(result.stdout)
                stderr = _to_text(result.stderr)
                parsed = parse_run_output(stdout, stderr)
                return CommandResult(
                    exit_status=result.exit_status,
                    stdout=stdout,
                    stderr=stderr,
                    elapsed_ms=elapsed_ms,
                    per_user_queue_delay_ms=per_user_queue_delay_ms,
                    ticket_id=parsed["ticket_id"],
                    compute_pod=parsed["compute_pod"],
                    compute_pod_ip=parsed["compute_pod_ip"],
                )
            except asyncio.TimeoutError:
                elapsed_ms = (time.monotonic() - started) * 1000.0
                return CommandResult(
                    exit_status=None,
                    stdout="",
                    stderr="",
                    elapsed_ms=elapsed_ms,
                    per_user_queue_delay_ms=per_user_queue_delay_ms,
                    ticket_id=None,
                    compute_pod=None,
                    compute_pod_ip=None,
                    timed_out=True,
                    error=f"command timed out after {timeout} seconds",
                )
            except Exception as exc:
                self._connection = None
                elapsed_ms = (time.monotonic() - started) * 1000.0
                return CommandResult(
                    exit_status=None,
                    stdout="",
                    stderr="",
                    elapsed_ms=elapsed_ms,
                    per_user_queue_delay_ms=per_user_queue_delay_ms,
                    ticket_id=None,
                    compute_pod=None,
                    compute_pod_ip=None,
                    error=str(exc),
                )

    async def close(self) -> None:
        if self._connection is None:
            return
        self._connection.close()
        try:
            await self._connection.wait_closed()
        finally:
            self._connection = None


class SSHSessionPool:
    def __init__(self, config: SimulatorConfig) -> None:
        self._sessions = {username: SSHUserSession(config, username) for username in config.user_names()}

    def get(self, username: str) -> SSHUserSession:
        return self._sessions[username]

    async def close(self) -> None:
        await asyncio.gather(*(session.close() for session in self._sessions.values()), return_exceptions=True)
