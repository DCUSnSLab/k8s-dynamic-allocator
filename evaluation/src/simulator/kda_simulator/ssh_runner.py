from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass

import asyncssh

from .config import (
    COMMAND_TIMEOUT_SECONDS,
    CONNECT_TIMEOUT_SECONDS,
    MARKER_PREFIX,
    PTY_HEIGHT,
    PTY_TERM_TYPE,
    PTY_WIDTH,
    SSH_KEEPALIVE_COUNT_MAX,
    SSH_KEEPALIVE_INTERVAL_SECONDS,
    SimulatorConfig,
)


# Any CSI sequence: colors, and the "\x1b[K" line erase the PTY puts before the first line.
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
TICKET_RE = re.compile(r"\[INFO\]\s+Ticket queued:\s*(?P<ticket_id>[0-9a-fA-F]{32})")
ALLOC_RE = re.compile(
    r"\[INFO\]\s+Compute pod allocated:\s*(?P<compute_pod>\S+) \((?P<compute_pod_ip>[^)]+)\)"
)
# Anchored to the line start: `run` first echoes the whole command, markers included.
START_RE = re.compile(rf"^{re.escape(MARKER_PREFIX)}_START\s")
END_RE = re.compile(rf"^{re.escape(MARKER_PREFIX)}_END\s")
MILESTONES = (
    ("ticket", TICKET_RE),
    ("allocated", ALLOC_RE),
    ("start", START_RE),
    ("end", END_RE),
)
SSH_RUN_ATTEMPTS = 2
SSH_RETRY_MAX_ELAPSED_SECONDS = 3.0
TRANSIENT_SSH_ERRORS = (
    "SSH connection closed",
    "Connection lost",
    "Connection reset",
    "Connection aborted",
    "not responding to keepalive",
    "Login timeout expired",
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
    # Milliseconds from sending the command until each milestone line arrived,
    # counted from the last attempt.
    until_ticket_ms: float | None = None
    until_allocated_ms: float | None = None
    until_start_ms: float | None = None
    until_end_ms: float | None = None
    # False means the server never accepted the command.
    command_delivered: bool = False
    ssh_attempts: int = 1
    # Time lost on attempts that failed before the last one started.
    ssh_retry_ms: float = 0.0


class _CommandTimeout(Exception):
    pass


class _TimedOutput:
    """Command output, plus when each milestone line first arrived."""

    def __init__(self, started: float) -> None:
        self._started = started
        self._lines: list[str] = []
        self.stderr = ""
        self.seen_ms: dict[str, float] = {}
        # Set once the server accepted the command; a failure before that is safe to resend.
        self.channel_opened = False

    def restart(self, started: float) -> None:
        self._started = started
        self.channel_opened = False

    @property
    def received(self) -> bool:
        return bool(self._lines)

    @property
    def stdout(self) -> str:
        return "".join(self._lines)

    def add_line(self, line: str) -> None:
        elapsed_ms = (time.monotonic() - self._started) * 1000.0
        self._lines.append(line)
        text = strip_ansi(line).strip()
        for name, pattern in MILESTONES:
            if name not in self.seen_ms and pattern.search(text):
                self.seen_ms[name] = elapsed_ms


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


class SSHUserSession:
    def __init__(self, config: SimulatorConfig, username: str) -> None:
        self._config = config
        self.username = username
        self._connection: asyncssh.SSHClientConnection | None = None
        self._connect_lock = asyncio.Lock()
        self._run_lock = asyncio.Semaphore(config.users.max_concurrent_requests)

    async def connect(self) -> None:
        if self._connection is not None and self._connection_is_closed():
            self._connection = None
        if self._connection is not None:
            return
        async with self._connect_lock:
            if self._connection is not None and self._connection_is_closed():
                self._connection = None
            if self._connection is not None:
                return
            self._connection = await asyncssh.connect(
                self._config.ssh.host,
                port=self._config.ssh.port,
                username=self.username,
                password=self._config.password_for(self.username),
                known_hosts=None,
                login_timeout=CONNECT_TIMEOUT_SECONDS,
                keepalive_interval=SSH_KEEPALIVE_INTERVAL_SECONDS,
                keepalive_count_max=SSH_KEEPALIVE_COUNT_MAX,
            )

    async def warmup(self) -> CommandResult:
        return await self.run_remote("pwd", timeout=COMMAND_TIMEOUT_SECONDS)

    async def run_remote(self, remote_command: str, timeout: float) -> CommandResult:
        lock_wait_started = time.monotonic()
        async with self._run_lock:
            per_user_queue_delay_ms = (time.monotonic() - lock_wait_started) * 1000.0
            started = time.monotonic()
            output = _TimedOutput(started)
            exit_status: int | None = None
            timed_out = False
            error: str | None = None
            attempts = [started]
            try:
                exit_status = await self._run_with_retry(remote_command, timeout, output, attempts)
            except _CommandTimeout:
                timed_out = True
                error = f"command timed out after {timeout} seconds"
            except Exception as exc:
                self._connection = None
                error = str(exc)

            parsed = parse_run_output(output.stdout, output.stderr)
            return CommandResult(
                exit_status=exit_status,
                stdout=output.stdout,
                stderr=output.stderr,
                elapsed_ms=(time.monotonic() - started) * 1000.0,
                per_user_queue_delay_ms=per_user_queue_delay_ms,
                ticket_id=parsed["ticket_id"],
                compute_pod=parsed["compute_pod"],
                compute_pod_ip=parsed["compute_pod_ip"],
                timed_out=timed_out,
                error=error,
                until_ticket_ms=output.seen_ms.get("ticket"),
                until_allocated_ms=output.seen_ms.get("allocated"),
                until_start_ms=output.seen_ms.get("start"),
                until_end_ms=output.seen_ms.get("end"),
                command_delivered=output.channel_opened or output.received,
                ssh_attempts=len(attempts),
                ssh_retry_ms=(attempts[-1] - started) * 1000.0,
            )

    async def _run_with_retry(
        self,
        remote_command: str,
        timeout: float,
        output: _TimedOutput,
        attempts: list[float],
    ) -> int | None:
        last_error: str | None = None
        for attempt in range(1, SSH_RUN_ATTEMPTS + 1):
            if attempt > 1:
                attempts.append(time.monotonic())
                output.restart(attempts[-1])
            try:
                await self.connect()
                if self._connection is None:
                    raise RuntimeError("SSH connection was not established")
                try:
                    return await asyncio.wait_for(self._stream(remote_command, output), timeout)
                except asyncio.TimeoutError as exc:
                    raise _CommandTimeout() from exc
            except _CommandTimeout:
                raise
            except Exception as exc:
                last_error = str(exc)
                self._connection = None
                # Once output arrives the remote side has acted on the command,
                # so running it again would submit a second request. An accepted
                # command with no output yet may already be running, so only a
                # quick failure is retried; one never accepted is always safe.
                can_retry = (
                    attempt < SSH_RUN_ATTEMPTS
                    and not output.received
                    and _is_transient_ssh_error(exc)
                    and (
                        not output.channel_opened
                        or (time.monotonic() - attempts[-1]) <= SSH_RETRY_MAX_ELAPSED_SECONDS
                    )
                )
                if not can_retry:
                    raise
        raise RuntimeError(last_error or "SSH command failed")

    async def _stream(self, remote_command: str, output: _TimedOutput) -> int | None:
        process = await self._connection.create_process(
            remote_command,
            term_type=PTY_TERM_TYPE,
            term_size=(PTY_WIDTH, PTY_HEIGHT),
            encoding="utf-8",
            errors="replace",
        )
        output.channel_opened = True
        try:
            async def read_stdout() -> None:
                async for line in process.stdout:
                    output.add_line(line)

            _, output.stderr = await asyncio.gather(read_stdout(), process.stderr.read())
            completed = await process.wait()
            return completed.exit_status
        finally:
            process.close()

    async def close(self) -> None:
        if self._connection is None:
            return
        self._connection.close()
        try:
            await self._connection.wait_closed()
        finally:
            self._connection = None

    def _connection_is_closed(self) -> bool:
        if self._connection is None:
            return True
        is_closed = getattr(self._connection, "is_closed", None)
        if callable(is_closed):
            return bool(is_closed())
        return False


class SSHSessionPool:
    def __init__(self, config: SimulatorConfig) -> None:
        self._sessions = {username: SSHUserSession(config, username) for username in config.user_names()}

    def get(self, username: str) -> SSHUserSession:
        return self._sessions[username]

    async def close(self) -> None:
        await asyncio.gather(*(session.close() for session in self._sessions.values()), return_exceptions=True)


def _is_transient_ssh_error(exc: Exception) -> bool:
    message = str(exc)
    return any(pattern in message for pattern in TRANSIENT_SSH_ERRORS)
