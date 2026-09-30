from __future__ import annotations

import asyncio
import re
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator

import asyncssh

from .config import (
    CONNECT_TIMEOUT_SECONDS,
    EXECUTION_MODE_BASELINE_DIRECT,
    MARKER_PREFIX,
    PTY_HEIGHT,
    PTY_TERM_TYPE,
    PTY_WIDTH,
    SETUP_COMMAND_TIMEOUT_SECONDS,
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
# Ways a request has been seen to end with nothing to measure, all of them from a
# user pod being reclaimed as its owner came back:
#   exit 137                  container killed at the end of its grace period
#   "container not found"     container gone, pod object not yet
#   "current phase is Failed" pod reached Failed between the read and the exec
#   exit 0 with one byte      the relay lost the output entirely
# The retry does not match on these. It matches on the outcome - no end marker,
# so nothing to measure - because the list was never complete.
# 128 + SIGKILL: the container was killed, so the shell never reached the end
# marker and never returned a status of its own.
KILLED_EXIT_STATUS = 137
RERUN_MAX_ATTEMPTS = 5
# The pod takes about three seconds to disappear after its container is killed,
# so a pause before reconnecting is what makes the next attempt land on a fresh
# pod rather than the one still going away.
RERUN_DELAY_SECONDS = 2.0
# Authenticating does not build a user pod. swlabssh creates it when a shell
# is requested, so a resume is only over once a command has run, and this is
# the cheapest one that forces it.
RESUME_PROBE_COMMAND = "pwd"
# Measured at 6-9s on an idle cluster. The cluster is shared and pod readiness
# has been seen to vary by nearly an order of magnitude, so this is generous:
# a slow resume is a number worth having, a timed-out one is a lost request.
RESUME_TIMEOUT_SECONDS = 300.0
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
    user_concurrency_delay_ms: float
    ticket_id: str | None
    compute_pod: str | None
    compute_pod_ip: str | None
    timed_out: bool = False
    error: str | None = None
    # Milliseconds from sending the command until each milestone line arrived,
    # counted from the last attempt.
    since_send_to_ticket_ms: float | None = None
    since_send_to_assigned_ms: float | None = None
    since_send_to_start_ms: float | None = None
    since_send_to_end_ms: float | None = None
    # False means the server never accepted the command.
    command_delivered: bool = False
    ssh_attempts: int = 1
    # Time lost on attempts that failed before the last one started.
    ssh_retry_delay_ms: float = 0.0
    # Extra attempts spent because an attempt ended with no end marker, so with
    # nothing to measure. 0 for a request that worked first time.
    reruns: int = 0
    # Set when an attempt delivered an exit status but no end marker: the shell
    # reached its last statement, so the command finished and its output was lost
    # on the way back. Not a failure of the command, and not re-run.
    output_lost: bool = False
    # What the attempts that were thrown away actually said. Without this a
    # re-run can only be diagnosed by arithmetic on the request's duration.
    discarded_output: str = ""
    discarded_exit_status: int | None = None


@dataclass
class ResumeTiming:
    """What it cost to bring a culled user's pod back.

    resume_latency_ms is the part a real user would feel: the SSH connect
    that made swlabssh recreate the pod. background_restore_ms is the
    simulator putting its own idle-load process back, which a user would
    never wait for, so the two are kept apart rather than summed into the
    request.
    """

    resume_latency_ms: float
    background_restore_ms: float | None = None
    # True when another request for this user was already bringing the pod back
    # and this one only waited for it. The wait was real; the work was not ours.
    waited_for_peer: bool = False
    error: str | None = None


@dataclass
class _Attempts:
    """What one run_remote call has spent, and what it threw away.

    A session is shared by every request that user has in flight, so none of this
    can live there: a sibling request would reset it mid-flight and read back the
    other's numbers.
    """

    reruns: int = 0
    output_lost: bool = False
    discarded_exit_status: int | None = None


class _CommandTimeout(Exception):
    pass


class _TimedOutput:
    """Command output, plus when each milestone line first arrived."""

    def __init__(self, started: float) -> None:
        self._started = started
        self._lines: list[str] = []
        # Output from attempts that were discarded, kept so a run that needed
        # several tries can still show what happened on the earlier ones.
        self._failed_output: list[str] = []
        self.stderr = ""
        self.seen_ms: dict[str, float] = {}
        # Set once the server accepted the command; a failure before that is safe to resend.
        self.channel_opened = False

    def restart(self, started: float) -> None:
        self._started = started
        self.channel_opened = False
        # CommandResult documents the milestone times as "counted from the last
        # attempt", and that only holds if the previous attempt's marks go.
        self.seen_ms.clear()
        # The lines go too, now that a retry can be triggered by text in them: a
        # leftover "container not found" from an earlier attempt would make every
        # later attempt look like it had failed the same way.
        self._failed_output.append("".join(self._lines))
        self._lines.clear()

    @property
    def received(self) -> bool:
        return bool(self._lines)

    @property
    def stdout(self) -> str:
        return "".join(self._lines)

    @property
    def discarded_stdout(self) -> str:
        return "".join(self._failed_output)

    def add_line(self, line: str) -> None:
        elapsed_ms = (time.monotonic() - self._started) * 1000.0
        self._lines.append(line)
        text = visible_text(line)
        for name, pattern in MILESTONES:
            if name not in self.seen_ms and pattern.search(text):
                self.seen_ms[name] = elapsed_ms


def strip_ansi(value: str) -> str:
    return ANSI_RE.sub("", value)


def visible_text(line: str) -> str:
    """What a terminal would actually show for this line.

    swlabssh prints a spinner while it builds a user pod, and it prints no
    newline, so the first real line of output arrives with the spinner still
    on the front: backspaces, then a carriage return, then the text. The
    marker patterns are anchored to the line start and have to stay that way,
    because `run` echoes the whole command back, markers included. Dropping
    the backspaces and keeping only what follows the last carriage return
    leaves the text the spinner overwrote itself with, which is what the
    anchor should be tested against.
    """
    text = strip_ansi(line).replace("\x08", "")
    # The trailing newline goes first: otherwise the last carriage-return
    # segment is just that newline and the line looks empty.
    text = text.rstrip("\r\n")
    return text.split("\r")[-1].strip()


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
        self._max_concurrent = config.users.max_concurrent_requests
        # Re-running is only safe where the command is a process in the user's own
        # pod. In kda mode it would submit a second ticket for work that may have
        # completed with only its output lost, so that case is reported instead.
        self._rerun_when_unmeasured = (
            config.execution.mode == EXECUTION_MODE_BASELINE_DIRECT
        )
        self._run_lock = asyncio.Semaphore(self._max_concurrent)
        # Last time this user did anything at all. Set now rather than at the
        # first request so a user who is never scheduled still ages out.
        self._idle_since = time.monotonic()
        self._culled = False
        # One resume per user at a time. Without this, concurrent requests each
        # wait for the pod to go and then race to recreate it, and the losers
        # wait out the full timeout against a pod the winner just created.
        self._resume_lock = asyncio.Lock()

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
        return await self.run_remote("pwd", timeout=SETUP_COMMAND_TIMEOUT_SECONDS)

    async def run_remote(self, remote_command: str, timeout: float) -> CommandResult:
        # Marked on arrival, not just on completion: a request queued behind
        # another one has already made this user active, and the culler must not
        # delete the pod out from under it.
        self._idle_since = time.monotonic()
        lock_wait_started = time.monotonic()
        async with self._run_lock:
            user_concurrency_delay_ms = (time.monotonic() - lock_wait_started) * 1000.0
            started = time.monotonic()
            output = _TimedOutput(started)
            spent = _Attempts()
            exit_status: int | None = None
            timed_out = False
            error: str | None = None
            attempts = [started]
            try:
                exit_status = await self._run_with_retry(
                    remote_command, timeout, output, attempts, spent)
            except _CommandTimeout:
                timed_out = True
                error = f"command timed out after {timeout} seconds"
            except Exception as exc:
                self._connection = None
                error = str(exc)

            parsed = parse_run_output(output.stdout, output.stderr)
            self._idle_since = time.monotonic()
            return CommandResult(
                exit_status=exit_status,
                stdout=output.stdout,
                stderr=output.stderr,
                elapsed_ms=(time.monotonic() - started) * 1000.0,
                user_concurrency_delay_ms=user_concurrency_delay_ms,
                ticket_id=parsed["ticket_id"],
                compute_pod=parsed["compute_pod"],
                compute_pod_ip=parsed["compute_pod_ip"],
                timed_out=timed_out,
                error=error,
                since_send_to_ticket_ms=output.seen_ms.get("ticket"),
                since_send_to_assigned_ms=output.seen_ms.get("allocated"),
                since_send_to_start_ms=output.seen_ms.get("start"),
                since_send_to_end_ms=output.seen_ms.get("end"),
                command_delivered=output.channel_opened or output.received,
                ssh_attempts=len(attempts),
                ssh_retry_delay_ms=(attempts[-1] - started) * 1000.0,
                reruns=spent.reruns,
                output_lost=spent.output_lost,
                discarded_output=output.discarded_stdout[-2000:],
                discarded_exit_status=spent.discarded_exit_status,
            )

    async def _run_with_retry(
        self,
        remote_command: str,
        timeout: float,
        output: _TimedOutput,
        attempts: list[float],
        spent: _Attempts,
    ) -> int | None:
        last_error: str | None = None
        # The upper bound is the reclaim path's; the ordinary transient-error
        # retry below still stops at SSH_RUN_ATTEMPTS.
        for attempt in range(1, RERUN_MAX_ATTEMPTS + 1):
            if attempt > 1:
                if spent.reruns:
                    # Give the pod a moment to finish going. Retrying with no
                    # pause is what made the first version of this fail: the
                    # container was already gone but the pod was not.
                    await asyncio.sleep(RERUN_DELAY_SECONDS)
                attempts.append(time.monotonic())
                output.restart(attempts[-1])
            try:
                await self.connect()
                if self._connection is None:
                    raise RuntimeError("SSH connection was not established")
                try:
                    exit_status = await asyncio.wait_for(
                        self._stream(remote_command, output), timeout
                    )
                except asyncio.TimeoutError as exc:
                    raise _CommandTimeout() from exc
                if self._needs_rerun(output, attempt, exit_status):
                    # Drop the connection so the next attempt reconnects, and let
                    # kubessh build a fresh pod once the old one has gone.
                    self._connection = None
                    spent.reruns += 1
                    spent.discarded_exit_status = exit_status
                    continue
                if "end" not in output.seen_ms and self._shell_finished(output, exit_status):
                    # The shell reached its last line, so the end marker was
                    # printed and lost on the way back. The same predicate the
                    # re-run decision uses, so the two cannot disagree and charge
                    # the server for work it finished.
                    spent.output_lost = True
                return exit_status
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
                # An exception is not a different situation from a clean return
                # that left nothing to measure: either way there is no
                # measurement, and on this arm another attempt allocates nothing.
                # Without this the two paths disagree on how many attempts are
                # allowed - a request that used one attempt on a re-run and then
                # lost its connection had no attempts left, and was reported as
                # an error with nothing measured.
                if not can_retry and _is_transient_ssh_error(exc):
                    # An exception carries no exit status, so the shell did not
                    # finish and another attempt is the right answer.
                    if self._needs_rerun(output, attempt, None):
                        spent.reruns += 1
                        continue
                if not can_retry:
                    raise
        raise RuntimeError(last_error or "SSH command failed")

    def _shell_finished(self, output: _TimedOutput, exit_status: int | None) -> bool:
        """Did the remote shell reach its last line, whatever came back as output?

        The wrapper is `echo START; <command>; exit_code=$?; echo END ...;
        exit $exit_code`, so the end marker is printed and only then does the
        shell exit. An exit status therefore implies the marker was printed, and a
        marker printed but not seen was lost on the way - with the command already
        done. Exit status travels on the SSH channel rather than in the output
        stream, so it arrives even when no output does.

        The status value is the discriminator, not the start marker:

          0             only the wrapper's own `exit $exit_code` produces it, and
                        that line runs after the end marker. kubectl failing, the
                        ticket failing, run failing are all non-zero.
          137           SIGKILL, so the shell never reached its last line.
          None          the channel died before a status arrived.
          other, with a start marker    the shell ran and the command failed; the
                        wrapper prints the end marker for that too.
          other, no start marker        the exec itself failed - "container not
                        found" returns 1 with nothing having run.

        An earlier version asked for the start marker in every case. That was
        right about the container-gone window and wrong in general: a request
        whose output was lost from the very first byte has no start marker either,
        and five of them in one cold-start run were read as unfinished.
        """
        if exit_status == 0:
            return True
        if exit_status is None or exit_status == KILLED_EXIT_STATUS:
            return False
        return "start" in output.seen_ms

    def _needs_rerun(
        self,
        output: _TimedOutput,
        attempt: int,
        exit_status: int | None,
    ) -> bool:
        """Is there nothing to measure, and is running it again the way to get it?

        Only when the shell did not finish. Re-running a command that completed
        repeats its work and bills the run twice for one measurement.
        """
        if not self._rerun_when_unmeasured or attempt >= RERUN_MAX_ATTEMPTS:
            return False
        if "end" in output.seen_ms:
            return False
        return not self._shell_finished(output, exit_status)

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

    @property
    def culled(self) -> bool:
        return self._culled

    def mark_culled(self) -> None:
        self._culled = True

    def unmark_culled(self) -> None:
        """Undo mark_culled when the delete it was set for did not happen."""
        self._culled = False

    def idle_seconds(self) -> float:
        return time.monotonic() - self._idle_since

    @asynccontextmanager
    async def offline(self, slot_timeout: float) -> AsyncIterator[None]:
        """Hold every request slot for this user and drop the connection.

        Raises asyncio.TimeoutError when the slots do not come free in time,
        which means a command is in flight and the user is not idle after
        all.
        """
        acquired = 0
        try:
            for _ in range(self._max_concurrent):
                await asyncio.wait_for(self._run_lock.acquire(), slot_timeout)
                acquired += 1
            # The pod is about to go. Leaving the connection open would have
            # the next request try to use a tunnel into a pod that no longer
            # exists and take the retry path instead of a clean reconnect.
            await self.close()
            yield
        finally:
            for _ in range(acquired):
                self._run_lock.release()

    async def resume_if_culled(
        self,
        restore_command: str | None = None,
    ) -> ResumeTiming | None:
        """Bring a culled user's pod back before their next command is sent.

        Returns None when this user was not culled. This sits outside
        run_remote on purpose: the pod recreation and the background-load
        restart would otherwise land inside the measured command.
        """
        if not self._culled:
            return None
        # Resuming is activity, so the culler leaves this user alone now.
        self._idle_since = time.monotonic()
        started = time.monotonic()
        async with self._resume_lock:
            if not self._culled:
                # Another request for this user got here first and the pod is
                # back. Nothing to do, but the time spent waiting for it is this
                # request's to report.
                self._idle_since = time.monotonic()
                return ResumeTiming(
                    resume_latency_ms=(time.monotonic() - started) * 1000.0,
                    waited_for_peer=True,
                )
            # A request slot as well, so this cannot run while the culler holds
            # them all to delete the pod. offline() never takes the resume lock,
            # so taking them in this order cannot deadlock.
            async with self._run_lock:
                return await self._resume(started, restore_command)

    async def _resume(
        self,
        started: float,
        restore_command: str | None,
    ) -> ResumeTiming:
        try:
            # No check that the old pod has gone. If it has, kubessh builds a new
            # one; if it has not, the attach fails and run_remote's retry
            # reconnects. Both happen in a real deployment when a user comes back
            # exactly as their pod is being reclaimed.
            await self.connect()
            # The pod does not exist yet: authentication alone did not ask for a
            # shell. This probe is what makes swlabssh build it, so it is part of
            # the resume rather than something to leave in the next command.
            await self._run_unlocked(RESUME_PROBE_COMMAND, RESUME_TIMEOUT_SECONDS)
        except Exception as exc:  # noqa: BLE001 - reported on the request
            self._connection = None
            return ResumeTiming(
                resume_latency_ms=(time.monotonic() - started) * 1000.0,
                error=str(exc),
            )
        resume_latency_ms = (time.monotonic() - started) * 1000.0
        self._culled = False

        error: str | None = None
        restore_ms: float | None = None
        if restore_command is not None:
            restore_started = time.monotonic()
            try:
                await self._run_unlocked(
                    restore_command,
                    SETUP_COMMAND_TIMEOUT_SECONDS,
                )
            except Exception as exc:  # noqa: BLE001 - the request can still run
                error = f"background restore failed: {exc}"
            restore_ms = (time.monotonic() - restore_started) * 1000.0
        self._idle_since = time.monotonic()
        # Keyword arguments on purpose: the field order here has changed once
        # already, and a positional call silently put a duration into a bool.
        return ResumeTiming(
            resume_latency_ms=resume_latency_ms,
            background_restore_ms=restore_ms,
            error=error,
        )

    async def _run_unlocked(self, remote_command: str, timeout: float) -> None:
        """Run a command without taking a request slot.

        Only resume_if_culled uses this. It runs before its caller asks for a
        slot, so taking one here would deadlock a user limited to one.
        """
        output = _TimedOutput(time.monotonic())
        await asyncio.wait_for(self._stream(remote_command, output), timeout)

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

    def all(self) -> list[SSHUserSession]:
        return list(self._sessions.values())

    async def close(self) -> None:
        await asyncio.gather(*(session.close() for session in self._sessions.values()), return_exceptions=True)


def _is_transient_ssh_error(exc: Exception) -> bool:
    message = str(exc)
    return any(pattern in message for pattern in TRANSIENT_SSH_ERRORS)
