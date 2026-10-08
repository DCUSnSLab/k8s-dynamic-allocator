"""Delete a user's pod once they have gone T minutes without a request.

This is the idle-baseline arm. The baseline arm keeps a high-spec pod alive for
every user for the whole run, so an idle user costs exactly as much as a busy
one. Here the pod is removed while nobody is using it, and swlabssh recreates it
on the next SSH connect; how long that recreation takes is the resume latency
this arm exists to measure.

Nothing on the server changes for this. A user pod is created on connect and is
not owned by any Deployment, so deleting it is enough and the controller is not
involved. The PVC is deliberately left in place, so the user's files come back
with the pod: a culled session resumes, it does not start over.
"""

from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import KUBERNETES_NAMESPACE


# The delete call itself only has to be accepted by the API server; the pod's
# grace period plays out afterwards and is waited for on the resume side.
DELETE_TIMEOUT_SECONDS = 30.0
# How long to wait for a user's request slots to come free before giving up on
# culling them this sweep. A user with a command in flight is not idle.
SLOT_TIMEOUT_SECONDS = 5.0
STOP_GRACE_SECONDS = 60.0
# The cull is now a single API call per user, so the bound is only here to keep
# a hundred kubectl processes from starting at the same instant. It no longer
# affects how late a cull can be.
MAX_CONCURRENT_CULLS = 25
# Deleting a user pod was measured at about 32 seconds end to end. Generous
# against that, because a wait that ends early puts the resume back on the
# broken path while a slow one only reports a larger resume latency - which is
# this arm's real cost either way.
POD_GONE_TIMEOUT_SECONDS = 180.0
POD_GONE_POLL_SECONDS = 2.0
POD_GET_TIMEOUT_SECONDS = 30.0


async def wait_for_pod_gone(
    username: str,
    timeout: float = POD_GONE_TIMEOUT_SECONDS,
) -> str | None:
    """Wait until the user's pod is out of the API. None once it is gone.

    The other half of _delete_pod, which deliberately returns as soon as the
    API server accepts the delete. Somebody has to wait out the grace period,
    and it is whoever resumes - they are the party losing the time, and this
    arm exists to report exactly that loss.

    Waiting is not optional. While the pod object is still there swlabssh sees
    a pod for the user and does not build a replacement, but its container is
    already dead, so every attach returns `container not found ("shell")`. The
    attach itself succeeds, so the resume looks fine (2-3s instead of the 20s a
    real recreation takes) and the measured command starts inside a pod that is
    being killed. On 10/8 that cost 3 of 91 requests, each one spending all five
    re-run attempts on the same error: the re-run pause is 2 seconds, sized for
    a pod that is already going, not for a 32-second grace period.

    Returns a note instead of raising when the pod outlives the timeout or
    kubectl cannot say. The caller carries on in that case: that is the old
    behaviour, with the re-run path as the backstop, and a resume blocked here
    forever would cost the run more than the request it is protecting.
    """
    deadline = time.monotonic() + timeout
    while True:
        exists, error = await _pod_exists(username)
        if error is not None:
            return f"could not tell whether {pod_name(username)} is gone: {error}"
        if not exists:
            return None
        if time.monotonic() >= deadline:
            return f"{pod_name(username)} was still there after {timeout:.0f}s"
        await asyncio.sleep(POD_GONE_POLL_SECONDS)


async def _pod_exists(username: str) -> tuple[bool, str | None]:
    """Whether the pod object is in the API, and why the answer is unknown."""
    process = await asyncio.create_subprocess_exec(
        "kubectl",
        "--namespace",
        KUBERNETES_NAMESPACE,
        "get",
        "pod",
        pod_name(username),
        "--ignore-not-found=true",
        "-o",
        "name",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=POD_GET_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        return False, "kubectl get did not return"
    except asyncio.CancelledError:
        process.kill()
        raise
    if process.returncode != 0:
        return False, stderr.decode(errors="replace").strip() or f"kubectl exited {process.returncode}"
    # --ignore-not-found makes an absent pod an empty success rather than an
    # error, so there is nothing to distinguish from a name here.
    return bool(stdout.decode(errors="replace").strip()), None


def pod_name(username: str) -> str:
    """The pod swlabssh creates for a user.

    kubessh builds this as 'ssh-{username}' after escaping the username down to
    lowercase letters and digits. Usernames here are generated as prefix+index
    and are validated to be safe already, so the escape is the identity.
    """
    return f"ssh-{username}"


class IdleCuller:
    """Watches per-user idle time and deletes the pods that pass T."""

    def __init__(self, config: Any, sessions: Any, path: Path) -> None:
        self._sessions = sessions
        self._path = path
        self._idle_seconds = float(config.idle_culling.idle_minutes) * 60.0
        self._poll_seconds = float(config.idle_culling.poll_seconds)
        self._sem = asyncio.Semaphore(min(MAX_CONCURRENT_CULLS, config.setup.max_inflight))
        self._file = None
        self._stopping = False
        self._task: asyncio.Task[None] | None = None
        self.culled = 0
        self.failed = 0
        self.skipped_busy = 0
        # Idle time at the moment each pod was actually deleted. The gap between
        # this and the threshold is the threshold the run really enforced, which
        # is not the one it was configured with once a sweep has a queue.
        self.idle_at_cull: list[float] = []

    async def start(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("a", encoding="utf-8")
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop culling. Never raises: the run's own teardown depends on it."""
        self._stopping = True
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=STOP_GRACE_SECONDS)
            except Exception:  # noqa: BLE001 - a stuck culler must not block teardown
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
        if self._file is not None:
            self._file.close()

    def to_dict(self) -> dict[str, Any]:
        late = sorted(v - self._idle_seconds for v in self.idle_at_cull)
        return {
            "culled": self.culled,
            "failed": self.failed,
            "skipped_busy": self.skipped_busy,
            "idle_minutes": self._idle_seconds / 60.0,
            # How much later than the threshold each cull landed. A sweep that
            # has more users due than it can delete at once pushes this up, and
            # the pods stay alive for the difference.
            "cull_lateness_s": {
                "count": len(late),
                "min": late[0] if late else None,
                "p50": late[len(late) // 2] if late else None,
                "p95": late[min(len(late) - 1, int(len(late) * 0.95))] if late else None,
                "max": late[-1] if late else None,
            },
            "effective_idle_minutes_max": (
                (self._idle_seconds + late[-1]) / 60.0 if late else None
            ),
        }

    async def _run(self) -> None:
        while not self._stopping:
            await asyncio.sleep(self._poll_seconds)
            if self._stopping:
                return
            try:
                await self._sweep()
            except Exception as exc:  # noqa: BLE001 - a culler must not end the run
                print(f"Idle culling sweep failed: {exc}")

    async def _sweep(self) -> None:
        due = [
            session
            for session in self._sessions.all()
            if not session.culled and session.idle_seconds() >= self._idle_seconds
        ]
        if not due:
            return
        await asyncio.gather(*(self._cull_bounded(session) for session in due))

    async def _cull_bounded(self, session: Any) -> None:
        async with self._sem:
            if self._stopping:
                return
            await self._cull(session)

    async def _cull(self, session: Any) -> None:
        try:
            async with session.offline(SLOT_TIMEOUT_SECONDS):
                # Re-checked while holding the slots: a request that arrived
                # since the sweep listed this user has reset the clock, and
                # deleting the pod under it would charge that request a resume
                # it never earned.
                if session.idle_seconds() < self._idle_seconds:
                    return
                idle_at_cull = session.idle_seconds()
                started = time.monotonic()
                # Flagged before the delete, not after. The delete takes about
                # 32 seconds, and a request that arrives inside that window has
                # to know the pod is going: otherwise it waits for the slot and
                # then rebuilds the pod itself, and the resume it paid for is
                # recorded as part of the command.
                session.mark_culled()
                error = await _delete_pod(session.username)
                delete_ms = (time.monotonic() - started) * 1000.0
                if error is None:
                    self.culled += 1
                    self.idle_at_cull.append(idle_at_cull)
                else:
                    # The pod may well still be there, so do not make the next
                    # request pay for a resume that never happened.
                    session.unmark_culled()
                    self.failed += 1
        except asyncio.TimeoutError:
            # The user has a command in flight, so they are not idle after all.
            self.skipped_busy += 1
            return

        self._write(
            {
                "type": "CULL",
                "observed_at": _now(),
                "username": session.username,
                "pod": pod_name(session.username),
                "idle_seconds": round(idle_at_cull, 3),
                "delete_request_ms": delete_ms,
                "error": error,
            }
        )

    def _write(self, record: dict[str, Any]) -> None:
        if self._file is None or self._file.closed:
            return
        self._file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        self._file.flush()


async def _delete_pod(username: str) -> str | None:
    """Ask for the pod to go. Returns None on success, else the error text.

    Deliberately does not wait for the pod to disappear. The call sets
    deletionTimestamp and returns; the grace period that follows is waited for by
    whichever request resumes this user, which is the party that actually loses
    the time.
    """
    process = await asyncio.create_subprocess_exec(
        "kubectl",
        "--namespace",
        KUBERNETES_NAMESPACE,
        "delete",
        "pod",
        pod_name(username),
        "--ignore-not-found=true",
        "--wait=false",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=DELETE_TIMEOUT_SECONDS + 30.0,
        )
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        return "kubectl delete did not return"
    except asyncio.CancelledError:
        # Teardown cancelled the sweep. Take the subprocess with it rather than
        # leaving a kubectl running after the run has reported itself finished.
        process.kill()
        raise
    if process.returncode != 0:
        return stderr.decode(errors="replace").strip() or f"kubectl exited {process.returncode}"
    return None


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")

