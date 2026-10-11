"""
Compute Agent - HTTP API Server
"""

import asyncio
import logging
import os
import runpy
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

def _config_value_to_env(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _extract_config_file(argv):
    config_file = None
    cleaned = [argv[0]]
    index = 1

    while index < len(argv):
        arg = argv[index]
        if arg == "--config-file":
            if index + 1 >= len(argv):
                raise SystemExit("--config-file requires a path")
            config_file = argv[index + 1]
            index += 2
            continue
        if arg.startswith("--config-file="):
            config_file = arg.split("=", 1)[1]
            index += 1
            continue

        cleaned.append(arg)
        index += 1

    argv[:] = cleaned
    return config_file


def _load_config_defaults(config_file):
    if not config_file:
        return

    config_path = Path(config_file)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path

    values = runpy.run_path(str(config_path))
    os.environ.setdefault("KDA_CONFIG_FILE", str(config_path))

    for name, value in values.items():
        if not name.isupper() or value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            os.environ.setdefault(name, _config_value_to_env(value))

    namespace = values.get("DEFAULT_NAMESPACE")
    if namespace:
        os.environ.setdefault("K8S_NAMESPACE", str(namespace))


_load_config_defaults(_extract_config_file(sys.argv))

from session_handler import session_handler
from workspace_connector import WorkspaceConnector

HTTP_PORT = int(os.getenv("COMPUTE_AGENT_HTTP_PORT") or os.getenv("COMPUTE_AGENT_PORT", "8080"))
TCP_TERMINAL_PORT = int(os.getenv("COMPUTE_AGENT_TCP_PORT", "8081"))
SCRUB_TIMEOUT_SECONDS = float(os.getenv("COMPUTE_AGENT_SCRUB_TIMEOUT_SECONDS", "10"))
# Counted from the end of the kill, so a kill that used up its own deadline
# still leaves the connection handlers time to drop the dead sessions.
SCRUB_SESSION_GRACE_SECONDS = float(os.getenv("COMPUTE_AGENT_SCRUB_SESSION_GRACE_SECONDS", "2"))
SCRUB_POLL_SECONDS = 0.05

_LOG_FORMAT = os.getenv("LOG_FORMAT", "detailed").lower()
_APP_LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
_LOG_LEVEL = getattr(logging, _APP_LOG_LEVEL, logging.INFO)
_handler = logging.StreamHandler()
if _LOG_FORMAT == "json":
    from pythonjsonlogger import jsonlogger
    _handler.setFormatter(jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s',
        rename_fields={'asctime': 'ts', 'levelname': 'level', 'name': 'logger'},
        datefmt='%Y-%m-%dT%H:%M:%S%z',
    ))
    logging.basicConfig(level=_LOG_LEVEL, handlers=[_handler], force=True)
else:
    _handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
    ))
    logging.basicConfig(level=_LOG_LEVEL, handlers=[_handler], force=True)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Compute Agent",
    description="Warm buffer compute pod agent for SSHFS mount and command execution",
    version="1.0.0"
)

workspace_connector = WorkspaceConnector()


class FormattedJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json
        return (json.dumps(content, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


class MountRequest(BaseModel):
    user_pod_ip: str
    command: str
    user_pod: Optional[str] = None
    ticket_id: Optional[str] = None


class ReleaseRequest(BaseModel):
    # The whole body is optional: a controller that sends none still works.
    ticket_id: Optional[str] = None


class AgentStatus:
    IDLE = "idle"
    MOUNTING = "mounting"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


class AgentState:
    READY_STATUSES = {AgentStatus.IDLE, AgentStatus.RUNNING, AgentStatus.COMPLETED}

    def __init__(self):
        self.status = AgentStatus.IDLE
        self.user_pod_ip: Optional[str] = None
        self.user_pod: Optional[str] = None
        self.ticket_id: Optional[str] = None
        self.command: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self._lock = asyncio.Lock()

    async def set_mounting(
        self,
        user_pod_ip: str,
        user_pod: Optional[str],
        command: str,
        ticket_id: Optional[str] = None,
    ):
        async with self._lock:
            self.status = AgentStatus.MOUNTING
            self.user_pod_ip = user_pod_ip
            self.user_pod = user_pod
            self.ticket_id = ticket_id
            self.command = command
            self.started_at = datetime.now()

    async def set_running(self):
        async with self._lock:
            self.status = AgentStatus.RUNNING

    async def set_error(self, error: str):
        async with self._lock:
            self.status = AgentStatus.ERROR
            self.error = error

    async def reset(self):
        async with self._lock:
            self.status = AgentStatus.IDLE
            self.user_pod_ip = None
            self.user_pod = None
            self.ticket_id = None
            self.command = None
            self.started_at = None
            self.error = None

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "status": self.status,
                "user_pod_ip": self.user_pod_ip,
                "user_pod": self.user_pod,
                "ticket_id": self.ticket_id,
                "command": self.command,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "error": self.error,
            }

    async def is_ready(self) -> bool:
        async with self._lock:
            return self.status in self.READY_STATUSES


state = AgentState()

# True while /scrub runs. The agent can be IDLE then (a late scrub of a pod
# already returned), and a /mount landing mid-scrub would have its assignment
# wiped by the scrub. Kept out of the status so /ready stays 200 meanwhile.
scrub_running = False


@app.get("/")
async def root():
    return FormattedJSONResponse({
        "service": "Compute Agent",
        "status": "running"
    })


@app.get("/status")
async def get_status():
    data = await state.snapshot()
    data["sessions"] = session_handler.get_sessions()
    return FormattedJSONResponse(data)


@app.get("/ready")
async def get_ready():
    ready = await state.is_ready()
    snapshot = await state.snapshot()
    return FormattedJSONResponse(
        {
            "status": "ready" if ready else "not_ready",
            "agent_status": snapshot["status"],
            "sessions": len(session_handler.get_sessions()),
        },
        status_code=200 if ready else 503,
    )


@app.post("/mount")
async def mount(request: MountRequest):
    # ERROR means the last cleanup did not finish, so the previous user's
    # processes or files may still be here; a reused pod must not take anyone.
    if scrub_running or state.status not in [AgentStatus.IDLE, AgentStatus.COMPLETED]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is not available (status: {'scrubbing' if scrub_running else state.status})"
        )

    await state.set_mounting(request.user_pod_ip, request.user_pod, request.command, request.ticket_id)

    logger.info(
        "[MountRequested] user_pod=%s user_pod_ip=%s ticket_id=%s",
        request.user_pod or "",
        request.user_pod_ip,
        request.ticket_id or "",
    )

    workspace_connector.attach_user_pod(request.user_pod_ip)

    logger.info("[MountContextAccepted] tcp_port=%s", TCP_TERMINAL_PORT)
    await state.set_running()
    await session_handler.begin_session_lifecycle(
        user_pod_ip=request.user_pod_ip,
        user_pod=request.user_pod or "",
        ticket_id=request.ticket_id or "",
    )

    return FormattedJSONResponse({
        "status": "success",
        "message": "Mount context accepted, ready for TCP connection",
        "user_pod_ip": request.user_pod_ip,
        "tcp_port": TCP_TERMINAL_PORT
    })


def _refuse_other_ticket(request: Optional[ReleaseRequest], event: str) -> Optional[JSONResponse]:
    """A 409 response if the release names a ticket other than the current one.

    On a reused pod a late release for the previous user would otherwise kill
    the next user's sessions and drop their assignment. A release without a
    ticket, or a pod mounted without one, cannot be told apart and goes ahead.
    """
    requested = request.ticket_id if request is not None else None
    current = state.ticket_id
    if not requested or not current or requested == current:
        return None
    logger.warning(
        "[%s] reason=ticket_mismatch ticket_id=%s current_ticket_id=%s",
        event,
        requested,
        current,
    )
    return FormattedJSONResponse(
        {"status": "error", "reason": "ticket_mismatch", "current_ticket_id": current},
        status_code=409,
    )


@app.post("/unmount")
async def unmount(request: Optional[ReleaseRequest] = None):
    refusal = _refuse_other_ticket(request, "UnmountRefused")
    if refusal is not None:
        return refusal

    cleanup_started = time.perf_counter()

    if workspace_connector.user_pod_ip is None:
        logger.info("[Unmounted] cleanup_ms=0 status=already_unmounted")
        return FormattedJSONResponse({
            "status": "success",
            "message": "Already unmounted"
        })

    await session_handler.terminate_all_sessions()

    try:
        workspace_connector.detach_user_pod()
        await state.reset()
        await session_handler.suppress_fallback_release()
        cleanup_ms = int((time.perf_counter() - cleanup_started) * 1000)
        logger.info("[Unmounted] cleanup_ms=%s", cleanup_ms)
        return FormattedJSONResponse({
            "status": "success",
            "message": "Unmounted and reset"
        })
    except Exception as e:
        cleanup_ms = int((time.perf_counter() - cleanup_started) * 1000)
        logger.error("[UnmountFailed] cleanup_ms=%s reason=%r", cleanup_ms, str(e))
        await state.set_error(str(e))
        return FormattedJSONResponse({
            "status": "error",
            "message": str(e)
        })


async def _wait_for_sessions_closed(grace_seconds: float) -> None:
    # A killed session leaves the tracking table only once its connection
    # handler notices, which takes a few event-loop turns. The deadline is
    # checked after sleeping so the handlers always get at least one poll.
    deadline = time.monotonic() + grace_seconds
    while session_handler.get_sessions():
        await asyncio.sleep(SCRUB_POLL_SECONDS)
        if time.monotonic() >= deadline:
            return


@app.post("/scrub")
async def scrub(request: Optional[ReleaseRequest] = None):
    """Clean this pod for the next user instead of deleting it (reuse mode).

    /unmount only forgets the assignment because the pod is deleted right
    after. A reused pod keeps whatever the previous user left running or
    stored outside their mount namespace, so this kills every session process,
    removes what sessions share, then checks the result. HTTP 200 whenever it
    runs: the body's status is the verdict, and a failed scrub leaves the agent
    in ERROR so /mount refuses it. HTTP 409 if the request names another
    ticket, and then nothing is touched.
    """
    global scrub_running

    refusal = _refuse_other_ticket(request, "ScrubRefused")
    if refusal is not None:
        return refusal

    scrub_running = True
    try:
        return await _run_scrub()
    finally:
        scrub_running = False


async def _run_scrub() -> JSONResponse:
    scrub_started = time.perf_counter()
    loop = asyncio.get_running_loop()
    killed = 0
    ipc_cleared = False
    checks: Dict[str, bool] = {}
    error: Optional[str] = None

    try:
        # Close the door first so a late connection from the outgoing user
        # cannot start a session while (or after) the pod is cleaned.
        session_handler.revoke_mount_context()
        # No terminate_all_sessions() here: its SIGTERM-and-wait blocks the
        # event loop, and sessions unshare their mount namespace before exec,
        # so this namespace kill already reaches every one of them.
        killed, _ = await loop.run_in_executor(
            None,
            workspace_connector.kill_session_processes,
            SCRUB_TIMEOUT_SECONDS,
        )
        await _wait_for_sessions_closed(SCRUB_SESSION_GRACE_SECONDS)
        ipc_cleared = await loop.run_in_executor(None, workspace_connector.clear_session_leftovers)
        workspace_connector.detach_user_pod()
        await session_handler.suppress_fallback_release()
        checks = await loop.run_in_executor(None, workspace_connector.scrub_checks)
        checks["no_tracked_sessions"] = not session_handler.get_sessions()
    except Exception as e:
        error = str(e) or type(e).__name__

    scrub_ms = int((time.perf_counter() - scrub_started) * 1000)
    failed_checks = [name for name, ok in checks.items() if not ok]

    if error is None and not failed_checks:
        # Reset only after the checks pass: IDLE is what lets /mount through.
        await state.reset()
        logger.info("[Scrubbed] scrub_ms=%s killed=%s", scrub_ms, killed)
        return FormattedJSONResponse({
            "status": "success",
            "scrub_ms": scrub_ms,
            "killed": killed,
            "ipc_cleared": ipc_cleared,
            "checks": checks,
            "message": "Scrubbed and reset",
        })

    message = error or f"Checks failed: {', '.join(failed_checks)}"
    logger.error(
        "[ScrubFailed] scrub_ms=%s failed_checks=%s reason=%r",
        scrub_ms,
        ",".join(failed_checks) or "-",
        message,
    )
    await state.set_error(message)
    return FormattedJSONResponse({
        "status": "error",
        "scrub_ms": scrub_ms,
        "killed": killed,
        "ipc_cleared": ipc_cleared,
        "checks": checks,
        "message": message,
    })


@app.on_event("startup")
async def startup():
    logger.info("Compute Agent starting...")

    if not workspace_connector.setup_ssh_key():
        logger.error("Compute Agent startup failed: SSH key setup was unsuccessful")
        raise RuntimeError("SSH key setup failed")
    # Before the TCP server opens, so no session can have touched /dev yet.
    workspace_connector.record_dev_entries()
    await session_handler.start()

    logger.info("Compute Agent ready")

    async def print_newline():
        import sys
        await asyncio.sleep(0.5)
        sys.stdout.write("\n")
        sys.stdout.flush()

    asyncio.create_task(print_newline())


@app.on_event("shutdown")
async def shutdown():
    await session_handler.stop()
    logger.info("Compute Agent stopped")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, access_log=False)
