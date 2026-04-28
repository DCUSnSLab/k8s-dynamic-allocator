"""
Backend Agent - HTTP API Server
"""

import asyncio
import logging
import os
import time
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mount_manager import MountManager
from tcp_terminal import tcp_terminal

HTTP_PORT = int(os.getenv("BACKEND_AGENT_HTTP_PORT", "8080"))
TCP_TERMINAL_PORT = int(os.getenv("BACKEND_AGENT_TCP_PORT", "8081"))

_LOG_FORMAT = os.getenv("LOG_FORMAT", "detailed").lower()
_handler = logging.StreamHandler()
if _LOG_FORMAT == "json":
    from pythonjsonlogger import jsonlogger
    _handler.setFormatter(jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s',
        rename_fields={'asctime': 'ts', 'levelname': 'level', 'name': 'logger'},
        datefmt='%Y-%m-%dT%H:%M:%S%z',
    ))
    logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
else:
    _handler.setFormatter(logging.Formatter(
        fmt='[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S %z',
    ))
    logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Backend Agent",
    description="Backend Pool Pod Agent for SSHFS mount and command execution",
    version="1.0.0"
)

mount_manager = MountManager()


class FormattedJSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        import json
        return (json.dumps(content, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


class MountRequest(BaseModel):
    frontend_ip: str
    command: str
    frontend_pod: Optional[str] = None


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
        self.frontend_ip: Optional[str] = None
        self.frontend_pod: Optional[str] = None
        self.command: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self._lock = asyncio.Lock()

    async def set_mounting(self, frontend_ip: str, frontend_pod: Optional[str], command: str):
        async with self._lock:
            self.status = AgentStatus.MOUNTING
            self.frontend_ip = frontend_ip
            self.frontend_pod = frontend_pod
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
            self.frontend_ip = None
            self.frontend_pod = None
            self.command = None
            self.started_at = None
            self.error = None

    async def snapshot(self) -> dict:
        async with self._lock:
            return {
                "status": self.status,
                "frontend_ip": self.frontend_ip,
                "frontend_pod": self.frontend_pod,
                "command": self.command,
                "started_at": self.started_at.isoformat() if self.started_at else None,
                "error": self.error,
            }

    async def is_ready(self) -> bool:
        async with self._lock:
            return self.status in self.READY_STATUSES


state = AgentState()


@app.get("/")
async def root():
    return FormattedJSONResponse({
        "service": "Backend Agent",
        "status": "running"
    })


@app.get("/status")
async def get_status():
    data = await state.snapshot()
    data["sessions"] = tcp_terminal.get_sessions()
    return FormattedJSONResponse(data)


@app.get("/ready")
async def get_ready():
    ready = await state.is_ready()
    snapshot = await state.snapshot()
    return FormattedJSONResponse(
        {
            "status": "ready" if ready else "not_ready",
            "agent_status": snapshot["status"],
            "sessions": len(tcp_terminal.get_sessions()),
        },
        status_code=200 if ready else 503,
    )


@app.post("/mount")
async def mount(request: MountRequest):
    if state.status not in [AgentStatus.IDLE, AgentStatus.COMPLETED, AgentStatus.ERROR]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is busy (status: {state.status})"
        )

    await state.set_mounting(request.frontend_ip, request.frontend_pod, request.command)

    logger.info(
        "[MountRequested] frontend_pod=%s frontend_ip=%s",
        request.frontend_pod or "",
        request.frontend_ip,
    )

    mount_success = await mount_manager.mount(request.frontend_ip)

    if not mount_success:
        logger.error("[MountFailed] reason=%r", "SSHFS mount failed")
        await state.set_error("SSHFS mount failed")
        return FormattedJSONResponse({
            "status": "error",
            "message": "SSHFS mount failed"
        }, status_code=500)

    logger.info("[MountContextAccepted] tcp_port=%s", TCP_TERMINAL_PORT)
    await state.set_running()
    await tcp_terminal.begin_session_lifecycle(
        frontend_ip=request.frontend_ip,
        frontend_pod=request.frontend_pod or "",
    )

    return FormattedJSONResponse({
        "status": "success",
        "message": "Mount context accepted, ready for TCP connection",
        "frontend_ip": request.frontend_ip,
        "tcp_port": TCP_TERMINAL_PORT
    })


@app.post("/unmount")
async def unmount():
    cleanup_started = time.perf_counter()

    if mount_manager.frontend_ip is None:
        logger.info("[Unmounted] cleanup_ms=0 status=already_unmounted")
        return FormattedJSONResponse({
            "status": "success",
            "message": "Already unmounted"
        })

    await tcp_terminal.terminate_all_sessions()

    try:
        success = await mount_manager.unmount()
        if success:
            await state.reset()
            await tcp_terminal.suppress_fallback_release()
            cleanup_ms = int((time.perf_counter() - cleanup_started) * 1000)
            logger.info("[Unmounted] cleanup_ms=%s", cleanup_ms)
            return FormattedJSONResponse({
                "status": "success",
                "message": "Unmounted and reset"
            })
        return FormattedJSONResponse({
            "status": "warning",
            "message": "Unmount completed with warnings"
        })
    except Exception as e:
        cleanup_ms = int((time.perf_counter() - cleanup_started) * 1000)
        logger.error("[UnmountFailed] cleanup_ms=%s reason=%r", cleanup_ms, str(e))
        return FormattedJSONResponse({
            "status": "error",
            "message": str(e)
        })


@app.on_event("startup")
async def startup():
    logger.info("Backend Agent starting...")

    if not mount_manager.setup_ssh_key():
        logger.error("Backend Agent startup failed: SSH key setup was unsuccessful")
        raise RuntimeError("SSH key setup failed")
    await tcp_terminal.start()

    logger.info("Backend Agent ready")

    async def print_newline():
        import sys
        await asyncio.sleep(0.5)
        sys.stdout.write("\n")
        sys.stdout.flush()

    asyncio.create_task(print_newline())


@app.on_event("shutdown")
async def shutdown():
    await tcp_terminal.stop()
    logger.info("Backend Agent stopped")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, access_log=False)
