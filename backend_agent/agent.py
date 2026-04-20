"""
Backend Agent - HTTP API Server
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mount_manager import MountManager
from tcp_terminal import tcp_terminal

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
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


class AgentState:
    READY_STATUSES = {"idle", "running", "completed"}

    def __init__(self):
        self.status = "idle"
        self.frontend_ip: Optional[str] = None
        self.frontend_pod: Optional[str] = None
        self.command: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.error: Optional[str] = None
        self._lock = asyncio.Lock()

    async def set_mounting(self, frontend_ip: str, frontend_pod: Optional[str], command: str):
        async with self._lock:
            self.status = "mounting"
            self.frontend_ip = frontend_ip
            self.frontend_pod = frontend_pod
            self.command = command
            self.started_at = datetime.now()

    async def set_running(self):
        async with self._lock:
            self.status = "running"

    async def set_error(self, error: str):
        async with self._lock:
            self.status = "error"
            self.error = error

    async def reset(self):
        async with self._lock:
            self.status = "idle"
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

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "frontend_ip": self.frontend_ip,
            "frontend_pod": self.frontend_pod,
            "command": self.command,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "error": self.error,
        }


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
    if state.status not in ["idle", "completed", "error"]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is busy (status: {state.status})"
        )

    await state.set_mounting(request.frontend_ip, request.frontend_pod, request.command)

    logger.info(
        "Mount requested: frontend=%s(%s), command=%s",
        request.frontend_pod,
        request.frontend_ip,
        request.command,
    )

    mount_success = await mount_manager.mount(request.frontend_ip)

    if not mount_success:
        logger.error("SSHFS mount failed!")
        await state.set_error("SSHFS mount failed")
        return FormattedJSONResponse({
            "status": "error",
            "message": "SSHFS mount failed"
        }, status_code=500)

    logger.info("Mount successful")
    await state.set_running()
    await tcp_terminal.begin_session_lifecycle()

    return FormattedJSONResponse({
        "status": "success",
        "message": "Mount completed, ready for TCP connection",
        "frontend_ip": request.frontend_ip,
        "tcp_port": 8081
    })


@app.post("/unmount")
async def unmount():
    logger.info("Unmount requested")

    if mount_manager.frontend_ip is None:
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
            return FormattedJSONResponse({
                "status": "success",
                "message": "Unmounted and reset"
            })
        return FormattedJSONResponse({
            "status": "warning",
            "message": "Unmount completed with warnings"
        })
    except Exception as e:
        logger.error("Unmount error: %s", e)
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

    uvicorn.run(app, host="0.0.0.0", port=8080, access_log=False)
