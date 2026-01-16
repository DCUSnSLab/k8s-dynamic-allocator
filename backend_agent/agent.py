"""
Backend Agent - FastAPI Server

Backend Pool Pod 내에서 실행되는 경량 HTTP 서버
- SSHFS 마운트 관리
- 명령어 실행
- 상태 조회
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from mount_manager import MountManager
from task_executor import TaskExecutor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI 앱
app = FastAPI(
    title="Backend Agent",
    description="Backend Pool Pod Agent for SSHFS mount and command execution",
    version="1.0.0"
)

# 컴포넌트 인스턴스
mount_manager = MountManager()
task_executor = TaskExecutor()


# === Request/Response 모델 ===

class MountRequest(BaseModel):
    """마운트 요청"""
    frontend_ip: str
    command: str


class MountResponse(BaseModel):
    """마운트 응답"""
    status: str
    message: Optional[str] = None


class StatusResponse(BaseModel):
    """상태 응답"""
    status: str  # idle, mounting, running, completed, error
    frontend_ip: Optional[str] = None
    command: Optional[str] = None
    result: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class UnmountResponse(BaseModel):
    """마운트 해제 응답"""
    status: str
    message: Optional[str] = None


# === 상태 관리 ===

class AgentState:
    """Agent 상태 관리"""
    def __init__(self):
        self.status = "idle"
        self.frontend_ip: Optional[str] = None
        self.command: Optional[str] = None
        self.result: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self._lock = asyncio.Lock()
    
    async def set_mounting(self, frontend_ip: str, command: str):
        async with self._lock:
            self.status = "mounting"
            self.frontend_ip = frontend_ip
            self.command = command
            self.started_at = datetime.now()
            self.completed_at = None
            self.result = None
    
    async def set_running(self):
        async with self._lock:
            self.status = "running"
    
    async def set_completed(self, result: str):
        async with self._lock:
            self.status = "completed"
            self.result = result
            self.completed_at = datetime.now()
    
    async def set_error(self, error: str):
        async with self._lock:
            self.status = "error"
            self.result = error
            self.completed_at = datetime.now()
    
    async def reset(self):
        async with self._lock:
            self.status = "idle"
            self.frontend_ip = None
            self.command = None
            self.result = None
            self.started_at = None
            self.completed_at = None
    
    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "frontend_ip": self.frontend_ip,
            "command": self.command,
            "result": self.result,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None
        }


state = AgentState()


# === API 엔드포인트 ===

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {"service": "Backend Agent", "status": "running"}


@app.get("/status", response_model=StatusResponse)
async def get_status():
    """
    Agent 상태 조회
    
    Returns:
        StatusResponse: 현재 상태
    """
    return StatusResponse(**state.to_dict())


@app.post("/mount", response_model=MountResponse)
async def mount(request: MountRequest):
    """
    Frontend에 SSHFS 마운트 및 명령 실행
    
    비동기로 실행되며 즉시 반환
    
    Args:
        request: 마운트 요청 (frontend_ip, command)
        
    Returns:
        MountResponse: 요청 수락 응답
    """
    if state.status not in ["idle", "completed", "error"]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is busy (status: {state.status})"
        )
    
    logger.info(f"Mount request received: frontend={request.frontend_ip}, command={request.command}")
    
    # 백그라운드 태스크로 실행
    asyncio.create_task(execute_mount_and_run(request.frontend_ip, request.command))
    
    return MountResponse(status="accepted", message="Mount and execute task started")


@app.post("/unmount", response_model=UnmountResponse)
async def unmount():
    """
    SSHFS 마운트 해제 및 상태 초기화
    
    Returns:
        UnmountResponse: 마운트 해제 결과
    """
    logger.info("Unmount request received")
    
    try:
        # 마운트 해제
        success = await mount_manager.unmount()
        
        if success:
            await state.reset()
            return UnmountResponse(status="success", message="Unmounted and reset")
        else:
            return UnmountResponse(status="warning", message="Unmount completed with warnings")
    except Exception as e:
        logger.error(f"Unmount error: {e}")
        return UnmountResponse(status="error", message=str(e))


# === 백그라운드 태스크 ===

async def execute_mount_and_run(frontend_ip: str, command: str):
    """
    마운트 및 명령 실행 (백그라운드)
    
    Args:
        frontend_ip: Frontend Pod IP
        command: 실행할 명령어
    """
    try:
        # 1. 상태 업데이트: mounting
        await state.set_mounting(frontend_ip, command)
        
        # 2. SSHFS 마운트
        logger.info(f"Mounting frontend: {frontend_ip}")
        mount_success = await mount_manager.mount(frontend_ip)
        
        if not mount_success:
            await state.set_error("SSHFS mount failed")
            return
        
        # 3. 상태 업데이트: running
        await state.set_running()
        
        # 4. 명령 실행
        logger.info(f"Executing command: {command}")
        result = await task_executor.execute(command, cwd="/mnt/frontend")
        
        # 5. 상태 업데이트: completed
        await state.set_completed(result)
        logger.info(f"Command completed: {result[:100]}...")
        
    except Exception as e:
        logger.error(f"Execute error: {e}")
        await state.set_error(str(e))


# === 시작 시 초기화 ===

@app.on_event("startup")
async def startup():
    """앱 시작 시 초기화"""
    logger.info("Backend Agent starting...")
    
    # SSH 키 설정
    mount_manager.setup_ssh_key()
    
    logger.info("Backend Agent ready")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
