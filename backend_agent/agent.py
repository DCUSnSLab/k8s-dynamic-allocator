"""
Backend Agent - HTTP API Server

Backend Pool Pod 내에서 실행되는 경량 HTTP 서버
- SSHFS 마운트 관리
- 백엔드 상태 단일 조회
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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
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


class FormattedJSONResponse(JSONResponse):
    """들여쓰기된 JSON 응답 (끝에 줄바꿈 포함)"""
    def render(self, content) -> bytes:
        import json
        return (json.dumps(
            content,
            indent=2,
            ensure_ascii=False
        ) + "\n").encode("utf-8")


# Request 모델

class MountRequest(BaseModel):
    """마운트 요청"""
    frontend_ip: str
    command: str
    frontend_pod: Optional[str] = None


# 상태 관리

class AgentState:
    """Agent 작동 상태 메모리 관리"""
    def __init__(self):
        self.status = "idle"
        self.frontend_ip: Optional[str] = None
        self.frontend_pod: Optional[str] = None
        self.command: Optional[str] = None
        self.started_at: Optional[datetime] = None
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
    
    async def reset(self):
        async with self._lock:
            self.status = "idle"
            self.frontend_ip = None
            self.command = None
            self.started_at = None
    
    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "frontend_ip": self.frontend_ip,
            "frontend_pod": self.frontend_pod,
            "command": self.command,
            "started_at": self.started_at.isoformat() if self.started_at else None
        }


state = AgentState()


# API 엔드포인트

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return FormattedJSONResponse({
        "service": "Backend Agent",
        "status": "running"
    })


@app.get("/status")
async def get_status():
    """
    Agent 상태 조회
    
    결과에 개행 문자가 포함된 경우 읽기 쉽게 표시
    """
    data = state.to_dict()
    data["sessions"] = tcp_terminal.get_sessions()
    return FormattedJSONResponse(data)


@app.post("/mount")
async def mount(request: MountRequest):
    """
    Frontend 대상 SSHFS 마운트 요청 처리
    (실제 명령어 PTY 실행은 별도의 TCP 서버 포트에서 당담)
    """
    if state.status not in ["idle", "completed", "error"]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is busy (status: {state.status})"
        )
    
    # 상태 업데이트
    await state.set_mounting(request.frontend_ip, request.frontend_pod, request.command)
    
    logger.info(f"Mount requested: frontend={request.frontend_pod}({request.frontend_ip}), command={request.command}")
    
    # SSHFS 마운트 (동기 실행)
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
    
    return FormattedJSONResponse({
        "status": "success",
        "message": "Mount completed, ready for TCP connection",
        "frontend_ip": request.frontend_ip,
        "tcp_port": 8081
    })


@app.post("/unmount")
async def unmount():
    """
    SSHFS 마운트 해제 및 상태 초기화
    """
    logger.info("Unmount requested")
    
    try:
        success = await mount_manager.unmount()
        
        if success:
            await state.reset()
            return FormattedJSONResponse({
                "status": "success",
                "message": "Unmounted and reset"
            })
        else:
            return FormattedJSONResponse({
                "status": "warning",
                "message": "Unmount completed with warnings"
            })
    except Exception as e:
        logger.error(f"Unmount error: {e}")
        return FormattedJSONResponse({
            "status": "error",
            "message": str(e)
        })


# 시작 시 초기화

@app.on_event("startup")
async def startup():
    """앱 시작 시 초기화"""
    logger.info("Backend Agent starting...")
    
    mount_manager.setup_ssh_key()
    
    # TCP 터미널 서버 시작
    await tcp_terminal.start()
    
    logger.info("Backend Agent ready")
    
    # Uvicorn 시작 프롬프트 출력 후 줄넘김
    async def print_newline():
        import sys
        await asyncio.sleep(0.5)
        sys.stdout.write("\n")
        sys.stdout.flush()
    asyncio.create_task(print_newline())


@app.on_event("shutdown")
async def shutdown():
    """앱 종료 시 정리"""
    await tcp_terminal.stop()
    logger.info("Backend Agent stopped")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080, access_log=False)
