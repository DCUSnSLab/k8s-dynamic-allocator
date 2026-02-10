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
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from mount_manager import MountManager
from tcp_terminal import tcp_terminal

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


class FormattedJSONResponse(JSONResponse):
    """들여쓰기된 JSON 응답 (끝에 줄바꿈 포함)"""
    def render(self, content) -> bytes:
        import json
        return (json.dumps(
            content,
            indent=2,
            ensure_ascii=False
        ) + "\n").encode("utf-8")


# Request/Response 모델

class MountRequest(BaseModel):
    """마운트 요청"""
    frontend_ip: str
    command: str


class MountResponse(BaseModel):
    """마운트 응답"""
    status: str
    message: Optional[str] = None


class UnmountResponse(BaseModel):
    """마운트 해제 응답"""
    status: str
    message: Optional[str] = None


# 상태 관리

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


@app.get("/result")
async def get_result():
    """
    실행 결과만 plain text로 반환
    
    터미널에서 직접 실행한 것처럼 결과 확인 가능
    """
    from fastapi.responses import PlainTextResponse
    
    if state.result is None:
        return PlainTextResponse(
            content="(결과 없음)\n",
            status_code=200
        )
    
    # 결과 끝에 줄바꿈 추가
    result_text = state.result
    if not result_text.endswith("\n"):
        result_text += "\n"
    
    return PlainTextResponse(content=result_text, status_code=200)


@app.post("/mount")
async def mount(request: MountRequest):
    """
    Frontend에 SSHFS 마운트 (명령 실행은 TCP 서버에서 처리)
    """
    if state.status not in ["idle", "completed", "error"]:
        raise HTTPException(
            status_code=409,
            detail=f"Agent is busy (status: {state.status})"
        )
    
    logger.info("=" * 50)
    logger.info("[Agent] 마운트 요청 수신")
    logger.info(f"  Frontend IP: {request.frontend_ip}")
    logger.info(f"  Command: {request.command}")
    logger.info("=" * 50)
    
    # 상태 업데이트
    await state.set_mounting(request.frontend_ip, request.command)
    logger.info("[Agent] Step 1: 마운트 시작...")
    
    # SSHFS 마운트 (동기 실행)
    mount_success = await mount_manager.mount(request.frontend_ip)
    
    if not mount_success:
        logger.error("[Agent] 마운트 실패!")
        await state.set_error("SSHFS mount failed")
        return FormattedJSONResponse({
            "status": "error",
            "message": "SSHFS mount failed"
        }, status_code=500)
    
    logger.info("[Agent] Step 2: 마운트 성공")
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
    logger.info("[Agent] 마운트 해제 요청 수신")
    
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
    logger.info("=" * 50)
    logger.info("[Agent] Backend Agent 시작")
    
    mount_manager.setup_ssh_key()
    
    # TCP 터미널 서버 시작
    await tcp_terminal.start()
    
    logger.info("[Agent] 준비 완료")
    logger.info("=" * 50)


@app.on_event("shutdown")
async def shutdown():
    """앱 종료 시 정리"""
    await tcp_terminal.stop()
    logger.info("[Agent] Backend Agent 종료")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
