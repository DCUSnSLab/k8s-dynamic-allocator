"""
Task Executor

명령어 실행 관리 클래스
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TaskExecutor:
    """
    명령어 실행 관리자
    
    - 쉘 명령어 비동기 실행
    - 실행 결과 캡처
    
    향후 확장:
    - 작업 유형별 실행 전략
    - 리소스 제한
    - 실행 시간 제한
    """
    
    DEFAULT_TIMEOUT = 300  # 5분
    
    def __init__(self, timeout: int = None):
        """
        초기화
        
        Args:
            timeout: 실행 타임아웃 (초)
        """
        self.timeout = timeout or self.DEFAULT_TIMEOUT
    
    async def execute(self, command: str, cwd: str = None) -> str:
        """
        명령어 실행
        
        Args:
            command: 실행할 명령어
            cwd: 작업 디렉토리 (기본: /mnt/frontend)
            
        Returns:
            str: 실행 결과 (stdout + stderr)
        """
        work_dir = cwd or "/mnt/frontend"
        
        logger.info(f"Executing command: {command} in {work_dir}")
        
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=work_dir
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout
            )
            
            # stdout과 stderr 결합
            output = ""
            if stdout:
                output += stdout.decode('utf-8', errors='replace')
            if stderr:
                output += f"\n[stderr]\n{stderr.decode('utf-8', errors='replace')}"
            
            logger.info(f"Command completed with return code: {process.returncode}")
            
            return output.strip() or "(no output)"
            
        except asyncio.TimeoutError:
            logger.error(f"Command timeout after {self.timeout}s: {command}")
            return f"[error] Command timeout after {self.timeout} seconds"
        except Exception as e:
            logger.error(f"Command execution error: {e}")
            return f"[error] {str(e)}"
    
    async def execute_with_status(self, command: str, cwd: str = None) -> dict:
        """
        명령어 실행 (상세 결과)
        
        Args:
            command: 실행할 명령어
            cwd: 작업 디렉토리
            
        Returns:
            dict: 실행 결과 상세
        """
        work_dir = cwd or "/mnt/frontend"
        
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=work_dir
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=self.timeout
            )
            
            return {
                "status": "success" if process.returncode == 0 else "failed",
                "return_code": process.returncode,
                "stdout": stdout.decode('utf-8', errors='replace') if stdout else "",
                "stderr": stderr.decode('utf-8', errors='replace') if stderr else ""
            }
            
        except asyncio.TimeoutError:
            return {
                "status": "timeout",
                "return_code": -1,
                "stdout": "",
                "stderr": f"Command timeout after {self.timeout} seconds"
            }
        except Exception as e:
            return {
                "status": "error",
                "return_code": -1,
                "stdout": "",
                "stderr": str(e)
            }
