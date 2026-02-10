"""
Task Executor

비대화형 명령어 실행 관리 클래스
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TaskExecutor:
    """
    비대화형 명령어 실행 관리자
    """
    
    DEFAULT_TIMEOUT = 300  # 5분
    
    def __init__(self, timeout: int = None):
        """초기화"""
        self.timeout = timeout or self.DEFAULT_TIMEOUT
    
    async def execute(self, command: str, cwd: str = None) -> str:
        """
        단순 명령어 실행 (비대화형)
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
