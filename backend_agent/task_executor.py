"""
Task Executor

PTY + 파일 I/O 기반 명령어 실행 관리 클래스
"""

import asyncio
import logging
import os
import pty
import subprocess
import select
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class TaskExecutor:
    """
    명령어 실행 관리자 (PTY + 파일 I/O)
    
    - PTY를 통한 터미널 에뮬레이션
    - 파일 기반 stdin/stdout 통신
    """
    
    DEFAULT_TIMEOUT = 300  # 5분
    RUN_DIR = "/mnt/frontend/.run"
    
    def __init__(self, timeout: int = None):
        """초기화"""
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.process = None
        self.master_fd = None
    
    async def execute_interactive(self, command: str, cwd: str = None) -> str:
        """
        PTY + 파일 I/O로 대화형 명령 실행
        
        Args:
            command: 실행할 명령어
            cwd: 작업 디렉토리
            
        Returns:
            str: 최종 결과 요약
        """
        work_dir = cwd or "/mnt/frontend"
        run_dir = Path(self.RUN_DIR)
        
        # .run 디렉터리 확인
        if not run_dir.exists():
            logger.warning(f"Run directory not found: {run_dir}")
            return await self.execute(command, cwd)
        
        stdin_file = run_dir / "stdin"
        stdout_file = run_dir / "stdout"
        status_file = run_dir / "status"
        
        logger.info(f"Starting interactive execution: {command}")
        
        try:
            # PTY 생성
            master_fd, slave_fd = pty.openpty()
            self.master_fd = master_fd
            
            # 프로세스 시작
            self.process = subprocess.Popen(
                command,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                shell=True,
                cwd=work_dir,
                env={**os.environ, "TERM": "xterm-256color"}
            )
            os.close(slave_fd)
            
            # 상태 업데이트
            status_file.write_text("running")
            
            # stdin 파일 위치 추적
            stdin_pos = 0
            
            # I/O 루프
            loop = asyncio.get_event_loop()
            
            while self.process.poll() is None:
                # PTY에서 출력 읽기
                readable, _, _ = select.select([master_fd], [], [], 0.1)
                if readable:
                    try:
                        output = os.read(master_fd, 4096)
                        if output:
                            with open(stdout_file, "ab") as f:
                                f.write(output)
                    except OSError:
                        break
                
                # stdin 파일에서 입력 읽기
                if stdin_file.exists():
                    current_size = stdin_file.stat().st_size
                    if current_size > stdin_pos:
                        with open(stdin_file, "rb") as f:
                            f.seek(stdin_pos)
                            new_input = f.read()
                            if new_input:
                                os.write(master_fd, new_input)
                                stdin_pos = current_size
                
                await asyncio.sleep(0.05)
            
            # 프로세스 종료 후 남은 출력 읽기
            while True:
                readable, _, _ = select.select([master_fd], [], [], 0.1)
                if not readable:
                    break
                try:
                    output = os.read(master_fd, 4096)
                    if output:
                        with open(stdout_file, "ab") as f:
                            f.write(output)
                    else:
                        break
                except OSError:
                    break
            
            # 상태 업데이트
            return_code = self.process.returncode
            status_file.write_text("completed" if return_code == 0 else "error")
            
            logger.info(f"Interactive execution completed with return code: {return_code}")
            return f"Process exited with code {return_code}"
            
        except Exception as e:
            logger.error(f"Interactive execution error: {e}")
            if status_file.exists():
                status_file.write_text("error")
            return f"[error] {str(e)}"
        finally:
            if self.master_fd:
                try:
                    os.close(self.master_fd)
                except OSError:
                    pass
    
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
