"""
TCP Terminal Server

Frontend의 터미널과 직접 연결하여 PTY 입출력을 처리하는 TCP 서버
"""

import asyncio
import glob
import logging
import os
import pty
import subprocess
import select
import termios
import struct
import fcntl
import json
from datetime import datetime
from typing import Optional

from mount_manager import MountManager

logger = logging.getLogger(__name__)


class TCPTerminalServer:
    """
    TCP 기반 터미널 서버

    - Frontend와 TCP로 직접 연결
    - PTY를 통한 터미널 에뮬레이션
    - 실시간 입출력 전달, 터미널 설정 동기화
    - 동시 접속 지원 (세션별 로컬 변수 관리)
    """

    def __init__(self, port: int = 8081):
        self.port = port
        self.server: Optional[asyncio.Server] = None
        self._active_sessions = {}  # {session_id: {process, addr, started_at, command}}

    async def start(self):
        """TCP 서버 시작"""
        self.server = await asyncio.start_server(
            self.handle_connection,
            host='0.0.0.0',
            port=self.port
        )
        logger.info(f"TCP Terminal Server started on port {self.port}")

    async def stop(self):
        """TCP 서버 중지 — 모든 활성 세션 정리"""
        for sid, info in list(self._active_sessions.items()):
            process = info.get("process")
            if process and process.poll() is None:
                process.terminate()
                logger.info(f"Terminated process for session {sid}")
        self._active_sessions.clear()

        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("TCP Terminal Server stopped")

    def get_sessions(self):
        """활성 세션 목록 반환 (모니터링용)"""
        return [
            {
                "id": sid,
                "addr": str(info["addr"]),
                "command": info.get("command", ""),
                "started_at": info["started_at"].isoformat(),
            }
            for sid, info in self._active_sessions.items()
        ]

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        클라이언트 연결 처리

        프로토콜:
        1. 첫 줄: JSON Header ({command, rows, cols, term, settings})
        2. 이후: 터미널 입출력

        모든 세션 상태는 로컬 변수로 관리 (dcusshk8s 패턴)
        """
        addr = writer.get_extra_info('peername')
        session_id = id(writer)
        logger.info(f"TCP connection from {addr} (session {session_id})")

        # 세션별 로컬 변수
        process = None
        master_fd = None
        verase_byte = b'\x7f'

        try:
            # 1. 헤더 수신 (JSON)
            line = await reader.readline()
            if not line:
                logger.warning("No data received, closing connection")
                return

            try:
                # JSON 파싱 시도 (새 프로토콜)
                header = json.loads(line.decode('utf-8').strip())
                command = header.get('command')
                rows = header.get('rows', 24)
                cols = header.get('cols', 80)
                term_env = header.get('term', 'xterm-256color')
                settings = header.get('settings', {})
            except json.JSONDecodeError:
                # 하위 호환성 (구 프로토콜: 단순 명령어)
                command = line.decode('utf-8').strip()
                rows, cols = 24, 80
                term_env = 'xterm-256color'
                settings = {}
                logger.warning("Received legacy command format")

            logger.info(f"Executing command: {command} (Size: {rows}x{cols}, Term: {term_env})")

            # 2. PTY 생성 및 프로세스 시작
            master_fd, slave_fd = pty.openpty()

            # 윈도우 크기 설정
            try:
                fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
            except Exception as e:
                logger.warning(f"Failed to set window size: {e}")

            # PTY 설정: canonical mode, echo ON, backspace 지원
            try:
                attrs = termios.tcgetattr(slave_fd)
                attrs[3] = attrs[3] | termios.ICANON | termios.ECHO | termios.ECHOE | termios.ECHOK | termios.ISIG
                # 입력 처리
                attrs[0] = attrs[0] | termios.ICRNL
                # 출력 처리
                attrs[1] = attrs[1] | termios.OPOST | termios.ONLCR

                # IUTF8: UTF-8 환경에서 멀티바이트 문자 삭제 지원
                if hasattr(termios, 'IUTF8'):
                    attrs[0] = attrs[0] | termios.IUTF8

                # 특수 문자 설정 negotiation
                cc = attrs[6]

                # 클라이언트에서 VERASE 보냈으면 적용, 아니면 DEL(\x7f) 기본값
                verase = settings.get('verase')
                if verase is not None:
                    cc[termios.VERASE] = bytes([verase])
                    verase_byte = bytes([verase])
                    logger.info(f"Applied client VERASE: {verase} (0x{verase:02x})")
                else:
                    cc[termios.VERASE] = b'\x7f'
                    verase_byte = b'\x7f'
                    logger.info("Applied default VERASE: DEL (0x7f)")

                cc[termios.VINTR] = b'\x03'
                cc[termios.VEOF] = b'\x04'

                termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
                logger.info(f"PTY configured: ICANON|ECHO|ECHOE|ISIG, VERASE=0x{ord(verase_byte):02x}")
            except Exception as e:
                logger.warning(f"Failed to configure PTY: {e}")

            # 프로세스 환경변수 설정
            # Chroot 환경이므로 Frontend 미러링 불필요 (그대로 사용)
            # PATH도 Frontend의 것이 그대로 사용됨
            env = {**os.environ, "TERM": term_env}
            
            # Client에서 받은 PATH가 있다면 적용? 
            # Chroot에서는 Frontend의 /bin, /usr/bin이 그대로 보이므로
            # Client PATH가 Frontend 기준이라면 그대로 적용하면 됨.
            client_path = header.get('env', {}).get('PATH', '')
            if client_path:
                 env["PATH"] = client_path

            # preexec_fn 정의 (Chroot & Namespace 설정)
            def preexec():
                # 1. New Session (setsid)
                os.setsid()
                
                # 2. Controlling Terminal 설정
                try:
                    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                except Exception:
                    pass

                # 3. Mount Namespace & Chroot (Mount Manager 위임)
                # 이 함수는 내부적으로 unshare -> mount -> chroot -> chdir 수행
                MountManager.setup_chroot_namespace(addr[0])

            process = subprocess.Popen(
                command,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                shell=True,
                preexec_fn=preexec,
                env=env
            )
            os.close(slave_fd)

            # 세션 추적 등록
            self._active_sessions[session_id] = {
                "process": process,
                "addr": addr,
                "command": command,
                "started_at": datetime.now(),
            }

            # 3. 비동기 I/O 처리
            await self._handle_io(reader, writer, master_fd, process, verase_byte)

        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            # 정리: master_fd 닫기
            if master_fd is not None:
                try:
                    os.close(master_fd)
                except OSError:
                    pass

            # 정리: 프로세스 강제 종료 (dcusshk8s 패턴)
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    logger.warning(f"Force killed process for session {session_id}")

            # 세션 추적 제거
            self._active_sessions.pop(session_id, None)

            writer.close()
            await writer.wait_closed()
            logger.info(f"Connection closed: {addr} (session {session_id})")

    async def _handle_io(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        master_fd: int,
        process: subprocess.Popen,
        verase_byte: bytes,
    ):
        """PTY와 TCP 간 I/O 처리 — 세션별 파라미터로 격리"""

        # PTY를 non-blocking으로 설정
        flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

        process_done = asyncio.Event()

        async def read_pty():
            """PTY에서 읽어서 TCP로 전송"""
            while process and process.poll() is None:
                try:
                    readable, _, _ = select.select([master_fd], [], [], 0.01)
                    if readable:
                        data = os.read(master_fd, 4096)
                        if data:
                            writer.write(data)
                            await writer.drain()
                except (OSError, BlockingIOError):
                    pass
                await asyncio.sleep(0.01)

            # 프로세스 종료 후 남은 출력 읽기
            for _ in range(10):
                try:
                    readable, _, _ = select.select([master_fd], [], [], 0.05)
                    if readable:
                        data = os.read(master_fd, 4096)
                        if data:
                            writer.write(data)
                            await writer.drain()
                        else:
                            break
                    else:
                        break
                except (OSError, BlockingIOError):
                    break

            # 프로세스 종료 신호
            process_done.set()

        async def read_tcp():
            """TCP에서 읽어서 PTY로 전송 (Raw 바이트)"""
            while not process_done.is_set():
                try:
                    data = await asyncio.wait_for(reader.read(1024), timeout=0.1)
                    if data:
                        # BS(\x08)와 DEL(\x7f) 모두 PTY의 VERASE 값으로 통일
                        data = data.replace(b'\x08', verase_byte)
                        if verase_byte != b'\x7f':
                            data = data.replace(b'\x7f', verase_byte)
                        os.write(master_fd, data)
                    elif reader.at_eof():
                        break
                except asyncio.TimeoutError:
                    pass
                except (OSError, BrokenPipeError):
                    break

        # 두 태스크 동시 실행
        await asyncio.gather(
            read_pty(),
            read_tcp(),
            return_exceptions=True
        )


# 전역 인스턴스
tcp_terminal = TCPTerminalServer()
