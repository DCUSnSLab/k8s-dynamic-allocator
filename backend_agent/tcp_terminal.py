"""
TCP Terminal Server

- Controller 중계 없이 Frontend(WebTTY) 클라이언트 간 다이렉트 TCP 소켓 연결 서버
- PTY 모듈을 이용한 양방향 Raw 바이너리 TTY/Shell 에뮬레이션 전담
"""

import asyncio
import logging
import os
import pty
import socket
import subprocess
import select
import termios
import struct
import fcntl
import json
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

from mount_manager import MountManager

logger = logging.getLogger(__name__)

TCP_KEEPIDLE = int(os.getenv("TCP_KEEPIDLE", "10"))
TCP_KEEPINTVL = int(os.getenv("TCP_KEEPINTVL", "5"))
TCP_KEEPCNT = int(os.getenv("TCP_KEEPCNT", "3"))


class TCPTerminalServer:
    """
    TCP 기반 터미널 서버

    - Frontend와 TCP로 직접 연결
    - PTY를 통한 터미널 에뮬레이션
    - 실시간 입출력 전달, 터미널 설정 동기화
    - 동시 접속 지원 (세션별 로컬 변수 관리)
    """

    def __init__(self, port: Optional[int] = None) -> None:
        self.port = port or int(os.getenv("BACKEND_AGENT_TCP_PORT", "8081"))
        self.server: Optional[asyncio.Server] = None
        self._active_sessions: Dict[int, Dict] = {}
        self._skip_release_notify = False
        self._release_notify_debounce_seconds = float(
            os.getenv("BACKEND_AGENT_RELEASE_NOTIFY_DEBOUNCE_SECONDS", "10")
        )
        self._release_notify_lock = asyncio.Lock()
        self._release_notify_pending = False
        self._release_notify_sent = False
        self._release_notify_task: Optional[asyncio.Task] = None
        self._release_notify_generation = 0
        self._release_notify_cancelled = threading.Event()
        self._release_notify_http_connection_lock = threading.Lock()
        self._release_notify_http_connection = None
        self._frontend_ip = ""
        self._frontend_pod = ""
        self._mount_context_lock = threading.Lock()

    async def start(self) -> None:
        """TCP 서버 시작"""
        self.server = await asyncio.start_server(
            self.handle_connection,
            host='0.0.0.0',
            port=self.port
        )
        # 서버 소켓에 TCP Keepalive 적용
        for sock in self.server.sockets:
            self._configure_keepalive(sock)
        logger.info("TCP Terminal Server started on port %s", self.port)

    async def stop(self) -> None:
        """TCP 서버 중지 — 모든 활성 세션 정리"""
        for sid, info in list(self._active_sessions.items()):
            process = info.get("process")
            if process and process.poll() is None:
                process.terminate()
                logger.info("Terminated process for session %s", sid)
        self._active_sessions.clear()

        if self.server:
            self.server.close()
            await self.server.wait_closed()
            logger.info("TCP Terminal Server stopped")

    async def terminate_all_sessions(self) -> None:
        """외부 요청에 의한 모든 활성 세션/프로세스 강제 종료 (fallback 상태는 변경하지 않음)"""
        count = 0
        for sid, info in list(self._active_sessions.items()):
            process = info.get("process")
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                count += 1
        if count:
            logger.info("Terminated %d active session(s) by external request", count)

    async def suppress_fallback_release(self) -> None:
        """primary cleanup 성공 후 호출 — fallback release를 확정적으로 억제한다."""
        self._skip_release_notify = True
        self._release_notify_cancelled.set()
        self._clear_mount_context()

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._close_release_notify_http_connection_sync)

        async with self._release_notify_lock:
            self._release_notify_generation += 1
            self._release_notify_pending = False
            self._release_notify_sent = True
            if self._release_notify_task and not self._release_notify_task.done():
                self._release_notify_task.cancel()

    def _close_release_notify_http_connection_sync(self) -> None:
        with self._release_notify_http_connection_lock:
            release_notify_http_connection = self._release_notify_http_connection
            self._release_notify_http_connection = None
        if release_notify_http_connection:
            try:
                release_notify_http_connection.close()
            except Exception:
                pass

    async def begin_session_lifecycle(self, frontend_ip: str = "", frontend_pod: str = "") -> None:
        with self._mount_context_lock:
            self._frontend_ip = frontend_ip or ""
            self._frontend_pod = frontend_pod or ""

        self._release_notify_cancelled.clear()
        async with self._release_notify_lock:
            self._skip_release_notify = False
            self._release_notify_generation += 1
            self._release_notify_pending = False
            self._release_notify_sent = False
            if self._release_notify_task and not self._release_notify_task.done():
                self._release_notify_task.cancel()
            self._release_notify_task = None

    def _mount_context_snapshot(self) -> Dict[str, str]:
        with self._mount_context_lock:
            return {
                "frontend_ip": self._frontend_ip,
                "frontend_pod": self._frontend_pod,
            }

    def _clear_mount_context(self) -> None:
        with self._mount_context_lock:
            self._frontend_ip = ""
            self._frontend_pod = ""

    @staticmethod
    def _configure_keepalive(sock: socket.socket) -> None:
        """TCP Keepalive 설정 — 비정상 연결 끊김 감지용"""
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, TCP_KEEPIDLE)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, TCP_KEEPINTVL)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, TCP_KEEPCNT)

    def get_sessions(self) -> List[Dict]:
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
        [신규 터미널 소켓 세션 수립 프로세스]

        - 통신 프로토콜:
          1. Connection 최초 수신: JSON 속성 메타데이터 Header ({command, cwd, rows, cols, term, settings})
          2. 이후 스트림: 터미널 양방향 Raw 바이너리 입출력

        - 세션 독립성: Window Size 제어(io_ctl), 환경변수, pty fd 등 모든 상태는 독립 변수 격리로 처리
        """
        addr = writer.get_extra_info('peername')
        session_id = id(writer)
        client_ip = addr[0]
        client_port = addr[1]
        connection_started = time.perf_counter()
        ready_at = None
        mount_context = self._mount_context_snapshot()
        mount_frontend_ip = mount_context.get("frontend_ip") or client_ip

        logger.info("[ConnectionOpened] client_ip=%s", client_ip)

        # 클라이언트 소켓에 TCP Keepalive 적용
        client_sock = writer.get_extra_info('socket')
        if client_sock:
            self._configure_keepalive(client_sock)

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
                cwd = header.get('cwd', '/home/dcuuser')
                rows = header.get('rows', 24)
                cols = header.get('cols', 80)
                term_env = header.get('term', 'xterm-256color')
                settings = header.get('settings', {})
            except json.JSONDecodeError:
                # 하위 호환성 (구 프로토콜: 단순 명령어)
                command = line.decode('utf-8').strip()
                cwd = '/home/dcuuser'
                rows, cols = 24, 80
                term_env = 'xterm-256color'
                settings = {}
                logger.warning("Received legacy command format")

            # 2. PTY 생성 및 프로세스 시작
            master_fd, slave_fd = pty.openpty()

            # 윈도우 크기 설정
            try:
                fcntl.ioctl(master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
            except Exception as e:
                logger.warning("Failed to set window size: %s", e)

            # PTY 설정
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
                else:
                    cc[termios.VERASE] = b'\x7f'
                    verase_byte = b'\x7f'

                cc[termios.VINTR] = b'\x03'
                cc[termios.VEOF] = b'\x04'

                termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
            except Exception as e:
                logger.warning("Failed to configure PTY: %s", e)

            if command.endswith("bash") or command.endswith("sh"):
                final_command = [command, "-l"]
            else:
                final_command = ["/bin/bash", "-l", "-c", command]

            env = {**os.environ, "TERM": term_env, "HOME": "/home/dcuuser", "USER": "dcuuser", "RUN_SESSION": "1"}

            def preexec():
                # 1. New Session (setsid)
                os.setsid()
                
                # 2. Controlling Terminal 설정
                try:
                    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
                except Exception:
                    pass

                # 3. Mount Namespace & Chroot 환경 구성
                # unshare -> mount -> chroot -> chdir 수행
                MountManager.setup_chroot_namespace(mount_frontend_ip, cwd=cwd)

            mount_setup_started = time.perf_counter()
            process = subprocess.Popen(
                final_command,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                shell=False,
                preexec_fn=preexec,
                env=env
            )
            ready_at = time.perf_counter()
            mount_setup_ms = int((ready_at - mount_setup_started) * 1000)
            connect_to_ready_ms = int((ready_at - connection_started) * 1000)
            os.close(slave_fd)

            # 세션 추적 등록
            self._active_sessions[session_id] = {
                "process": process,
                "addr": addr,
                "command": command,
                "started_at": datetime.now(),
                "ready_at": ready_at,
            }
            logger.info(
                "[SessionReady] connect_to_ready_ms=%s mount_setup_ms=%s",
                connect_to_ready_ms,
                mount_setup_ms,
            )

            # 3. 비동기 I/O 처리
            await self._handle_io(reader, writer, master_fd, process, verase_byte)

        except Exception as e:
            logger.error("Connection error: %s", e)
        finally:
            # 정리: master_fd 닫기
            if master_fd is not None:
                try:
                    os.close(master_fd)
                except OSError:
                    pass

            # 프로세스 강제 종료
            if process and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    logger.warning("Force killed process for session %s", session_id)

            # 세션 추적 제거
            session_info = self._active_sessions.pop(session_id, None)

            writer.close()
            await writer.wait_closed()
            session_ready_at = session_info.get("ready_at") if session_info else None
            session_started_at = session_ready_at or ready_at or connection_started
            session_ms = int((time.perf_counter() - session_started_at) * 1000)
            logger.info("[ConnectionClosed] session_ms=%s", max(0, session_ms))

            # 활성 세션이 없으면 Controller에 자원 해제 요청
            await self._maybe_notify_release()

    async def _maybe_notify_release(self):
        """Schedule one fallback release notify per backend lifecycle."""
        # Fast path: avoid lock if obviously not needed (optimization)
        if self._skip_release_notify or self._active_sessions:
            return

        # Double-check with lock to prevent race conditions
        async with self._release_notify_lock:
            if self._skip_release_notify or self._active_sessions:
                return
            # Ensure we only schedule one notification per lifecycle
            if self._release_notify_pending or self._release_notify_sent:
                return
            self._release_notify_pending = True
            loop = asyncio.get_running_loop()
            generation = self._release_notify_generation
            self._release_notify_task = loop.create_task(self._notify_release(generation))

    async def _notify_release(self, generation: int):
        """
        Controller에 Backend 해제 요청 (TCP 끊김 시 안전망).

        debounce → generation 검증 → cancelled_event 검사 → HTTP 전송 순서로 진행.
        primary cleanup이 먼저 끝나면 cancelled_event.set()와 HTTP 연결 종료로
        fallback 요청을 best-effort로 중단하며, 중복 전송 가능성은 크게 줄이되
        blocking HTTP 자체의 마지막 미세 race window까지 완전히 없애지는 않는다.
        """
        hostname = os.environ.get("HOSTNAME", "")
        if not hostname:
            return

        notified = False
        cancelled_event = self._release_notify_cancelled

        http_connection_lock = self._release_notify_http_connection_lock

        def make_request():
            if cancelled_event.is_set():
                return False
            import http.client
            body = json.dumps({
                "backend_pod": hostname,
                "source": "backend_fallback",
            }).encode("utf-8")
            release_notify_http_connection = None
            try:
                release_notify_http_connection = http.client.HTTPConnection("controller-service", 9001, timeout=10)
                with http_connection_lock:
                    if cancelled_event.is_set():
                        return False
                    self._release_notify_http_connection = release_notify_http_connection
                release_notify_http_connection.connect()
                if cancelled_event.is_set():
                    return False
                release_notify_http_connection.request("POST", "/api/pool/release/", body=body, headers={
                    "Content-Type": "application/json",
                    "Content-Length": str(len(body)),
                })
                if cancelled_event.is_set():
                    return False
                response = release_notify_http_connection.getresponse()
                response.read()
                logger.debug("Fallback release notified to Controller (%s)", response.status)
                return True
            except Exception as e:
                if not cancelled_event.is_set():
                    logger.warning("Fallback release notification failed: %s", e)
                return False
            finally:
                with http_connection_lock:
                    if self._release_notify_http_connection is release_notify_http_connection:
                        self._release_notify_http_connection = None
                if release_notify_http_connection:
                    release_notify_http_connection.close()

        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(self._release_notify_debounce_seconds)
            async with self._release_notify_lock:
                if generation != self._release_notify_generation:
                    return
                if self._skip_release_notify or self._active_sessions:
                    return
            notified = bool(await loop.run_in_executor(None, make_request))
        except asyncio.CancelledError:
            raise
        finally:
            async with self._release_notify_lock:
                if generation == self._release_notify_generation:
                    self._release_notify_pending = False
                    if notified:
                        self._release_notify_sent = True
                    if self._release_notify_task is asyncio.current_task():
                        self._release_notify_task = None

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
                except (ConnectionResetError, BrokenPipeError):
                    break
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
                        if process and process.poll() is None:
                            process.terminate()
                        process_done.set()
                        break
                except asyncio.TimeoutError:
                    pass
                except (OSError, BrokenPipeError):
                    if process and process.poll() is None:
                        process.terminate()
                    process_done.set()
                    break

        # 두 태스크 동시 실행
        await asyncio.gather(
            read_pty(),
            read_tcp(),
            return_exceptions=True
        )


# 전역 인스턴스
tcp_terminal = TCPTerminalServer()
