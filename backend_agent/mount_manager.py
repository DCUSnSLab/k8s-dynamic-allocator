"""
Mount Manager

SSHFS 마운트/언마운트 관리 클래스
Frontend의 시스템 디렉토리까지 마운트하여 환경 미러링
"""

import asyncio
import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class MountManager:
    """
    SSHFS 마운트 관리자

    - Frontend Pod에 SSHFS 마운트
    - 시스템 디렉토리 추가 마운트 (/usr, /lib, /var)
    - 마운트 해제
    - SSH 키 설정
    """

    # 마운트 설정: (remote_path, local_mount_point)
    MOUNTS = [
        ("/home/dcuuser",  "/mnt/frontend"),   # 기존: 사용자 홈 디렉토리
        ("/usr",           "/mnt/f/usr"),       # 추가: pip, apt 패키지, 바이너리
        ("/lib",           "/mnt/f/lib"),       # 추가: 공유 라이브러리
        ("/var",           "/mnt/f/var"),       # 추가: apt cache, dpkg 상태
    ]

    SSH_KEY_SOURCE = "/etc/ssh-key/id_rsa"
    SSH_KEY_DEST = "/root/.ssh/id_rsa"
    SSH_USER = "dcuuser"

    def __init__(self):
        """초기화"""
        self.mounted = False
        self.frontend_ip: str = None

    def setup_ssh_key(self):
        """
        SSH 키 설정

        /etc/ssh-key/id_rsa → /root/.ssh/id_rsa 복사
        """
        try:
            ssh_dir = Path("/root/.ssh")
            ssh_dir.mkdir(parents=True, exist_ok=True)

            if os.path.exists(self.SSH_KEY_SOURCE):
                shutil.copy2(self.SSH_KEY_SOURCE, self.SSH_KEY_DEST)
                os.chmod(self.SSH_KEY_DEST, 0o600)
                logger.info("SSH key configured successfully")
            else:
                logger.warning(f"SSH key source not found: {self.SSH_KEY_SOURCE}")
        except Exception as e:
            logger.error(f"Failed to setup SSH key: {e}")

    async def _sshfs_mount(self, frontend_ip: str, remote_path: str, local_path: str) -> bool:
        """
        단일 SSHFS 마운트 실행

        Args:
            frontend_ip: Frontend Pod IP
            remote_path: Frontend의 원격 경로
            local_path: Backend의 로컬 마운트 포인트

        Returns:
            bool: 마운트 성공 여부
        """
        try:
            # 마운트 포인트 생성
            Path(local_path).mkdir(parents=True, exist_ok=True)

            cmd = [
                "sshfs",
                "-o", f"IdentityFile={self.SSH_KEY_DEST}",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "reconnect",
                "-o", "ServerAliveInterval=15",
                "-o", "allow_other",
                f"{self.SSH_USER}@{frontend_ip}:{remote_path}",
                local_path
            ]

            logger.info(f"Mounting: {self.SSH_USER}@{frontend_ip}:{remote_path} -> {local_path}")

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=30.0
            )

            if process.returncode == 0:
                logger.info(f"Successfully mounted {remote_path} to {local_path}")
                return True
            else:
                logger.error(f"Mount failed ({remote_path}): {stderr.decode()}")
                return False

        except asyncio.TimeoutError:
            logger.error(f"Mount timeout ({remote_path})")
            return False
        except Exception as e:
            logger.error(f"Mount error ({remote_path}): {e}")
            return False

    async def mount(self, frontend_ip: str) -> bool:
        """
        Frontend에 SSHFS 마운트 (홈 + 시스템 디렉토리)

        Args:
            frontend_ip: Frontend Pod IP

        Returns:
            bool: 마운트 성공 여부
        """
        if self.mounted:
            logger.warning("Already mounted, unmounting first...")
            await self.unmount()

        # 홈 디렉토리 마운트 (필수)
        home_remote, home_local = self.MOUNTS[0]
        if not await self._sshfs_mount(frontend_ip, home_remote, home_local):
            return False

        # 시스템 디렉토리 마운트 (선택 — 실패해도 계속 진행)
        for remote_path, local_path in self.MOUNTS[1:]:
            success = await self._sshfs_mount(frontend_ip, remote_path, local_path)
            if not success:
                logger.warning(f"Optional mount failed: {remote_path} (continuing)")

        self.mounted = True
        self.frontend_ip = frontend_ip
        logger.info(f"All mounts completed for {frontend_ip}")
        return True

    async def _fusermount(self, mount_point: str, force: bool = False) -> bool:
        """단일 마운트 포인트 해제"""
        try:
            flag = "-uz" if force else "-u"
            cmd = ["fusermount", flag, mount_point]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            await asyncio.wait_for(process.communicate(), timeout=10.0)
            return process.returncode == 0
        except Exception as e:
            logger.warning(f"Unmount failed ({mount_point}): {e}")
            return False

    async def unmount(self) -> bool:
        """
        모든 SSHFS 마운트 해제

        Returns:
            bool: 해제 성공 여부
        """
        if not self.mounted:
            logger.info("Not mounted, nothing to unmount")
            return True

        all_success = True

        # 역순으로 해제 (시스템 디렉토리 먼저, 홈 마지막)
        for remote_path, local_path in reversed(self.MOUNTS):
            # 마운트되어 있는지 확인
            if not os.path.ismount(local_path):
                logger.info(f"Skipping unmount for {local_path} (not mounted)")
                continue

            if not await self._fusermount(local_path):
                # 강제 해제 시도
                if not await self._fusermount(local_path, force=True):
                    # 홈 디렉토리(필수) 해제 실패 시에만 실패로 간주
                    if local_path == self.MOUNTS[0][1]:
                        all_success = False
                        logger.error(f"Failed to unmount critical path {local_path}")
                    else:
                        logger.warning(f"Failed to unmount optional path {local_path}")
                else:
                    logger.info(f"Force unmounted {local_path}")
            else:
                logger.info(f"Unmounted {local_path}")

        self.mounted = False
        self.frontend_ip = None
        return all_success

    def is_mounted(self) -> bool:
        """마운트 상태 확인"""
        return self.mounted
