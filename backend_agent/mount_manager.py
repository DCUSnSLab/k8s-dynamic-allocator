"""
Mount Manager

SSHFS 마운트/언마운트 관리 클래스
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
    - 마운트 해제
    - SSH 키 설정
    """
    
    MOUNT_POINT = "/mnt/frontend"
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
    
    async def mount(self, frontend_ip: str) -> bool:
        """
        Frontend에 SSHFS 마운트
        
        Args:
            frontend_ip: Frontend Pod IP
            
        Returns:
            bool: 마운트 성공 여부
        """
        if self.mounted:
            logger.warning("Already mounted, unmounting first...")
            await self.unmount()
        
        try:
            # 마운트 포인트 생성
            mount_path = Path(self.MOUNT_POINT)
            mount_path.mkdir(parents=True, exist_ok=True)
            
            # SSHFS 명령 실행
            cmd = [
                "sshfs",
                "-o", f"IdentityFile={self.SSH_KEY_DEST}",
                "-o", "StrictHostKeyChecking=no",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "reconnect",
                "-o", "ServerAliveInterval=15",
                f"{self.SSH_USER}@{frontend_ip}:/home/{self.SSH_USER}",
                self.MOUNT_POINT
            ]
            
            logger.info(f"Mounting: {' '.join(cmd)}")
            
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
                self.mounted = True
                self.frontend_ip = frontend_ip
                logger.info(f"Successfully mounted {frontend_ip} to {self.MOUNT_POINT}")
                return True
            else:
                logger.error(f"Mount failed: {stderr.decode()}")
                return False
                
        except asyncio.TimeoutError:
            logger.error("Mount timeout")
            return False
        except Exception as e:
            logger.error(f"Mount error: {e}")
            return False
    
    async def unmount(self) -> bool:
        """
        SSHFS 마운트 해제
        
        Returns:
            bool: 해제 성공 여부
        """
        if not self.mounted:
            logger.info("Not mounted, nothing to unmount")
            return True
        
        try:
            # fusermount로 마운트 해제
            cmd = ["fusermount", "-u", self.MOUNT_POINT]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=10.0
            )
            
            if process.returncode == 0:
                self.mounted = False
                self.frontend_ip = None
                logger.info(f"Successfully unmounted {self.MOUNT_POINT}")
                return True
            else:
                logger.warning(f"Unmount warning: {stderr.decode()}")
                # 강제 해제 시도
                return await self._force_unmount()
                
        except asyncio.TimeoutError:
            logger.error("Unmount timeout, trying force unmount")
            return await self._force_unmount()
        except Exception as e:
            logger.error(f"Unmount error: {e}")
            return False
    
    async def _force_unmount(self) -> bool:
        """강제 마운트 해제"""
        try:
            cmd = ["fusermount", "-uz", self.MOUNT_POINT]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            await process.communicate()
            self.mounted = False
            self.frontend_ip = None
            logger.info("Force unmount completed")
            return True
        except Exception as e:
            logger.error(f"Force unmount failed: {e}")
            return False
    
    def is_mounted(self) -> bool:
        """마운트 상태 확인"""
        return self.mounted
