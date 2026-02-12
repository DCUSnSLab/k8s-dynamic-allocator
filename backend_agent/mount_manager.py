"""
Mount Manager (Chroot Version)

SSHFS 마운트 및 Chroot 환경 구성을 담당
Mount Namespace를 사용하여 프로세스 격리 및 자동 Cleanup 지원
"""

import asyncio
import logging
import os
import sys
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class MountManager:
    """
    SSHFS 마운트 및 Namespace 관리자
    """
    
    CHROOT_PATH = "/mnt/chroot"
    
    def __init__(self):
        self.frontend_ip = None
    
    async def mount(self, frontend_ip: str) -> bool:
        """
        Frontend 마운트 준비 (실제 마운트는 각 세션 프로세스 내에서 수행)
        여기서는 Frontend IP만 기록하고 연결 테스트만 수행
        
        Args:
            frontend_ip: Frontend Pod IP
            
        Returns:
            bool: 성공 여부
        """
        self.frontend_ip = frontend_ip
        logger.info(f"Mount prepared for {frontend_ip}")
        return True

    def setup_ssh_key(self):
        """
        SSH 키 설정 (Pod 시작 시 호출)
        Secret으로 마운트된 키를 ~/.ssh/id_rsa로 복사하고 권한 설정
        """
        try:
            ssh_dir = Path("/root/.ssh")
            ssh_dir.mkdir(parents=True, exist_ok=True)
            
            key_src = Path("/etc/ssh-key/ssh-privatekey")
            key_dst = ssh_dir / "id_rsa"
            
            if key_src.exists():
                shutil.copy(key_src, key_dst)
                key_dst.chmod(0o600)
                logger.info("SSH key setup completed")
            else:
                logger.warning("SSH key source not found")
        except Exception as e:
            logger.error(f"Failed to setup SSH key: {e}")

    async def unmount(self) -> bool:
        """
        마운트 해제
        
        Namespace를 사용하므로 별도의 언마운트 작업이 필요 없음.
        프로세스가 종료되면 마운트도 자동으로 정리됨.
        """
        logger.info("Unmount requested (No-op due to Namespace)")
        self.frontend_ip = None
        return True

    @staticmethod
    def setup_chroot_namespace(frontend_ip: str):
        """
        [Child Process Only]
        새로운 Mount Namespace를 생성하고 Chroot 환경을 구성
        
        이 함수는 subprocess의 preexec_fn에서 호출됨
        """
        try:
            import ctypes
            libc = ctypes.CDLL(None)
            
            # Linux Constants
            CLONE_NEWNS = 0x00020000
            MS_REC = 16384
            MS_PRIVATE = 262144
            MS_BIND = 4096

            # 1. New Mount Namespace 생성 (CLONE_NEWNS)
            if libc.unshare(CLONE_NEWNS) != 0:
                raise OSError("Failed to create mount namespace")
            
            # 2. Mount Propagation 설정 (Private)
            # 호스트의 마운트 이벤트가 전파되지 않도록 설정
            if libc.mount(b"none", b"/", b"none", MS_REC | MS_PRIVATE, None) != 0:
                 raise OSError("Failed to set mount propagation to private")

            # 3. Frontend Root Mount (SSHFS)
            # /mnt/chroot 생성
            os.makedirs(MountManager.CHROOT_PATH, exist_ok=True)
            
            # sshfs 명령어 실행 (os.system 사용 - namespace 내부이므로 안전)
            ssh_cmd = (
                f"sshfs dcuuser@{frontend_ip}:/ {MountManager.CHROOT_PATH} "
                "-o allow_other,StrictHostKeyChecking=no,UserKnownHostsFile=/dev/null"
            )
            if os.system(ssh_cmd) != 0:
                raise OSError(f"Failed to mount SSHFS from {frontend_ip}")

            # 4. Bind Mounts (Overlay)
            # Backend의 /proc, /sys, /dev를 Chroot 내부로 덮어쓰기
            bind_mounts = ["proc", "sys", "dev"]
            
            for d in bind_mounts:
                src = f"/{d}".encode('utf-8')
                target = f"{MountManager.CHROOT_PATH}/{d}".encode('utf-8')
                
                # 타겟 디렉토리가 없으면 생성 (SSHFS라 있을 수 있지만 안전하게)
                if not os.path.exists(target):
                    os.makedirs(target, exist_ok=True)
                
                if libc.mount(src, target, b"none", MS_BIND, None) != 0:
                    raise OSError(f"Failed to bind mount {d}")

            # 5. Chroot 진입
            os.chroot(MountManager.CHROOT_PATH)
            os.chdir("/home/dcuuser")
            
        except Exception as e:
            # 자식 프로세스 내에서의 에러는 stderr로 출력
            print(f"Error in setup_chroot_namespace: {e}", file=sys.stderr)
            sys.exit(1)
