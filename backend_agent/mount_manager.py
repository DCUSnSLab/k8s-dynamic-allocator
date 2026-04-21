"""
Mount Manager (Chroot Version)

- SSHFS 원격 마운트 및 Mount Namespace 프로세스 격리 모듈
- Target Frontend Pod 파일시스템 바인딩 및 세션 종료 시의 자동 Cleanup 환경 제어
"""

import logging
import os
import subprocess
import sys
import shutil
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


CLONE_NEWNS = 0x00020000
MS_REC = 16384
MS_PRIVATE = 262144
MS_BIND = 4096

SUID_PERMISSION = 0o4755
DEFAULT_UID = 1000
DEFAULT_GID = 1000
DEFAULT_USER = "dcuuser"
DEFAULT_HOME = f"/home/{DEFAULT_USER}"

BIND_MOUNT_TARGETS = ["proc", "sys", "dev", "usr/lib/sudo"]


class MountManager:
    """
    SSHFS 마운트 및 Namespace 관리자
    """

    CHROOT_PATH = "/mnt/chroot"
    
    def __init__(self) -> None:
        self.frontend_ip: Optional[str] = None

    async def mount(self, frontend_ip: str, frontend_pod: Optional[str] = None) -> bool:
        self.frontend_ip = frontend_ip
        return True

    def setup_ssh_key(self) -> bool:
        """Secret으로 마운트된 SSH 키를 ~/.ssh/id_rsa로 복사하고 권한 설정"""
        try:
            ssh_dir = Path("/root/.ssh")
            ssh_dir.mkdir(parents=True, exist_ok=True)
            
            key_mount_dir = Path("/etc/ssh-key")
            key_file = None
            
            # 마운트된 디렉토리에서 키 파일 검색 (숨김 파일 제외, .pub 제외)
            if key_mount_dir.exists():
                candidates = []
                for f in key_mount_dir.iterdir():
                    if f.is_file() and not f.name.startswith("..") and not f.name.endswith(".pub"):
                        candidates.append(f)
                
                # 우선순위: id_rsa > ssh-privatekey > 그 외
                candidates.sort(key=lambda x: (x.name != 'id_rsa', x.name != 'ssh-privatekey'))
                
                if candidates:
                    key_file = candidates[0]
            
            if key_file and key_file.exists():
                key_dst = ssh_dir / "id_rsa"
                shutil.copy(key_file, key_dst)
                key_dst.chmod(0o600)
                logger.info("SSH key setup completed using %s", key_file.name)
                (ssh_dir / "known_hosts").touch(mode=0o644)
                return True
            else:
                logger.warning("SSH key source not found in %s", key_mount_dir)
                if key_mount_dir.exists():
                    logger.warning("Contents: %s", [f.name for f in key_mount_dir.iterdir()])
                return False
        except Exception as e:
            logger.error("Failed to setup SSH key: %s", e)
            return False

    async def unmount(self) -> bool:
        """Namespace 사용으로 프로세스 종료 시 자동 정리, IP만 리셋"""
        self.frontend_ip = None
        return True

    @staticmethod
    def _create_mount_namespace(libc: Any) -> None:
        if libc.unshare(CLONE_NEWNS) != 0:
            raise OSError("Failed to create mount namespace")
        if libc.mount(b"none", b"/", b"none", MS_REC | MS_PRIVATE, None) != 0:
            raise OSError("Failed to set mount propagation to private")

    @staticmethod
    def _mount_sshfs(frontend_ip: str) -> None:
        os.makedirs(MountManager.CHROOT_PATH, exist_ok=True)
        sshfs_result = subprocess.run(
            [
                "sshfs",
                f"{DEFAULT_USER}@{frontend_ip}:/",
                MountManager.CHROOT_PATH,
                "-o", "allow_other,suid,StrictHostKeyChecking=no,UserKnownHostsFile=/dev/null",
                "-o", "sftp_server=/usr/bin/sudo /usr/lib/openssh/sftp-server",
            ],
            capture_output=True,
        )
        if sshfs_result.returncode != 0:
            raise OSError(f"Failed to mount SSHFS from {frontend_ip}: {sshfs_result.stderr.decode(errors='replace')}")

    @staticmethod
    def _setup_secure_sudo(libc: Any) -> None:
        secure_bin_path = "/tmp/secure_bin"
        if os.path.exists(secure_bin_path):
            shutil.rmtree(secure_bin_path)
        os.makedirs(secure_bin_path, exist_ok=True)

        local_sudo = "/usr/bin/sudo"
        target_sudo_tmp = f"{secure_bin_path}/sudo"

        if os.path.exists(local_sudo):
            shutil.copy(local_sudo, target_sudo_tmp)
            os.chmod(target_sudo_tmp, SUID_PERMISSION)

        final_sudo_target_str = f"{MountManager.CHROOT_PATH}/usr/bin/sudo"
        final_sudo_target = final_sudo_target_str.encode('utf-8')

        if not os.path.exists(final_sudo_target):
            open(final_sudo_target, 'a').close()

        safe_sudo_src = target_sudo_tmp.encode('utf-8')
        if libc.mount(safe_sudo_src, final_sudo_target, b"none", MS_BIND, None) != 0:
            logger.warning("Failed to bind mount secure sudo")

        remount_result = subprocess.run(
            ["mount", "-o", "remount,bind,suid", final_sudo_target_str],
            capture_output=True,
        )
        if remount_result.returncode != 0:
            logger.warning("Failed to remount secure sudo with suid")

    @staticmethod
    def _bind_mount_filesystems(libc: Any) -> None:
        for d in BIND_MOUNT_TARGETS:
            src = f"/{d}".encode('utf-8')
            target = f"{MountManager.CHROOT_PATH}/{d}".encode('utf-8')
            source_path = f"/{d}"

            if not os.path.exists(source_path):
                logger.warning("Source %s not found, skipping bind mount", d)
                continue

            if not os.path.exists(target):
                if os.path.isdir(source_path):
                    os.makedirs(target, exist_ok=True)
                else:
                    parent = os.path.dirname(target)
                    if not os.path.exists(parent):
                        os.makedirs(parent, exist_ok=True)
                    open(target, 'a').close()

            if libc.mount(src, target, b"none", MS_BIND | MS_REC, None) != 0:
                raise OSError(f"Failed to bind mount {d}")

    @staticmethod
    def _fix_hostname(libc: Any) -> None:
        try:
            chroot_hosts = f"{MountManager.CHROOT_PATH}/etc/hosts"
            temp_hosts = "/tmp/hosts_overlay"

            hosts_content = ""
            if os.path.exists(chroot_hosts):
                with open(chroot_hosts, 'r') as f:
                    hosts_content = f.read()

            current_hostname = os.uname().nodename
            if current_hostname not in hosts_content:
                hosts_content += f"\n127.0.0.1\t{current_hostname}\n"

            with open(temp_hosts, 'w') as f:
                f.write(hosts_content)

            if libc.mount(temp_hosts.encode('utf-8'), chroot_hosts.encode('utf-8'), b"none", MS_BIND, None) != 0:
                logger.warning("Failed to bind mount overlay hosts file")
        except Exception as e:
            logger.warning("Failed to setup hosts overlay: %s", e)

    @staticmethod
    def _fix_apt_sandbox() -> None:
        try:
            apt_conf_d = f"{MountManager.CHROOT_PATH}/etc/apt/apt.conf.d"
            apt_conf_file = f"{apt_conf_d}/99nosandbox"
            if os.path.exists(apt_conf_d):
                with open(apt_conf_file, 'w') as f:
                    f.write('APT::Sandbox::User "root";\n')
        except Exception as e:
            logger.warning("Failed to setup apt sandbox config: %s", e)

    @staticmethod
    def _enter_chroot_and_drop_privileges(cwd: str) -> None:
        os.chroot(MountManager.CHROOT_PATH)

        try:
            os.chdir(cwd)
        except (FileNotFoundError, PermissionError):
            os.chdir(DEFAULT_HOME)

        os.setgid(DEFAULT_GID)
        os.setuid(DEFAULT_UID)

        os.environ["HOME"] = DEFAULT_HOME
        os.environ["USER"] = DEFAULT_USER

    @staticmethod
    def setup_chroot_namespace(frontend_ip: str, cwd: str = DEFAULT_HOME) -> None:
        """
        Mount Namespace 생성 기반 Frontend Pod 대상 SSHFS 격리 마운트 (Chroot)

        - 호출 시점: Subprocess 생성 시 preexec_fn
        - 영향 범위: 호스트(부모)가 아닌 분기된 자식 세션 프로세스의 네임스페이스 한정
        """
        try:
            import ctypes
            libc = ctypes.CDLL(None)

            MountManager._create_mount_namespace(libc)
            MountManager._mount_sshfs(frontend_ip)
            MountManager._bind_mount_filesystems(libc)
            MountManager._setup_secure_sudo(libc)
            MountManager._fix_hostname(libc)
            MountManager._fix_apt_sandbox()
            MountManager._enter_chroot_and_drop_privileges(cwd)

        except Exception as e:
            print(f"Error in setup_chroot_namespace: {e}", file=sys.stderr)
            sys.exit(1)
