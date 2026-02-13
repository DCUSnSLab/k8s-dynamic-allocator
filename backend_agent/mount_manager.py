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
                logger.info(f"SSH key setup completed using {key_file.name}")
                
                # Known hosts 파일 생성 (빈 파일)
                (ssh_dir / "known_hosts").touch(mode=0o644)
            else:
                logger.warning(f"SSH key source not found in {key_mount_dir}")
                # 디버깅을 위해 디렉토리 목록 출력
                if key_mount_dir.exists():
                    logger.warning(f"Contents: {[f.name for f in key_mount_dir.iterdir()]}")
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
            # 중요: Remote에서 root 권한으로 파일에 접근하기 위해 sftp-server를 sudo로 실행
            # (dcuuser가 NOPASSWD sudo 권한을 가지고 있다고 가정)
            ssh_cmd = (
                f"sshfs dcuuser@{frontend_ip}:/ {MountManager.CHROOT_PATH} "
                "-o allow_other,suid,StrictHostKeyChecking=no,UserKnownHostsFile=/dev/null "
                "-o sftp_server=\"/usr/bin/sudo /usr/lib/openssh/sftp-server\"" 
            )
            if os.system(ssh_cmd) != 0:
                raise OSError(f"Failed to mount SSHFS from {frontend_ip}")

            # 4. Bind Mounts (Overlay)
            # Backend의 /proc, /sys, /dev를 Chroot 내부로 덮어쓰기
            # 4. Bind Mounts (Overlay)
            # Backend의 /proc, /sys, /dev를 Chroot 내부로 덮어쓰기
            
            # [Secure Bin Strategy]
            # SSHFS 상에서 SUID가 제대로 동작하지 않는 문제를 해결하기 위해
            # Tmpfs(메모리 파일시스템)를 생성하고 거기에 sudo를 복사한 뒤 SUID 설정
            # 중요: Frontend에 흔적을 남기지 않기 위해 Chroot 외부 경로(/tmp/secure_bin) 사용
            
            secure_bin_path = "/tmp/secure_bin"
            if os.path.exists(secure_bin_path):
                shutil.rmtree(secure_bin_path)
            os.makedirs(secure_bin_path, exist_ok=True)
            
            # 1. Tmpfs 마운트 (Optional: 컨테이너 /tmp가 이미 램디스크거나 로컬이면 굳이 필요없지만 확실히 하기 위해)
            # 여기서는 로컬 디스크(/tmp)를 그대로 사용해도 SUID가 지원되므로 Tmpfs 마운트 생략 가능하나
            # 깔끔한 정리를 위해 파일 카피만 수행
            
            # 2. sudo 바이너리 복사 및 권한 설정
            local_sudo = "/usr/bin/sudo"
            target_sudo_tmp = f"{secure_bin_path}/sudo"
            
            if os.path.exists(local_sudo):
                shutil.copy(local_sudo, target_sudo_tmp)
                os.chmod(target_sudo_tmp, 0o4755) # rwsr-xr-x (SUID)
            
            # sudo를 제외한 일반 Bind Mount 목록
            bind_mounts = ["proc", "sys", "dev", "usr/lib/sudo"]
            
            for d in bind_mounts:
                src = f"/{d}".encode('utf-8')
                target = f"{MountManager.CHROOT_PATH}/{d}".encode('utf-8')
                
                source_path = f"/{d}"
                if os.path.exists(source_path):
                    is_dir = os.path.isdir(source_path)
                    if not os.path.exists(target):
                        if is_dir:
                            os.makedirs(target, exist_ok=True)
                        else:
                            parent = os.path.dirname(target)
                            if not os.path.exists(parent):
                                os.makedirs(parent, exist_ok=True)
                            open(target, 'a').close()

                    # Bind Mount 실행
                    # 중요: /dev/pts 등 하위 마운트 포인트까지 포함하기 위해 MS_REC(Recursive) 플래그 추가
                    # MS_REC = 16384
                    if libc.mount(src, target, b"none", MS_BIND | MS_REC, None) != 0:
                        raise OSError(f"Failed to bind mount {d}")
                else:
                    logger.warning(f"Source {d} not found, skipping bind mount")

            # 3. Secure Bin의 sudo를 /usr/bin/sudo로 Bind Mount
            # 이제 원본은 SSHFS가 아닌 Tmpfs에 있으므로 확실하게 SUID가 동작함
            final_sudo_target_str = f"{MountManager.CHROOT_PATH}/usr/bin/sudo"
            final_sudo_target = final_sudo_target_str.encode('utf-8')
            
             # 타겟 파일 생성 (없을 경우) - SSHFS라 존재하겠지만 안전하게
            if not os.path.exists(final_sudo_target):
                 open(final_sudo_target, 'a').close()
            
            safe_sudo_src = target_sudo_tmp.encode('utf-8')
            if libc.mount(safe_sudo_src, final_sudo_target, b"none", MS_BIND, None) != 0:
                 logger.warning("Failed to bind mount secure sudo")
            
            # [CRITICAL] Bind Mount된 파일에 대해 명시적으로 SUID 허용 Remount 수행
            # 상위 마운트(SSHFS)가 nosuid일 경우, 하위 Bind Mount도 nosuid를 상속받으므로 이를 해제해야 함
            if os.system(f"mount -o remount,bind,suid {final_sudo_target_str}") != 0:
                 logger.warning("Failed to remount secure sudo with suid")

            # [Hostname Fix]
            # sudo 실행 시 "unable to resolve host" 경고 해결을 위해
            # Frontend의 /etc/hosts에 Backend의 Hostname을 추가하여 Bind Mount
            try:
                chroot_hosts = f"{MountManager.CHROOT_PATH}/etc/hosts"
                temp_hosts = "/tmp/hosts_overlay"
                
                # 1. Frontend Hosts 파일 읽기
                hosts_content = ""
                if os.path.exists(chroot_hosts):
                    with open(chroot_hosts, 'r') as f:
                        hosts_content = f.read()
                
                # 2. 현재 Hostname 추가 (이미 있으면 생략)
                current_hostname = os.uname().nodename
                if current_hostname not in hosts_content:
                    hosts_content += f"\n127.0.0.1\t{current_hostname}\n"
                
                # 3. 임시 파일 생성
                with open(temp_hosts, 'w') as f:
                    f.write(hosts_content)
                
                # 4. Bind Mount (/tmp/hosts_overlay -> /mnt/chroot/etc/hosts)
                # 이것도 Bind Mount이므로, 원본(SSHFS)은 수정되지 않음
                if libc.mount(temp_hosts.encode('utf-8'), chroot_hosts.encode('utf-8'), b"none", MS_BIND, None) != 0:
                     logger.warning("Failed to bind mount overlay hosts file")
                     
            except Exception as e:
                logger.warning(f"Failed to setup hosts overlay: {e}")

            # 5. Chroot 진입
            os.chroot(MountManager.CHROOT_PATH)
            os.chdir("/home/dcuuser")

            # 6. Drop Privileges (Switch to dcuuser: 1000)
            # 순서 중요: setgid 먼저, 그 다음 setuid
            # (만약 setuid를 먼저 하면 root 권한을 잃어서 setgid 불가능)
            os.setgid(1000)
            os.setuid(1000)
            
            # HOME 환경변수 재설정 (Login shell이 올바르게 로드하도록)
            os.environ["HOME"] = "/home/dcuuser"
            os.environ["USER"] = "dcuuser"
            
        except Exception as e:
            # 자식 프로세스 내에서의 에러는 stderr로 출력
            print(f"Error in setup_chroot_namespace: {e}", file=sys.stderr)
            sys.exit(1)
