"""
Mount Manager (Chroot Version)

- SSHFS 원격 마운트 및 Mount Namespace 프로세스 격리 모듈
- Target User Pod 파일시스템 바인딩 및 세션 종료 시의 자동 Cleanup 환경 제어
"""

import logging
import os
import re
import signal
import subprocess
import sys
import shutil
import time
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

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

PROC_ROOT = "/proc"
# 좀비(Z)와 사라지는 중(X)인 프로세스는 이미 죽은 것으로 본다
DEAD_PROCESS_STATES = ("Z", "X")
KILL_POLL_SECONDS = 0.05
IPCRM_TIMEOUT_SECONDS = 5
SYSV_IPC_KINDS = ("shm", "msg", "sem")
MOUNTINFO_ESCAPE_RE = re.compile(r"\\([0-7]{3})")


class WorkspaceConnector:
    """
    SSHFS 마운트 및 Namespace 관리자
    """

    CHROOT_PATH = "/mnt/chroot"
    DEV_PATH = "/dev"
    SHM_PATH = "/dev/shm"
    MQUEUE_PATH = "/dev/mqueue"

    def __init__(self) -> None:
        self.user_pod_ip: Optional[str] = None
        # record_dev_entries 가 채우는 정리 검증 기준
        self.dev_entries: Optional[FrozenSet[str]] = None

    def attach_user_pod(self, user_pod_ip: str) -> None:
        """이 Compute Pod가 어느 User Pod를 위한 것인지 기록한다.

        실제 SSHFS 마운트는 세션이 붙을 때 setup_chroot_namespace 안에서
        일어난다. 여기서는 대상만 정해둔다.
        """
        self.user_pod_ip = user_pod_ip

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

    def detach_user_pod(self) -> None:
        """배정 기록을 지운다.

        마운트는 세션마다 만든 mount namespace 안에 있어 프로세스가 끝나면
        커널이 정리하므로, 따로 해제할 대상이 없다.
        """
        self.user_pod_ip = None

    @staticmethod
    def _read_mount_namespace(pid: str, proc_root: str = PROC_ROOT) -> Optional[str]:
        """pid 의 mount namespace 식별자. 그새 끝났거나 좀비면 None.

        권한 부족 같은 다른 오류는 올려보낸다. 못 읽은 프로세스를 건너뛰면
        정리 검증이 남은 세션을 놓친다.
        """
        try:
            return os.readlink(os.path.join(proc_root, pid, "ns", "mnt"))
        except (FileNotFoundError, ProcessLookupError):
            return None

    @staticmethod
    def _read_process_state(pid: str, proc_root: str = PROC_ROOT) -> Optional[str]:
        try:
            with open(os.path.join(proc_root, pid, "stat")) as f:
                stat = f.read()
        except (FileNotFoundError, ProcessLookupError):
            return None
        # comm 에 공백이나 괄호가 들어갈 수 있어 마지막 ')' 뒤에서 읽는다
        parts = stat.rsplit(")", 1)
        fields = parts[1].split() if len(parts) == 2 else []
        return fields[0] if fields else None

    @staticmethod
    def find_session_processes(proc_root: str = PROC_ROOT) -> List[int]:
        """Agent 와 다른 mount namespace 에서 살아 있는 프로세스의 pid 목록.

        세션은 무엇보다 먼저 자기 mount namespace 를 만들므로 namespace 가
        곧 사용자 프로세스의 표식이다. chroot 안에서 sudo 로 root 가 될 수
        있어 uid 로는 가릴 수 없다. 세션이 띄운 sshfs 데몬도 여기에 걸린다.
        Pod 가 다른 컨테이너와 PID namespace 를 공유하지 않는다는 전제다.
        """
        own_namespace = WorkspaceConnector._read_mount_namespace("self", proc_root)
        if own_namespace is None:
            # 기준이 없으면 모든 프로세스가 남의 것으로 보인다
            raise OSError("Cannot read the agent's own mount namespace")

        pids = []
        for entry in os.listdir(proc_root):
            if not entry.isdigit():
                continue
            namespace = WorkspaceConnector._read_mount_namespace(entry, proc_root)
            if namespace is None or namespace == own_namespace:
                continue
            state = WorkspaceConnector._read_process_state(entry, proc_root)
            if state is None or state in DEAD_PROCESS_STATES:
                continue
            pids.append(int(entry))
        return sorted(pids)

    @staticmethod
    def kill_session_processes(timeout_seconds: float, proc_root: str = PROC_ROOT) -> Tuple[int, List[int]]:
        """세션 프로세스를 모두 SIGKILL 하고 다 죽을 때까지 기다린다.

        죽이는 사이 새로 fork 된 것까지 잡도록 매번 다시 훑는다.
        (SIGKILL 을 보낸 프로세스 수, 마감까지 살아남은 pid) 를 돌려준다.
        """
        deadline = time.monotonic() + timeout_seconds
        killed = set()
        while True:
            alive = WorkspaceConnector.find_session_processes(proc_root)
            if not alive:
                return len(killed), []
            # 마감이 지났어도 한 번은 보내 본다
            if killed and time.monotonic() >= deadline:
                return len(killed), alive
            for pid in alive:
                try:
                    os.kill(pid, signal.SIGKILL)
                    killed.add(pid)
                except ProcessLookupError:
                    pass
            time.sleep(KILL_POLL_SECONDS)

    @staticmethod
    def _mount_points_under(path: str, proc_root: str = PROC_ROOT) -> List[str]:
        """Agent namespace 에서 path 자신이나 그 아래에 걸린 마운트 지점."""
        prefix = path.rstrip("/") + "/"
        mount_points = []
        with open(os.path.join(proc_root, "self", "mountinfo")) as f:
            for line in f:
                fields = line.split()
                if len(fields) < 5:
                    continue
                # mountinfo 는 공백 등을 \040 같은 8진수로 적는다
                mount_point = MOUNTINFO_ESCAPE_RE.sub(lambda m: chr(int(m.group(1), 8)), fields[4])
                if mount_point == path or mount_point.startswith(prefix):
                    mount_points.append(mount_point)
        return mount_points

    @staticmethod
    def _clear_directory(path: str, proc_root: str = PROC_ROOT) -> None:
        """path 안의 항목을 지운다. 아래에 마운트가 걸린 항목은 건드리지 않는다.

        마운트 너머는 다른 파일시스템이라 지우면 안 된다. 남은 항목은
        정리 검증에서 실패로 드러난다.
        """
        if not os.path.isdir(path):
            return
        mount_points = WorkspaceConnector._mount_points_under(path, proc_root)
        for name in os.listdir(path):
            entry = os.path.join(path, name)
            if any(mp == entry or mp.startswith(entry + "/") for mp in mount_points):
                logger.warning("[Warning] operation=scrub_remove path=%s reason=%r", entry, "mounted")
                continue
            try:
                if os.path.isdir(entry) and not os.path.islink(entry):
                    shutil.rmtree(entry)
                else:
                    os.unlink(entry)
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning("[Warning] operation=scrub_remove path=%s reason=%r", entry, str(e))

    @staticmethod
    def _clear_sysv_ipc() -> bool:
        """SysV IPC 객체를 모두 지운다. ipcrm 이 없거나 실패하면 False."""
        if shutil.which("ipcrm") is None:
            return False
        result = subprocess.run(["ipcrm", "--all"], capture_output=True, timeout=IPCRM_TIMEOUT_SECONDS)
        if result.returncode != 0:
            logger.warning(
                "[Warning] operation=scrub_ipcrm reason=%r",
                result.stderr.decode(errors="replace").strip(),
            )
            return False
        return True

    @staticmethod
    def clear_session_leftovers(proc_root: str = PROC_ROOT) -> bool:
        """세션이 자기 mount namespace 밖에 남길 수 있는 것을 지운다.

        /dev 를 통째로 bind 하고 IPC namespace 는 나누지 않으므로 /dev/shm,
        /dev/mqueue 와 SysV IPC 는 다음 사용자와 공유된다. /mnt/chroot 는
        agent 쪽에서는 빈 디렉터리여야 하지만, 마운트가 걸려 있으면 그 너머가
        User Pod 의 파일이므로 지우지 않는다. SysV IPC 를 비웠는지 돌려준다.
        """
        WorkspaceConnector._clear_directory(WorkspaceConnector.SHM_PATH, proc_root)
        WorkspaceConnector._clear_directory(WorkspaceConnector.MQUEUE_PATH, proc_root)
        ipc_cleared = WorkspaceConnector._clear_sysv_ipc()
        if not WorkspaceConnector._mount_points_under(WorkspaceConnector.CHROOT_PATH, proc_root):
            WorkspaceConnector._clear_directory(WorkspaceConnector.CHROOT_PATH, proc_root)
        return ipc_cleared

    @staticmethod
    def _is_empty_dir(path: str) -> bool:
        try:
            return not os.listdir(path)
        except FileNotFoundError:
            return True

    @staticmethod
    def _sysv_ipc_empty(proc_root: str = PROC_ROOT) -> bool:
        """SysV IPC 객체가 하나도 남지 않았는지. 각 파일의 첫 줄은 머리글이다.

        ipcrm 이 없거나 일부를 못 지운 경우가 여기서 드러난다. 파일이 없으면
        (커널이 노출하지 않으면) 그 종류는 확인할 수 없어 건너뛴다.
        """
        for kind in SYSV_IPC_KINDS:
            try:
                with open(os.path.join(proc_root, "sysvipc", kind)) as f:
                    lines = f.read().splitlines()
            except FileNotFoundError:
                continue
            if any(line.strip() for line in lines[1:]):
                return False
        return True

    def record_dev_entries(self) -> None:
        """지금의 /dev 최상위 항목을 정리 검증의 기준으로 남긴다 (agent 시작 시)."""
        self.dev_entries = frozenset(os.listdir(self.DEV_PATH))

    def _dev_unchanged(self) -> bool:
        """/dev 최상위 항목이 기준과 같은지.

        세션은 /dev 를 bind 하므로 sudo 로 만든 항목이 agent 의 /dev 에 남는다.
        무엇이 지워도 되는 것인지 가릴 수 없어 지우지 않고 실패로 돌린다.
        그러면 Pod 는 재사용되지 않고 삭제된다. 기준이 없으면 비교할 수 없어
        실패다.
        """
        if self.dev_entries is None:
            return False
        current = frozenset(os.listdir(self.DEV_PATH))
        if current == self.dev_entries:
            return True
        logger.warning(
            "[Warning] operation=scrub_check check=dev_unchanged added=%s removed=%s",
            ",".join(sorted(current - self.dev_entries)) or "-",
            ",".join(sorted(self.dev_entries - current)) or "-",
        )
        return False

    def scrub_checks(self, proc_root: str = PROC_ROOT) -> Dict[str, bool]:
        """정리 뒤 다음 사용자에게 넘겨도 되는지 항목별로 확인한다."""
        return {
            "no_session_processes": not self.find_session_processes(proc_root),
            "chroot_unmounted": not self._mount_points_under(self.CHROOT_PATH, proc_root),
            "chroot_empty": self._is_empty_dir(self.CHROOT_PATH),
            "dev_shm_empty": self._is_empty_dir(self.SHM_PATH),
            "dev_mqueue_empty": self._is_empty_dir(self.MQUEUE_PATH),
            "sysv_ipc_empty": self._sysv_ipc_empty(proc_root),
            "dev_unchanged": self._dev_unchanged(),
        }

    @staticmethod
    def _create_mount_namespace(libc: Any) -> None:
        if libc.unshare(CLONE_NEWNS) != 0:
            raise OSError("Failed to create mount namespace")
        if libc.mount(b"none", b"/", b"none", MS_REC | MS_PRIVATE, None) != 0:
            raise OSError("Failed to set mount propagation to private")

    @staticmethod
    def _mount_sshfs(user_pod_ip: str) -> None:
        os.makedirs(WorkspaceConnector.CHROOT_PATH, exist_ok=True)
        sshfs_result = subprocess.run(
            [
                "sshfs",
                f"{DEFAULT_USER}@{user_pod_ip}:/",
                WorkspaceConnector.CHROOT_PATH,
                "-o", "allow_other,suid,StrictHostKeyChecking=no,UserKnownHostsFile=/dev/null",
                "-o", "sftp_server=/usr/bin/sudo /usr/lib/openssh/sftp-server",
            ],
            capture_output=True,
        )
        if sshfs_result.returncode != 0:
            raise OSError(f"Failed to mount SSHFS from {user_pod_ip}: {sshfs_result.stderr.decode(errors='replace')}")

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

        final_sudo_target_str = f"{WorkspaceConnector.CHROOT_PATH}/usr/bin/sudo"
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
            target = f"{WorkspaceConnector.CHROOT_PATH}/{d}".encode('utf-8')
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
            chroot_hosts = f"{WorkspaceConnector.CHROOT_PATH}/etc/hosts"
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
            apt_conf_d = f"{WorkspaceConnector.CHROOT_PATH}/etc/apt/apt.conf.d"
            apt_conf_file = f"{apt_conf_d}/99nosandbox"
            if os.path.exists(apt_conf_d):
                with open(apt_conf_file, 'w') as f:
                    f.write('APT::Sandbox::User "root";\n')
        except Exception as e:
            logger.warning("Failed to setup apt sandbox config: %s", e)

    @staticmethod
    def _enter_chroot_and_drop_privileges(cwd: str) -> None:
        os.chroot(WorkspaceConnector.CHROOT_PATH)

        try:
            os.chdir(cwd)
        except (FileNotFoundError, PermissionError):
            os.chdir(DEFAULT_HOME)

        os.setgid(DEFAULT_GID)
        os.setuid(DEFAULT_UID)

        os.environ["HOME"] = DEFAULT_HOME
        os.environ["USER"] = DEFAULT_USER

    @staticmethod
    def setup_chroot_namespace(user_pod_ip: str, cwd: str = DEFAULT_HOME) -> None:
        """
        Mount Namespace 생성 기반 User Pod 대상 SSHFS 격리 마운트 (Chroot)

        - 호출 시점: Subprocess 생성 시 preexec_fn
        - 영향 범위: 호스트(부모)가 아닌 분기된 자식 세션 프로세스의 네임스페이스 한정
        """
        try:
            import ctypes
            libc = ctypes.CDLL(None)

            WorkspaceConnector._create_mount_namespace(libc)
            WorkspaceConnector._mount_sshfs(user_pod_ip)
            WorkspaceConnector._bind_mount_filesystems(libc)
            WorkspaceConnector._setup_secure_sudo(libc)
            WorkspaceConnector._fix_hostname(libc)
            WorkspaceConnector._fix_apt_sandbox()
            WorkspaceConnector._enter_chroot_and_drop_privileges(cwd)

        except Exception as e:
            print(f"Error in setup_chroot_namespace: {e}", file=sys.stderr)
            sys.exit(1)
