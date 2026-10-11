"""Put the test server back into a clean state before an experiment.

Restarting pods is not enough: Redis keeps its data on a volume, so tickets,
assigned-request records and sessions policy from earlier runs survive a restart
and can leak into the next run. Controllers are stopped while Redis is cleared
so nothing writes old state back, then everything is restarted.
"""

from __future__ import annotations

import time

from .cluster import (
    ServerSettingsError,
    describe_settings,
    read_server_settings,
    run_kubectl,
    wait_for_current_policy,
)

REDIS_KEY_PATTERN = "kda:*"
ROLLOUT_TIMEOUT = "5m"
COMMAND_TIMEOUT_SECONDS = 360
LOG_READER_POD = "kda-log-reset"

# Reading this label is safe - the production service uses it too, but only in
# its own namespace, and nothing here changes it.
USER_POD_SELECTOR = "kubessh=userpods"
USER_CLAIM_PREFIX = "ssh-"
USER_CLAIM_SUFFIX = "-pvc"
# A hundred Longhorn volumes take far longer to detach than a pod.
USER_POD_TIMEOUT_SECONDS = 900
# 사용자 파드를 한 번에 이만큼만 지우고, 다 지워진 뒤 다음 묶음으로 간다. 100개를 한꺼번에
# 지우면 Longhorn 볼륨 detach 가 몰려 apiserver(etcd) 쓰기가 밀린다(10/11 error budget
# 알림). 시뮬레이터 웜업 동시 접속 수(setup.max_inflight)와 같은 크기다.
USER_POD_DELETE_BATCH = 10


class ResetError(RuntimeError):
    pass


def reset_server(*, clear_logs: bool, recreate_user_pods: bool = False) -> None:
    controller_replicas = _controller_replicas()

    _step(f"Stop controllers (replicas {controller_replicas} -> 0)")
    _kubectl("scale", "deployment/controller", "--replicas=0")
    _kubectl("wait", "--for=delete", "pod", "-l", "app=controller", "--timeout=120s", allow_missing=True)

    _step(f"Delete Redis keys matching {REDIS_KEY_PATTERN}")
    before = _redis_key_count()
    if before:
        _kubectl(
            "exec", "deployment/controller-queue-redis", "--", "sh", "-c",
            f"redis-cli --scan --pattern '{REDIS_KEY_PATTERN}' | xargs -r -n 500 redis-cli del > /dev/null",
        )
    after = _redis_key_count()
    print(f"  keys: {before} -> {after}")
    if after:
        raise ResetError(f"{after} Redis keys still match {REDIS_KEY_PATTERN}")

    # Assigned pods have left the Deployment selector, so a rollout restart
    # would not remove them; cold-start pods are standalone. Deployment 없이
    # 직접 관리하는 arm 의 미리 만든 파드(idle)도 Deployment 밖이라 함께 지운다.
    _step("Delete compute pods outside the Deployment")
    _kubectl(
        "delete", "pod", "-l", "app=compute-pod,compute-status in (assigned,idle)",
        "--wait=true", "--timeout=120s",
    )

    # Arms do not share a user pod shape: the work runs inside the user pod in
    # the baseline arm, so its shell container carries a compute pod's limits,
    # while the other arms leave it at the shell's own small limits. A pod that
    # survives the switch keeps the previous arm's shape, so it is recreated.
    # PVC 는 지우지 않는다(10/11 사용자 결정): 볼륨을 새로 만들고 지우는 것이
    # Longhorn 부하의 대부분이고, 다음 파드가 같은 볼륨을 다시 붙이면 된다.
    # Controllers are already stopped and swlabssh is restarted below, so nothing
    # recreates a pod in between.
    if recreate_user_pods:
        _step("Delete user pods (PVCs are kept)")
        _delete_user_pods()

    # compute 버퍼 Deployment 는 Deployment 로 버퍼를 두는 arm(develop, reuse)에만 있다.
    # 컨트롤러가 파드를 직접 만드는 arm(cold start, warm_buffer_direct)에는 없으므로
    # 있을 때만 재시작한다. 이름을 박아 두면 그런 arm 에서 초기화가 실패한다.
    restarted = ["swlabssh", *_resource_names("deployment", "-l", "app=compute-pod")]
    _step(f"Start controllers (replicas 0 -> {controller_replicas}) and restart {', '.join(restarted)}")
    _kubectl("scale", "deployment/controller", f"--replicas={controller_replicas}")
    _kubectl("rollout", "restart", *[f"deployment/{name}" for name in restarted])
    for deployment in ("controller", *restarted):
        _kubectl("rollout", "status", f"deployment/{deployment}", f"--timeout={ROLLOUT_TIMEOUT}")

    _step("Wait for warm pods to match R/N")
    wait_for_current_policy()

    if clear_logs:
        _step("Clear collected logs on logs-pvc")
        _clear_logs()

    settings = read_server_settings()
    print(f"\nServer: {describe_settings(settings)}")
    print(f"Redis keys matching {REDIS_KEY_PATTERN}: {_redis_key_count()} (controllers republish sessions policy)")


def _delete_user_pods() -> None:
    pods = _resource_names("pod", "-l", USER_POD_SELECTOR)
    print(f"  pods: {len(pods)} (batches of {USER_POD_DELETE_BATCH})")
    for i in range(0, len(pods), USER_POD_DELETE_BATCH):
        batch = pods[i:i + USER_POD_DELETE_BATCH]
        _kubectl(
            "delete", "pod", *batch, "--ignore-not-found=true",
            "--wait=true", f"--timeout={USER_POD_TIMEOUT_SECONDS}s",
            timeout=USER_POD_TIMEOUT_SECONDS,
        )
        print(f"  deleted {min(i + len(batch), len(pods))}/{len(pods)}")

    left_pods = _resource_names("pod", "-l", USER_POD_SELECTOR)
    if left_pods:
        raise ResetError(f"{len(left_pods)} user pods are still there")
    print("  pods: 0 (PVCs kept)")


def _resource_names(kind: str, *selector: str) -> list[str]:
    output = _kubectl("get", kind, *selector, "-o", "name", allow_missing=True)
    return [line.split("/", 1)[-1] for line in output.split() if "/" in line]


def _controller_replicas() -> int:
    replicas = _kubectl("get", "deployment/controller", "-o", "jsonpath={.spec.replicas}").strip()
    # A reset interrupted after the scale-down leaves 0 behind; fall back to
    # the deployed default instead of restarting with no controllers.
    return int(replicas) if replicas.isdigit() and int(replicas) > 0 else 2


def _redis_key_count() -> int:
    output = _kubectl(
        "exec", "deployment/controller-queue-redis", "--", "sh", "-c",
        f"redis-cli --scan --pattern '{REDIS_KEY_PATTERN}' | wc -l",
    )
    return int(output.strip() or 0)


def _clear_logs() -> None:
    overrides = (
        '{"apiVersion":"v1","spec":{"restartPolicy":"Never","containers":[{"name":"shell",'
        '"image":"alpine:3.20","command":["sleep","600"],'
        '"volumeMounts":[{"name":"logs","mountPath":"/mnt/logs"}]}],'
        '"volumes":[{"name":"logs","persistentVolumeClaim":{"claimName":"logs-pvc"}}]}}'
    )
    _kubectl("delete", "pod", LOG_READER_POD, "--ignore-not-found=true", "--wait=true")
    _kubectl("run", LOG_READER_POD, "--image", "alpine:3.20", "--restart=Never", "--overrides", overrides)
    try:
        _kubectl("wait", "--for=condition=Ready", f"pod/{LOG_READER_POD}", "--timeout=90s")
        output = _kubectl(
            "exec", LOG_READER_POD, "--", "sh", "-c",
            "du -sh /mnt/logs; rm -f /mnt/logs/*.jsonl /mnt/logs/*.jsonl.gz; ls /mnt/logs",
        )
        print("  " + output.strip().replace("\n", "\n  "))
    finally:
        _kubectl("delete", "pod", LOG_READER_POD, "--ignore-not-found=true", "--wait=false")


def _step(message: str) -> None:
    print(f"\n[{time.strftime('%H:%M:%S')}] {message}")


def _kubectl(*args: str, allow_missing: bool = False, timeout: int = COMMAND_TIMEOUT_SECONDS) -> str:
    # Rollouts and pod deletions take far longer than a settings read, so this
    # reuses the shared runner with a longer deadline and reports as a reset.
    try:
        return run_kubectl(
            *args,
            timeout=timeout,
            allow_missing=allow_missing,
        )
    except ServerSettingsError as exc:
        raise ResetError(str(exc)) from exc
