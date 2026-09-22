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


class ResetError(RuntimeError):
    pass


def reset_server(*, clear_logs: bool) -> None:
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
    # would not remove them; cold-start pods are standalone.
    _step("Delete compute pods outside the Deployment")
    _kubectl("delete", "pod", "-l", "app=compute-pod,compute-status=assigned", "--wait=true", "--timeout=120s")

    _step(f"Start controllers (replicas 0 -> {controller_replicas}) and restart swlabssh, compute-general")
    _kubectl("scale", "deployment/controller", f"--replicas={controller_replicas}")
    _kubectl("rollout", "restart", "deployment/swlabssh", "deployment/compute-general")
    for deployment in ("controller", "swlabssh", "compute-general"):
        _kubectl("rollout", "status", f"deployment/{deployment}", f"--timeout={ROLLOUT_TIMEOUT}")

    _step("Wait for warm pods to match R/N")
    wait_for_current_policy()

    if clear_logs:
        _step("Clear collected logs on logs-pvc")
        _clear_logs()

    settings = read_server_settings()
    print(f"\nServer: {describe_settings(settings)}")
    print(f"Redis keys matching {REDIS_KEY_PATTERN}: {_redis_key_count()} (controllers republish sessions policy)")


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


def _kubectl(*args: str, allow_missing: bool = False) -> str:
    # Rollouts and pod deletions take far longer than a settings read, so this
    # reuses the shared runner with a longer deadline and reports as a reset.
    try:
        return run_kubectl(
            *args,
            timeout=COMMAND_TIMEOUT_SECONDS,
            allow_missing=allow_missing,
        )
    except ServerSettingsError as exc:
        raise ResetError(str(exc)) from exc
