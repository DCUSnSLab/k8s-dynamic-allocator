"""Align R/N on the server before a run and record what the server was running.

R/N are Deployment annotations the controller picks up without a restart, and
Jenkins resets them on every deploy, so a run sets them itself. Controller
count, pool mode and images are only read: changing those needs a redeploy.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any

from .config import KUBERNETES_NAMESPACE, SimulatorConfig


ANNOTATION_R = "k8s-dynamic-allocator/pool-available-min"
ANNOTATION_N = "k8s-dynamic-allocator/pool-total-max"
KUBECTL_TIMEOUT_SECONDS = 30
POOL_SETTLE_TIMEOUT_SECONDS = 180.0
POOL_SETTLE_POLL_SECONDS = 2.0


class ServerSettingsError(RuntimeError):
    pass


def prepare_server(config: SimulatorConfig) -> dict[str, Any]:
    """Apply R/N from the config when both are set, then return the server settings."""
    wanted = (config.experiment.pool_size, config.experiment.pool_total_max)
    apply_policy = None not in wanted
    if not apply_policy and wanted != (None, None):
        print("R/N not applied: set both experiment.pool_size and experiment.pool_total_max")

    try:
        settings = read_server_settings()
    except ServerSettingsError as exc:
        if apply_policy:
            raise
        print(f"Server settings not recorded: {exc}")
        return {"error": str(exc)}

    if apply_policy:
        if settings["pool_mode"] != "warm_pool":
            print(f"R/N not applied: pool mode is {settings['pool_mode']}")
        else:
            r, n = wanted
            for pool in settings["pools"]:
                if (pool["R"], pool["N"]) != (r, n):
                    print(f"Pool policy {pool['deployment']}: R={pool['R']} N={pool['N']} -> R={r} N={n}")
                    _kubectl(
                        "annotate",
                        f"deployment/{pool['deployment']}",
                        f"{ANNOTATION_R}={r}",
                        f"{ANNOTATION_N}={n}",
                        "--overwrite",
                    )
            wait_for_pools(r, n)
            settings = read_server_settings()

    print(f"Server: {describe_settings(settings)}")
    return settings


def read_server_settings() -> dict[str, Any]:
    deployments = {item["metadata"]["name"]: item for item in _kubectl_json("get", "deployments")["items"]}
    pods = _kubectl_json("get", "pods", "-l", "app=warm-pod-pool")["items"]
    controller = deployments.get("controller")
    swlabssh = deployments.get("swlabssh")

    pools = []
    for name, deployment in sorted(deployments.items()):
        labels = deployment["metadata"].get("labels") or {}
        if labels.get("app") != "warm-pod-pool":
            continue
        compute_type = labels.get("compute-type", "")
        annotations = deployment["metadata"].get("annotations") or {}
        members = [
            pod for pod in pods
            if _labels(pod).get("compute-type") == compute_type
            and not pod["metadata"].get("deletionTimestamp")
        ]
        available = [pod for pod in members if _labels(pod).get("pool-status") == "available"]
        pools.append(
            {
                "deployment": name,
                "compute_type": compute_type,
                "R": _int_or_none(annotations.get(ANNOTATION_R)),
                "N": _int_or_none(annotations.get(ANNOTATION_N)),
                "available": len(available),
                "available_ready": sum(1 for pod in available if _is_ready(pod)),
                "assigned": sum(1 for pod in members if _labels(pod).get("pool-status") == "assigned"),
                "image": _image(deployment),
            }
        )

    pool_mode = None
    if controller is not None:
        # Same default and normalization as the controller's COMPUTE_POOL_MODE.
        pool_mode = (_env(controller, "COMPUTE_POOL_MODE") or "warm_pool").strip().lower().replace("-", "_")

    return {
        "namespace": KUBERNETES_NAMESPACE,
        "pool_mode": pool_mode,
        "controller_replicas": _replicas(controller),
        "controller_image": _image(controller),
        "swlabssh_replicas": _replicas(swlabssh),
        "swlabssh_image": _image(swlabssh),
        "pools": pools,
    }


def wait_for_pools(r: int, n: int) -> None:
    """Wait until every pool holds the ready warm pods R/N asks for."""
    started = time.monotonic()
    while True:
        pending = [pool for pool in read_server_settings()["pools"] if not _settled(pool, r, n)]
        waited = time.monotonic() - started
        if not pending:
            if waited >= POOL_SETTLE_POLL_SECONDS:
                print(f"Warm pool ready after {waited:.0f}s")
            return
        if waited >= POOL_SETTLE_TIMEOUT_SECONDS:
            pool = pending[0]
            raise ServerSettingsError(
                f"{pool['deployment']} did not reach {_desired(pool, r, n)} ready warm pods "
                f"within {POOL_SETTLE_TIMEOUT_SECONDS:.0f}s "
                f"(available={pool['available']}, ready={pool['available_ready']})"
            )
        time.sleep(POOL_SETTLE_POLL_SECONDS)


def _desired(pool: dict[str, Any], r: int, n: int) -> int:
    # Same formula as the controller's capacity reconciler.
    return min(r, max(0, n - pool["assigned"]))


def _settled(pool: dict[str, Any], r: int, n: int) -> bool:
    desired = _desired(pool, r, n)
    return pool["available"] == desired and pool["available_ready"] == desired


def wait_for_current_policy() -> None:
    """Wait using the R/N already on the server, for callers that change neither."""
    pools = read_server_settings()["pools"]
    policies = {(pool["R"], pool["N"]) for pool in pools if None not in (pool["R"], pool["N"])}
    for r, n in policies:
        wait_for_pools(r, n)


def describe_settings(settings: dict[str, Any]) -> str:
    pools = ", ".join(
        f"{pool['deployment']} R={pool['R']} N={pool['N']} ready={pool['available_ready']}"
        for pool in settings["pools"]
    )
    return (
        f"mode={settings['pool_mode']} controllers={settings['controller_replicas']} "
        f"swlabssh={settings['swlabssh_replicas']} {pools}"
    )


def _kubectl(*args: str) -> str:
    command = ["kubectl", "--namespace", KUBERNETES_NAMESPACE, *args]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=KUBECTL_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ServerSettingsError(f"kubectl {args[0]} failed: {exc}") from exc
    if result.returncode != 0:
        raise ServerSettingsError(f"kubectl {args[0]} failed: {result.stderr.strip()}")
    return result.stdout


def _kubectl_json(*args: str) -> dict[str, Any]:
    return json.loads(_kubectl(*args, "-o", "json"))


def _labels(pod: dict[str, Any]) -> dict[str, str]:
    return pod["metadata"].get("labels") or {}


def _is_ready(pod: dict[str, Any]) -> bool:
    status = pod.get("status") or {}
    if status.get("phase") != "Running":
        return False
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )


def _containers(deployment: dict[str, Any] | None) -> list[dict[str, Any]]:
    if deployment is None:
        return []
    return deployment["spec"]["template"]["spec"].get("containers") or []


def _image(deployment: dict[str, Any] | None) -> str | None:
    containers = _containers(deployment)
    return containers[0].get("image") if containers else None


def _env(deployment: dict[str, Any] | None, name: str) -> str | None:
    for container in _containers(deployment):
        for item in container.get("env") or []:
            if item.get("name") == name:
                return item.get("value")
    return None


def _replicas(deployment: dict[str, Any] | None) -> int | None:
    if deployment is None:
        return None
    return deployment["spec"].get("replicas")


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
