"""Align R/N on the server before a run and record what the server was running.

R/N are Deployment annotations the controller picks up without a restart, and
Jenkins resets them on every deploy, so a run sets them itself. Controller
count, allocation mode and images are only read: changing those needs a redeploy.
"""

from __future__ import annotations

import json
import subprocess
import time
from typing import Any

from .config import KUBERNETES_NAMESPACE, SimulatorConfig


BUFFER_RESERVE_ANNOTATION = "k8s-dynamic-allocator/buffer-reserve"
BUFFER_CAPACITY_ANNOTATION = "k8s-dynamic-allocator/buffer-capacity"
KUBECTL_TIMEOUT_SECONDS = 30
BUFFER_SETTLE_TIMEOUT_SECONDS = 180.0
BUFFER_SETTLE_POLL_SECONDS = 2.0


class ServerSettingsError(RuntimeError):
    pass


def prepare_server(config: SimulatorConfig) -> dict[str, Any]:
    """Apply R/N from the config when both are set, then return the server settings."""
    wanted = (config.experiment.buffer_reserve, config.experiment.buffer_capacity)
    apply_policy = None not in wanted
    if not apply_policy and wanted != (None, None):
        print("R/N not applied: set both experiment.buffer_reserve and experiment.buffer_capacity")

    try:
        settings = read_server_settings()
    except ServerSettingsError as exc:
        if apply_policy:
            raise
        print(f"Server settings not recorded: {exc}")
        return {"error": str(exc)}

    if apply_policy:
        if settings["allocation_mode"] != "warm_buffer":
            print(f"R/N not applied: allocation mode is {settings['allocation_mode']}")
        else:
            r, n = wanted
            for buffer in settings["buffers"]:
                if (buffer["R"], buffer["N"]) != (r, n):
                    print(f"Buffer policy {buffer['deployment']}: R={buffer['R']} N={buffer['N']} -> R={r} N={n}")
                    run_kubectl(
                        "annotate",
                        f"deployment/{buffer['deployment']}",
                        f"{BUFFER_RESERVE_ANNOTATION}={r}",
                        f"{BUFFER_CAPACITY_ANNOTATION}={n}",
                        "--overwrite",
                    )
            wait_for_buffers(r, n)
            settings = read_server_settings()

    print(f"Server: {describe_settings(settings)}")
    return settings


def read_server_settings() -> dict[str, Any]:
    deployments = {item["metadata"]["name"]: item for item in _kubectl_json("get", "deployments")["items"]}
    pods = _kubectl_json("get", "pods", "-l", "app=compute-pod")["items"]
    controller = deployments.get("controller")
    swlabssh = deployments.get("swlabssh")

    buffers = []
    for name, deployment in sorted(deployments.items()):
        labels = deployment["metadata"].get("labels") or {}
        if labels.get("app") != "compute-pod":
            continue
        compute_type = labels.get("compute-type", "")
        annotations = deployment["metadata"].get("annotations") or {}
        members = [
            pod for pod in pods
            if _labels(pod).get("compute-type") == compute_type
            and not pod["metadata"].get("deletionTimestamp")
        ]
        available = [pod for pod in members if _labels(pod).get("compute-status") == "available"]
        buffers.append(
            {
                "deployment": name,
                "compute_type": compute_type,
                "R": _int_or_none(annotations.get(BUFFER_RESERVE_ANNOTATION)),
                "N": _int_or_none(annotations.get(BUFFER_CAPACITY_ANNOTATION)),
                "available": len(available),
                "available_ready": sum(1 for pod in available if _is_ready(pod)),
                "assigned": sum(1 for pod in members if _labels(pod).get("compute-status") == "assigned"),
                "image": _image(deployment),
            }
        )

    allocation_mode = None
    if controller is not None:
        # Same default and normalization as the controller's COMPUTE_ALLOCATION_MODE.
        allocation_mode = (_env(controller, "COMPUTE_ALLOCATION_MODE") or "warm_buffer").strip().lower().replace("-", "_")

    return {
        "namespace": KUBERNETES_NAMESPACE,
        "allocation_mode": allocation_mode,
        "controller_replicas": _replicas(controller),
        "controller_image": _image(controller),
        "swlabssh_replicas": _replicas(swlabssh),
        "swlabssh_image": _image(swlabssh),
        "buffers": buffers,
    }


def wait_for_buffers(r: int, n: int) -> None:
    """Wait until every buffer holds the ready pods R/N asks for."""
    started = time.monotonic()
    while True:
        pending = [b for b in read_server_settings()["buffers"] if not _settled(b, r, n)]
        waited = time.monotonic() - started
        if not pending:
            if waited >= BUFFER_SETTLE_POLL_SECONDS:
                print(f"Warm buffer ready after {waited:.0f}s")
            return
        if waited >= BUFFER_SETTLE_TIMEOUT_SECONDS:
            buffer = pending[0]
            raise ServerSettingsError(
                f"{buffer['deployment']} did not reach {_desired(buffer, r, n)} ready pods "
                f"within {BUFFER_SETTLE_TIMEOUT_SECONDS:.0f}s "
                f"(available={buffer['available']}, ready={buffer['available_ready']})"
            )
        time.sleep(BUFFER_SETTLE_POLL_SECONDS)


def _desired(buffer: dict[str, Any], r: int, n: int) -> int:
    # Same formula as the controller's capacity reconciler.
    return min(r, max(0, n - buffer["assigned"]))


def _settled(buffer: dict[str, Any], r: int, n: int) -> bool:
    desired = _desired(buffer, r, n)
    return buffer["available"] == desired and buffer["available_ready"] == desired


def wait_for_current_policy() -> None:
    """Wait using the R/N already on the server, for callers that change neither."""
    pools = read_server_settings()["buffers"]
    policies = {(buffer["R"], buffer["N"]) for buffer in pools if None not in (buffer["R"], buffer["N"])}
    for r, n in policies:
        wait_for_buffers(r, n)


def describe_settings(settings: dict[str, Any]) -> str:
    buffer_text = ", ".join(
        f"{buffer['deployment']} R={buffer['R']} N={buffer['N']} ready={buffer['available_ready']}"
        for buffer in settings["buffers"]
    )
    return (
        f"mode={settings['allocation_mode']} controllers={settings['controller_replicas']} "
        f"swlabssh={settings['swlabssh_replicas']} {buffer_text}"
    )


def run_kubectl(
    *args: str,
    timeout: float = KUBECTL_TIMEOUT_SECONDS,
    allow_missing: bool = False,
) -> str:
    """Run kubectl in the experiment namespace and return stdout.

    allow_missing swallows the "no matching resources" failure that commands
    like `wait --for=delete` report when the thing is already gone, which is
    the outcome the caller wanted.
    """
    command = ["kubectl", "--namespace", KUBERNETES_NAMESPACE, *args]
    label = " ".join(args[:2])
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ServerSettingsError(f"kubectl {label} failed: {exc}") from exc
    if result.returncode != 0:
        if allow_missing and "no matching resources" in result.stderr.lower():
            return ""
        raise ServerSettingsError(f"kubectl {label} failed: {result.stderr.strip()}")
    return result.stdout


def _kubectl_json(*args: str) -> dict[str, Any]:
    return json.loads(run_kubectl(*args, "-o", "json"))


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
