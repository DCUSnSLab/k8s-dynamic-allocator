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


def prepare_server(
    config: SimulatorConfig,
    allow_short_headroom: bool = False,
) -> dict[str, Any]:
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

    # Read before warm-up, so this is the room the run had before its own pods
    # took any of it. Nested under the server settings so summary.json gains a
    # field without the schema version moving, which would make the runs the
    # other arms recorded unreadable.
    settings["node_headroom"] = read_node_headroom()
    settings["storage_headroom"] = read_storage_headroom()

    print(f"Server: {describe_settings(settings)}")
    print(f"Headroom: {describe_headroom(settings['node_headroom'])}")
    print(f"Storage: {describe_storage(settings['storage_headroom'], users=config.users.count)}")
    require_headroom(
        settings["node_headroom"],
        users=config.users.count,
        capacity=config.experiment.buffer_capacity,
        storage=settings["storage_headroom"],
        allow_short=allow_short_headroom,
        stage="실행 전",
    )
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

    if allocation_mode == "cold_start":
        # Cold start creates a pod per request and allocates from no
        # Deployment at all. A compute Deployment still sitting in the
        # namespace is a leftover from a warm run: its annotations describe a
        # policy that is not in force and its image is not the one these pods
        # ran. Recording those would put the wrong N - and N=0 in particular,
        # which reads as the value that blocks every request - into the run's
        # provenance. Both facts come from the controller instead, which is
        # where this arm actually reads them.
        live = [
            pod for pod in pods
            if not pod["metadata"].get("deletionTimestamp")
        ]
        buffers = [
            {
                "deployment": None,
                "compute_type": "",
                "R": None,
                "N": _int_or_none(_env(controller, "BUFFER_CAPACITY")),
                "available": 0,
                "available_ready": 0,
                "assigned": len(live),
                "image": _cold_start_compute_image(),
            }
        ]

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
    # Cold start has no Deployment and no R, so the warm line would read
    # "None R=None" for both. Only N is meaningful here.
    if settings["allocation_mode"] == "cold_start":
        buffer_text = ", ".join(
            f"N={buffer['N']}" for buffer in settings["buffers"]
        )
    else:
        buffer_text = ", ".join(
            f"{buffer['deployment']} R={buffer['R']} N={buffer['N']} ready={buffer['available_ready']}"
            for buffer in settings["buffers"]
        )
    return (
        f"mode={settings['allocation_mode']} controllers={settings['controller_replicas']} "
        f"swlabssh={settings['swlabssh_replicas']} {buffer_text}"
    )


def _cold_start_compute_image() -> str | None:
    """The compute image this arm creates pods from.

    It is baked into the controller image rather than set on the Deployment,
    so it cannot be read from the spec the way other fields are; the deploy's
    own verification reads it the same way, through the running container.
    Provenance should not fail a run, so an unreadable value is recorded as
    unknown rather than raised.
    """
    try:
        value = run_kubectl(
            "exec",
            "deployment/controller",
            "--",
            "sh",
            "-c",
            'printf "%s" "$COMPUTE_POD_IMAGE"',
        )
    except ServerSettingsError:
        return None
    return value.strip() or None


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


# --- node headroom -------------------------------------------------------------

# Phases in which the scheduler has already handed the pod's request back, so it
# no longer occupies anything a new pod would need.
_RELEASED_PHASES = frozenset({"Succeeded", "Failed"})

_MEMORY_UNITS = (
    ("Ki", 1024),
    ("Mi", 1024 ** 2),
    ("Gi", 1024 ** 3),
    ("Ti", 1024 ** 4),
    ("K", 1000),
    ("M", 1000 ** 2),
    ("G", 1000 ** 3),
    ("T", 1000 ** 4),
)


def _cpu_millis(value: Any) -> int | None:
    """A Kubernetes CPU quantity in millicores: '2' is 2000, '1500m' is 1500."""
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    try:
        if text.endswith("m"):
            return int(float(text[:-1]))
        if text.endswith("n"):
            return int(float(text[:-1]) / 1_000_000)
        if text.endswith("u"):
            return int(float(text[:-1]) / 1_000)
        return int(float(text) * 1000)
    except ValueError:
        return None


def _memory_bytes(value: Any) -> int | None:
    """A Kubernetes memory quantity in bytes: '4Gi' is 4294967296."""
    text = str(value if value is not None else "").strip()
    if not text:
        return None
    for suffix, factor in _MEMORY_UNITS:
        if text.endswith(suffix):
            try:
                return int(float(text[: -len(suffix)]) * factor)
            except ValueError:
                return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _pod_request(pod: dict[str, Any]) -> tuple[int, int]:
    """CPU millicores and bytes the scheduler reserves for one pod.

    Init containers run to completion before the app containers start, so the
    scheduler reserves the larger of the two rather than their sum.
    """
    spec = pod.get("spec") or {}
    cpu = memory = 0
    for container in spec.get("containers") or []:
        requests = (container.get("resources") or {}).get("requests") or {}
        cpu += _cpu_millis(requests.get("cpu")) or 0
        memory += _memory_bytes(requests.get("memory")) or 0
    init_cpu = init_memory = 0
    for container in spec.get("initContainers") or []:
        requests = (container.get("resources") or {}).get("requests") or {}
        init_cpu = max(init_cpu, _cpu_millis(requests.get("cpu")) or 0)
        init_memory = max(init_memory, _memory_bytes(requests.get("memory")) or 0)
    return max(cpu, init_cpu), max(memory, init_memory)


def _compute_pod_shape() -> dict[str, Any]:
    """What one compute pod asks for, and where it is allowed to run.

    Taken from the compute Deployment's pod template rather than a constant in
    here. The template survives cold start scaling the Deployment to zero, so it
    is readable before a run has created anything, and it cannot drift away from
    the server the way a copy in the simulator would.
    """
    shape: dict[str, Any] = {"node_selector": {}, "cpu_m": None, "memory_bytes": None}
    try:
        deployments = _kubectl_json("get", "deployments")["items"]
    except ServerSettingsError:
        return shape
    for deployment in deployments:
        labels = deployment["metadata"].get("labels") or {}
        if labels.get("app") != "compute-pod":
            continue
        spec = deployment["spec"]["template"]["spec"]
        shape["node_selector"] = spec.get("nodeSelector") or {}
        for container in spec.get("containers") or []:
            requests = (container.get("resources") or {}).get("requests") or {}
            shape["cpu_m"] = _cpu_millis(requests.get("cpu"))
            shape["memory_bytes"] = _memory_bytes(requests.get("memory"))
            break
        break
    return shape


def _node_usage() -> dict[str, dict[str, int | None]]:
    """Actual per-node usage, or nothing when the metrics API is unavailable.

    Usage is not a substitute for requests - a node can be 10% busy and still
    refuse a pod - so a missing reading degrades the record rather than the run.
    """
    try:
        text = run_kubectl("top", "nodes", "--no-headers")
    except ServerSettingsError as exc:
        print(f"Node usage not recorded: {exc}")
        return {}
    usage: dict[str, dict[str, int | None]] = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        usage[fields[0]] = {
            "cpu_m": _cpu_millis(fields[1]),
            "memory_bytes": _memory_bytes(fields[3]),
        }
    return usage


def read_node_headroom() -> dict[str, Any]:
    """What the run can have on the nodes its pods are allowed to land on.

    free_for_run adds this run's own pods back to the free figure. Without that,
    the same cluster reads differently before and after warm-up, and a check
    against a fixed requirement would pass at one point and fail at the other.

    The permanent workloads - swlabssh, controller, redis, the log collector -
    stay subtracted, because they are not pods this run creates. They are
    reported separately so that choice is visible in the record.
    """
    shape = _compute_pod_shape()
    selector = shape["node_selector"]
    try:
        nodes = _kubectl_json("get", "nodes")["items"]
        # Every namespace: other tenants hold node capacity this run cannot use.
        pods = json.loads(run_kubectl("get", "pods", "-A", "-o", "json"))["items"]
    except ServerSettingsError as exc:
        print(f"Node headroom not recorded: {exc}")
        return {"error": str(exc)}

    reserved: dict[str, list[int]] = {}
    ours: dict[str, list[int]] = {}
    user_pods: list[dict[str, Any]] = []
    for pod in pods:
        node_name = (pod.get("spec") or {}).get("nodeName")
        if not node_name:
            continue
        if (pod.get("status") or {}).get("phase") in _RELEASED_PHASES:
            continue
        cpu, memory = _pod_request(pod)
        entry = reserved.setdefault(node_name, [0, 0, 0])
        entry[0] += cpu
        entry[1] += memory
        entry[2] += 1
        # This run's own pods, and only in its own namespace: the production user
        # pods in swlabpods carry the same label and are not ours to reclaim.
        if pod["metadata"].get("namespace") != KUBERNETES_NAMESPACE:
            continue
        labels = _labels(pod)
        is_user = labels.get("kubessh") == "userpods"
        is_compute = labels.get("app") == "compute-pod"
        if not (is_user or is_compute):
            continue
        if is_user and not pod["metadata"].get("deletionTimestamp"):
            user_pods.append(pod)
        mine = ours.setdefault(node_name, [0, 0, 0])
        mine[0] += cpu
        mine[1] += memory
        mine[2] += 1

    usage = _node_usage()
    rows: list[dict[str, Any]] = []
    for node in nodes:
        name = node["metadata"]["name"]
        labels = node["metadata"].get("labels") or {}
        spec = node.get("spec") or {}
        allocatable = node["status"].get("allocatable") or {}
        cpu_alloc = _cpu_millis(allocatable.get("cpu")) or 0
        memory_alloc = _memory_bytes(allocatable.get("memory")) or 0
        cpu_res, memory_res, pod_count = reserved.get(name, [0, 0, 0])
        cpu_ours, memory_ours, ours_count = ours.get(name, [0, 0, 0])
        # A node the compute pods may not land on has no headroom for this run,
        # however idle it looks.
        eligible = bool(selector) and all(
            labels.get(key) == value for key, value in selector.items()
        )
        rows.append(
            {
                "name": name,
                "eligible": eligible,
                "schedulable": not bool(spec.get("unschedulable")),
                "taints": [taint.get("key") for taint in spec.get("taints") or []],
                "pods": pod_count,
                "our_pods": ours_count,
                "cpu_allocatable_m": cpu_alloc,
                "cpu_requested_m": cpu_res,
                "cpu_free_m": cpu_alloc - cpu_res,
                "cpu_ours_m": cpu_ours,
                "cpu_free_for_run_m": cpu_alloc - cpu_res + cpu_ours,
                "cpu_used_m": (usage.get(name) or {}).get("cpu_m"),
                "memory_allocatable_bytes": memory_alloc,
                "memory_requested_bytes": memory_res,
                "memory_free_bytes": memory_alloc - memory_res,
                "memory_ours_bytes": memory_ours,
                "memory_free_for_run_bytes": memory_alloc - memory_res + memory_ours,
                "memory_used_bytes": (usage.get(name) or {}).get("memory_bytes"),
            }
        )

    usable = [row for row in rows if row["eligible"] and row["schedulable"]]
    return {
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "node_selector": selector,
        "compute_pod_request": {
            "cpu_m": shape["cpu_m"],
            "memory_bytes": shape["memory_bytes"],
        },
        # kubessh hardcodes this inside the swlabssh image rather than taking it
        # from a config or a template, so it is only knowable from a live pod.
        "user_pod_request": _user_pod_request(user_pods),
        "user_pods_live": len(user_pods),
        "usage_available": bool(usage),
        "nodes": sorted(rows, key=lambda row: row["name"]),
        "eligible": {
            "nodes": len(usable),
            "cpu_free_m": sum(row["cpu_free_m"] for row in usable),
            "memory_free_bytes": sum(row["memory_free_bytes"] for row in usable),
            "cpu_free_for_run_m": sum(row["cpu_free_for_run_m"] for row in usable),
            "memory_free_for_run_bytes": sum(
                row["memory_free_for_run_bytes"] for row in usable
            ),
            "cpu_ours_m": sum(row["cpu_ours_m"] for row in usable),
            "memory_ours_bytes": sum(row["memory_ours_bytes"] for row in usable),
        },
    }


def _fits(rows: list[dict[str, Any]], cpu_m: int, memory_bytes: int) -> int:
    """How many pods of that size the nodes can take, counted node by node.

    A two-core pod needs two cores free on one node, so this is a per-node floor
    summed up. Dividing the totals would count room no single pod can use.
    """
    total = 0
    for row in rows:
        total += max(
            0,
            min(
                row["cpu_free_for_run_m"] // cpu_m,
                row["memory_free_for_run_bytes"] // memory_bytes,
            ),
        )
    return total


def plan_requirement(
    headroom: dict[str, Any],
    *,
    users: int,
    capacity: int | None,
) -> dict[str, Any]:
    """What this run needs on the eligible nodes, taken from its config.

    Fixed by the config rather than by what is currently deployed, so it does not
    move between the pre-flight and the post-warm-up check.
    """
    user = headroom.get("user_pod_request") or {}
    compute = headroom.get("compute_pod_request") or {}
    parts: list[dict[str, Any]] = []
    if user.get("cpu_m") is not None:
        parts.append({"kind": "user pod", "count": users, **user})
    if capacity and compute.get("cpu_m") is not None:
        parts.append({"kind": "compute pod", "count": capacity, **compute})
    return {
        "parts": parts,
        "cpu_m": sum(p["count"] * p["cpu_m"] for p in parts),
        "memory_bytes": sum(p["count"] * (p["memory_bytes"] or 0) for p in parts),
        "unchecked": [
            name
            for name, known in (
                ("user pod", user.get("cpu_m") is not None),
                ("compute pod", not capacity or compute.get("cpu_m") is not None),
            )
            if not known
        ],
    }


def headroom_shortfall(
    headroom: dict[str, Any],
    *,
    users: int,
    capacity: int | None,
) -> list[str]:
    """Why this run will not fit, or an empty list when it will.

    Only figures that were actually read are checked; a missing one produces an
    "unchecked" note from plan_requirement rather than a silent pass, because a
    silent pass on a missing number is the failure this check exists to prevent.
    """
    if not headroom or "error" in headroom:
        return []

    totals = headroom["eligible"]
    if totals["nodes"] == 0:
        return [
            f"파드를 올릴 수 있는 노드가 없음 (nodeSelector {headroom.get('node_selector')})"
        ]

    need = plan_requirement(headroom, users=users, capacity=capacity)
    reasons: list[str] = []
    cpu_free = totals["cpu_free_for_run_m"]
    memory_free = totals["memory_free_for_run_bytes"]
    if need["cpu_m"] > cpu_free or need["memory_bytes"] > memory_free:
        breakdown = " + ".join(
            f"{p['kind']} {p['count']}개({p['count'] * p['cpu_m'] / 1000:.0f}코어/"
            f"{p['count'] * (p['memory_bytes'] or 0) / 1024 ** 3:.0f}Gi)"
            for p in need["parts"]
        )
        reasons.append(
            f"필요 {need['cpu_m'] / 1000:.0f}코어/"
            f"{need['memory_bytes'] / 1024 ** 3:.0f}Gi "
            f"= {breakdown}, 그런데 쓸 수 있는 여유는 "
            f"{cpu_free / 1000:.0f}코어/{memory_free / 1024 ** 3:.0f}Gi "
            f"(노드 {totals['nodes']}개)"
        )

    # Totals can be enough while no single node can take one pod. Only the
    # compute pods are large enough for that to bite.
    compute = headroom.get("compute_pod_request") or {}
    if capacity and compute.get("cpu_m") and compute.get("memory_bytes"):
        usable = [r for r in headroom["nodes"] if r["eligible"] and r["schedulable"]]
        fits = _fits(usable, compute["cpu_m"], compute["memory_bytes"])
        if fits < capacity:
            reasons.append(
                f"compute 파드 {capacity}개가 필요한데 노드별로 나눠 담으면 "
                f"{fits}개만 들어감 (1개당 {compute['cpu_m'] / 1000:.1f}코어/"
                f"{compute['memory_bytes'] / 1024 ** 3:.1f}Gi)"
            )
    return reasons


def _headroom_note(headroom: dict[str, Any], users: int, capacity: int | None) -> str:
    """What the check could not look at, so a pass is never read as a full pass."""
    if not headroom or "error" in headroom:
        return "여유 검사 생략: 노드 정보를 읽지 못함"
    unchecked = plan_requirement(headroom, users=users, capacity=capacity)["unchecked"]
    if not unchecked:
        return ""
    detail = {
        "user pod": f"사용자 파드 {users}개분 (요청량은 파드가 떠 있을 때만 읽힘)",
        "compute pod": "compute 파드 요청량",
    }
    return "여유 검사에서 빠진 항목: " + ", ".join(detail[name] for name in unchecked)


def require_headroom(
    headroom: dict[str, Any],
    *,
    users: int,
    capacity: int | None,
    storage: dict[str, Any] | None = None,
    allow_short: bool = False,
    stage: str,
) -> None:
    """Stop the run when the cluster cannot hold what its config asks for.

    Raises ServerSettingsError, which main turns into a SystemExit, so a refusal
    reads like any other failed pre-flight rather than a crash.

    storage is optional because it is only worth asking before the run starts:
    once warm-up has finished, the claims it needed are already bound.
    """
    note = _headroom_note(headroom, users, capacity)
    if note:
        print(note)
    reasons = headroom_shortfall(headroom, users=users, capacity=capacity)
    if storage is not None:
        reasons += storage_shortfall(storage, users=users)
        if "error" in storage:
            print(f"PVC 검사 생략: {storage['error']}")
        elif not storage.get("claim_size_bytes"):
            print("PVC 검사에서 빠진 항목: 사용자 PVC 크기 "
                  "(기존 PVC 가 있을 때만 읽힘)")
    if not reasons:
        return
    detail = "; ".join(reasons)
    if allow_short:
        print(f"[Warning] 여유 부족을 무시하고 진행 ({stage}): {detail}")
        return
    raise ServerSettingsError(
        f"클러스터 여유가 부족해 실행하지 않음 ({stage}): {detail}"
        " — 그래도 돌리려면 --ignore-headroom"
    )


def _user_pod_request(user_pods: list[dict[str, Any]]) -> dict[str, int | None] | None:
    """What one user pod reserves, read from a live one, or None when none exist."""
    for pod in user_pods:
        cpu, memory = _pod_request(pod)
        if cpu or memory:
            return {"cpu_m": cpu, "memory_bytes": memory}
    return None


def describe_headroom(headroom: dict[str, Any]) -> str:
    """One line: what the run can have, and what it is already holding."""
    if not headroom or "error" in headroom:
        return f"unavailable ({(headroom or {}).get('error')})"
    totals = headroom["eligible"]
    usable = [r for r in headroom["nodes"] if r["eligible"] and r["schedulable"]]
    used = [r["cpu_used_m"] for r in usable if r["cpu_used_m"] is not None]
    actual = f" actual={sum(used) / 1000:.1f} cores" if used else ""
    return (
        f"nodes={totals['nodes']} "
        f"for-run={totals['cpu_free_for_run_m'] / 1000:.0f} cores/"
        f"{totals['memory_free_for_run_bytes'] / 1024 ** 3:.0f}Gi "
        f"(ours={totals['cpu_ours_m'] / 1000:.0f} cores held now)"
        f"{actual}"
    )


# --- storage headroom ----------------------------------------------------------

# Longhorn's defaults, used when its settings cannot be read. Being wrong here
# makes the check more permissive, never less, which is the safe direction for a
# gate.
LONGHORN_NAMESPACE = "longhorn-system"
LONGHORN_DEFAULT_OVER_PROVISIONING = 100
LONGHORN_DEFAULT_MINIMAL_AVAILABLE = 25
LONGHORN_PROVISIONER = "driver.longhorn.io"


def _longhorn_setting(name: str, default: int) -> int:
    try:
        raw = run_kubectl(
            "--namespace", LONGHORN_NAMESPACE,
            "get", f"settings.longhorn.io/{name}",
            "-o", "jsonpath={.value}",
        )
    except ServerSettingsError:
        return default
    value = _int_or_none(raw)
    return default if value is None else value


def _user_storage_class() -> str | None:
    """The class the user PVCs are created with.

    swlabssh carries it in its own environment, so unlike the claim's size this
    is readable before any claim exists. kubessh falls back to the template's
    own class when the variable is unset, so an unset value is not an answer.
    """
    try:
        deployments = _kubectl_json("get", "deployments")["items"]
    except ServerSettingsError:
        return None
    for deployment in deployments:
        if deployment["metadata"]["name"] != "swlabssh":
            continue
        value = (_env(deployment, "USER_POD_STORAGE_CLASS") or "").strip()
        return value or None
    return None


def _user_claims() -> list[dict[str, Any]]:
    try:
        claims = _kubectl_json("get", "pvc")["items"]
    except ServerSettingsError:
        return []
    return [c for c in claims if c["metadata"]["name"].startswith("ssh-")]


def read_storage_headroom() -> dict[str, Any]:
    """Whether the user PVCs this run needs can be provisioned.

    Everything the answer rests on is reported alongside it, because the figures
    come from three places - the storage class, Longhorn's settings, and its node
    records - and a bare verdict would be impossible to argue with.
    """
    claims = _user_claims()
    class_name = _user_storage_class()
    if class_name is None and claims:
        class_name = claims[0]["spec"].get("storageClassName")
    sizes = {c["spec"]["resources"]["requests"]["storage"] for c in claims}
    result: dict[str, Any] = {
        "read_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "storage_class": class_name,
        "existing_user_claims": len(claims),
        # One claim's size. Hardcoded in the swlabssh image, so only knowable
        # from a claim that already exists.
        "claim_size_bytes": _memory_bytes(sorted(sizes)[0]) if len(sizes) == 1 else None,
        "claim_sizes_seen": sorted(sizes),
    }
    if not class_name:
        result["error"] = "사용자 PVC 의 storageClass 를 알 수 없음"
        return result

    try:
        storage_class = _kubectl_json("get", f"storageclass/{class_name}")
    except ServerSettingsError as exc:
        result["error"] = str(exc)
        return result
    parameters = storage_class.get("parameters") or {}
    result["provisioner"] = storage_class.get("provisioner")
    result["replicas"] = _int_or_none(parameters.get("numberOfReplicas")) or 1
    result["node_tag"] = (parameters.get("nodeSelector") or "").strip() or None

    if result["provisioner"] != LONGHORN_PROVISIONER:
        # Another provisioner has its own capacity model; saying so beats
        # applying Longhorn's arithmetic to it.
        result["error"] = f"용량 계산 방식을 모르는 프로비저너: {result['provisioner']}"
        return result

    over = _longhorn_setting("storage-over-provisioning-percentage",
                             LONGHORN_DEFAULT_OVER_PROVISIONING)
    minimal = _longhorn_setting("storage-minimal-available-percentage",
                                LONGHORN_DEFAULT_MINIMAL_AVAILABLE)
    result["over_provisioning_percent"] = over
    result["minimal_available_percent"] = minimal
    try:
        lh_nodes = json.loads(
            run_kubectl("get", "nodes.longhorn.io", "-A", "-o", "json")
        )["items"]
    except ServerSettingsError as exc:
        result["error"] = str(exc)
        return result

    rows: list[dict[str, Any]] = []
    for node in lh_nodes:
        name = node["metadata"]["name"]
        spec = node.get("spec") or {}
        tags = spec.get("tags") or []
        eligible = result["node_tag"] is None or result["node_tag"] in tags
        allowed = bool(spec.get("allowScheduling"))
        room = 0
        maximum = available = scheduled = 0
        for disk in ((node.get("status") or {}).get("diskStatus") or {}).values():
            disk_max = disk.get("storageMaximum") or 0
            disk_avail = disk.get("storageAvailable") or 0
            disk_sched = disk.get("storageScheduled") or 0
            maximum += disk_max
            available += disk_avail
            scheduled += disk_sched
            # Longhorn admits a replica only if both hold.
            by_commitment = disk_max * over // 100 - disk_sched
            by_fill = disk_avail - disk_max * minimal // 100
            room += max(0, min(by_commitment, by_fill))
        rows.append({
            "name": name,
            "tags": tags,
            "eligible": eligible,
            "allow_scheduling": allowed,
            "storage_maximum_bytes": maximum,
            "storage_available_bytes": available,
            "storage_scheduled_bytes": scheduled,
            "schedulable_bytes": room if (eligible and allowed) else 0,
        })

    usable = [r for r in rows if r["eligible"] and r["allow_scheduling"]]
    result["nodes"] = sorted(rows, key=lambda r: r["name"])
    result["eligible"] = {
        "nodes": len(usable),
        "schedulable_bytes": sum(r["schedulable_bytes"] for r in usable),
    }
    return result


def storage_shortfall(storage: dict[str, Any], *, users: int) -> list[str]:
    """Why the user PVCs cannot be created, or an empty list when they can.

    A necessary condition rather than a placement decision: when it fails the run
    definitely cannot be provisioned, and when it passes Longhorn still has the
    final say. A gate that has to guess should guess towards starting.
    """
    if not storage or "error" in storage:
        return []

    totals = storage.get("eligible") or {}
    replicas = storage.get("replicas") or 1
    # Hard anti-affinity: the replicas of one volume must sit on distinct nodes,
    # so too few eligible nodes blocks every volume regardless of free space.
    if totals.get("nodes", 0) < replicas:
        return [
            f"복제 {replicas}개가 서로 다른 노드에 있어야 하는데 "
            f"{storage.get('storage_class')} 가 쓸 수 있는 노드는 "
            f"{totals.get('nodes', 0)}개 "
            f"(태그 {storage.get('node_tag')!r})"
        ]

    size = storage.get("claim_size_bytes")
    if not size:
        return []
    # A claim that already exists is disk already committed; the run only has to
    # create the rest.
    pending = max(0, users - int(storage.get("existing_user_claims") or 0))
    if not pending:
        return []
    need = pending * size * replicas
    room = totals.get("schedulable_bytes", 0)
    if need > room:
        g = 1024 ** 3
        return [
            f"사용자 PVC {pending}개에 {need / g:.0f}Gi 가 필요한데 "
            f"({size / g:.0f}Gi x 복제 {replicas}) "
            f"{storage.get('storage_class')} 가 배치할 수 있는 양은 {room / g:.0f}Gi"
        ]
    return []


def describe_storage(storage: dict[str, Any], *, users: int) -> str:
    if not storage:
        return "storage  not checked"
    if "error" in storage:
        return f"storage  unavailable ({storage['error']})"
    g = 1024 ** 3
    totals = storage["eligible"]
    size = storage.get("claim_size_bytes")
    pending = max(0, users - int(storage.get("existing_user_claims") or 0))
    need = (
        f"need {pending * size * storage['replicas'] / g:.0f}Gi for {pending} new"
        if size else "size unknown"
    )
    return (
        f"storage  {storage['storage_class']} x{storage['replicas']}  "
        f"{totals['nodes']} nodes  room {totals['schedulable_bytes'] / g:.0f}Gi  "
        f"{storage['existing_user_claims']} claims exist  {need}"
    )
