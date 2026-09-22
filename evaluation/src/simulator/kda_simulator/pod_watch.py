"""Record compute, user and controller pod lifecycles while a run is going.

Resource footprint, warm-pod counts and concurrency are computed from these
records after the run. Kubernetes keeps pod events for about an hour and has no
history for deleted pods, so they have to be captured live.

Each (re)connect writes a SNAPSHOT of the current pods and then follows changes
with a watch; a pod missing from a later snapshot was deleted during the gap.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import KUBERNETES_NAMESPACE

RECONNECT_DELAY_SECONDS = 2.0
STOP_GRACE_SECONDS = 10.0
STREAM_LIMIT_BYTES = 16 * 1024 * 1024
QUANTITY_RE = re.compile(r"^([0-9.]+)([A-Za-z]*)$")
MEMORY_UNITS = {
    "": 1, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
    "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40,
}


class PodRecorder:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._file = None
        self._stopping = False
        self._disabled = False
        self._process: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._file = self._path.open("a", encoding="utf-8")
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop recording. Never raises: the run's own teardown depends on it."""
        self._stopping = True
        self._terminate()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=STOP_GRACE_SECONDS)
            except Exception:  # noqa: BLE001 - a stuck or failed recorder must not block teardown
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
        # A watch may have started in the gap before the flag was seen.
        self._terminate()
        if self._process is not None and self._process.returncode is None:
            try:
                await asyncio.wait_for(self._process.wait(), timeout=STOP_GRACE_SECONDS)
            except asyncio.TimeoutError:
                self._process.kill()
        if not self._disabled:
            # One last look so pods deleted just before the end are closed off.
            try:
                await self._snapshot()
            except Exception as exc:  # noqa: BLE001 - a missing last sample is acceptable
                print(f"Pod recording final snapshot skipped: {exc}")
        if self._file is not None:
            self._file.close()

    def _terminate(self) -> None:
        if self._process is not None and self._process.returncode is None:
            self._process.terminate()

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self._snapshot()
                if self._stopping:
                    return
                await self._watch()
            except FileNotFoundError:
                print("Pod recording disabled: kubectl not found")
                self._disabled = True
                return
            except Exception as exc:  # noqa: BLE001 - a recorder must not end the run
                if not self._stopping:
                    print(f"Pod recording reconnecting: {exc}")
            if not self._stopping:
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def _snapshot(self) -> None:
        process = await asyncio.create_subprocess_exec(
            "kubectl", "--namespace", KUBERNETES_NAMESPACE, "get", "pods", "-o", "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace").strip())
        pods = [summary for item in json.loads(stdout)["items"] if (summary := pod_summary(item))]
        self._write({"type": "SNAPSHOT", "observed_at": _now(), "pods": pods})

    async def _watch(self) -> None:
        self._process = await asyncio.create_subprocess_exec(
            "kubectl", "--namespace", KUBERNETES_NAMESPACE,
            "get", "pods", "--watch-only", "--output-watch-events", "-o", "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            limit=STREAM_LIMIT_BYTES,
        )
        assert self._process.stdout is not None
        decoder = json.JSONDecoder()
        buffer = ""
        while True:
            chunk = await self._process.stdout.read(65536)
            if not chunk:
                break
            buffer += chunk.decode("utf-8", errors="replace")
            while True:
                buffer = buffer.lstrip()
                if not buffer:
                    break
                try:
                    event, end = decoder.raw_decode(buffer)
                except json.JSONDecodeError:
                    break  # the rest of this object has not arrived yet
                buffer = buffer[end:]
                summary = pod_summary(event.get("object") or {})
                if summary:
                    self._write({"type": event.get("type"), "observed_at": _now(), "pod": summary})
        await self._process.wait()

    def _write(self, record: dict[str, Any]) -> None:
        if self._file is None or self._file.closed:
            return
        self._file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        self._file.flush()


def pod_summary(pod: dict[str, Any]) -> dict[str, Any] | None:
    metadata = pod.get("metadata") or {}
    labels = metadata.get("labels") or {}
    group = pod_group(labels)
    if group is None:
        return None
    spec = pod.get("spec") or {}
    status = pod.get("status") or {}
    containers = spec.get("containers") or []
    return {
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
        "group": group,
        "compute_status": labels.get("compute-status"),
        "compute_type": labels.get("compute-type"),
        "node": spec.get("nodeName"),
        "phase": status.get("phase"),
        "ready": any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in status.get("conditions") or []
        ),
        "deleting": bool(metadata.get("deletionTimestamp")),
        "created_at": metadata.get("creationTimestamp"),
        "cpu_limit": _sum_quantity(containers, "limits", "cpu"),
        "cpu_request": _sum_quantity(containers, "requests", "cpu"),
        "memory_limit": _sum_quantity(containers, "limits", "memory"),
        "memory_request": _sum_quantity(containers, "requests", "memory"),
    }


def pod_group(labels: dict[str, str]) -> str | None:
    if labels.get("app") == "compute-pod":
        return "compute"
    if labels.get("kubessh") == "userpods":
        return "user"
    if labels.get("app") == "controller":
        return "controller"
    return None


def parse_cpu(value: str) -> float:
    text = str(value).strip()
    return float(text[:-1]) / 1000.0 if text.endswith("m") else float(text)


def parse_memory(value: str) -> float:
    match = QUANTITY_RE.match(str(value).strip())
    if not match or match.group(2) not in MEMORY_UNITS:
        raise ValueError(f"unsupported memory quantity: {value!r}")
    return float(match.group(1)) * MEMORY_UNITS[match.group(2)]


def _sum_quantity(containers: list[dict[str, Any]], kind: str, resource: str) -> float | None:
    parse = parse_cpu if resource == "cpu" else parse_memory
    values = [
        ((container.get("resources") or {}).get(kind) or {}).get(resource)
        for container in containers
    ]
    present = [parse(value) for value in values if value is not None]
    return sum(present) if present else None


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="milliseconds")
