#!/usr/bin/env python3
"""Compute the common experiment metrics for one simulator run.

Inputs, all under evaluation/data/<run-id>/:
- simulator/summary.json   experiment window, user count, server R/N
- simulator/requests.jsonl per-request start delay and status
- simulator/pods.jsonl     pod lifecycle records (resource footprint, warm pods)
- log_analysis/raw-jsonl/  controller logs from export_experiment_logs.py (optional)

Writes analysis/metrics.json and prints a short report. With --bucket-minutes the
same metrics are also split into fixed windows, e.g. one per load step; a request
belongs to the window its schedule falls in, not the one it ran in.
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

# The simulator already defines how a percentile is computed, and summary.json
# is written with it. Reusing it here keeps the two files from reporting
# slightly different p95s for the same run.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))
from kda_simulator.metrics import summarize_values  # noqa: E402

GIB = 2**30
CONTROLLER_EVENT_RE = re.compile(r"\] \[(?P<label>[^\]]+)\] \[(?P<event>Request|Assigned)\] (?P<rest>.*)")
REQUEST_ID_RE = re.compile(r"request_id=(?P<request_id>[^\s;'\"]+)")
KEY_VALUE_RE = re.compile(r"(?P<key>[a-z_]+)=(?P<value>\S+)")


@dataclass
class PodLife:
    name: str
    group: str
    cpu_limit: float
    memory_limit: float
    start: datetime
    ready_at: datetime | None = None
    assigned_at: datetime | None = None
    deleting_at: datetime | None = None
    gone_at: datetime | None = None


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute experiment metrics for one run directory.")
    parser.add_argument("run_dir", help="evaluation/data/<run-id> (the folder that contains simulator/)")
    parser.add_argument("--logs-dir", help="Controller JSONL logs. Default: <run_dir>/log_analysis/raw-jsonl")
    parser.add_argument("--bucket-minutes", type=float, help="Also report metrics per window of this length.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    simulator_dir = run_dir / "simulator"
    summary = json.loads((simulator_dir / "summary.json").read_text(encoding="utf-8"))
    window = (_parse_time(summary["experiment_started_at"]), _parse_time(summary["experiment_finished_at"]))
    users = int(summary["users"]["count"])
    requests = _read_jsonl(simulator_dir / "requests.jsonl")

    pods_path = simulator_dir / "pods.jsonl"
    pod_records = _read_jsonl(pods_path) if pods_path.exists() else []
    if pods_path.exists() and not pod_records:
        # An empty file means recording failed; zero resources would read as a result.
        print(f"WARNING: {pods_path.name} is empty, pod metrics are skipped")
    pods = build_pod_lives(pod_records, window[1]) if pod_records else None
    logs_dir = Path(args.logs_dir) if args.logs_dir else run_dir / "log_analysis" / "raw-jsonl"
    assignments = read_assignments(logs_dir, {r["request_id"] for r in requests}) if logs_dir.exists() else None

    metrics: dict[str, Any] = {
        "run": run_dir.name,
        "window": {"start": window[0].isoformat(), "end": window[1].isoformat(),
                   "minutes": _seconds(*window) / 60.0},
        "users": users,
        "server_pools": [
            {"deployment": p.get("deployment"), "R": p.get("R"), "N": p.get("N")}
            for p in (summary.get("server") or {}).get("pools") or []
        ],
        "sources": {"pods": pods is not None, "controller_logs": assignments is not None},
        "overall": window_metrics(window, requests, pods, assignments, users, summary),
    }
    if args.bucket_minutes:
        step = timedelta(minutes=args.bucket_minutes)
        buckets = []
        start = window[0]
        while start < window[1]:
            bucket = (start, min(start + step, window[1]))
            buckets.append({"start_minute": _seconds(window[0], bucket[0]) / 60.0,
                            **window_metrics(bucket, requests, pods, assignments, users, summary)})
            start += step
        metrics["buckets"] = buckets

    out_path = run_dir / "analysis" / "metrics.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print_report(metrics)
    print(f"\nmetrics: {out_path}")
    return 0


def window_metrics(
    window: tuple[datetime, datetime],
    requests: list[dict[str, Any]],
    pods: list[PodLife] | None,
    assignments: dict[str, dict[str, float]] | None,
    users: int,
    summary: dict[str, Any],
) -> dict[str, Any]:
    run_start = _parse_time(summary["experiment_started_at"])
    in_window = [
        r for r in requests
        if window[0] <= run_start + timedelta(seconds=float(r["planned_offset_seconds"])) < window[1]
    ]
    result: dict[str, Any] = {"requests": request_metrics(in_window)}
    if assignments is not None:
        result["controller"] = assignment_metrics([assignments[r["request_id"]] for r in in_window
                                                   if r["request_id"] in assignments])
    if pods is not None:
        result["pods"] = pod_metrics(pods, window, users)
    return result


def request_metrics(requests: list[dict[str, Any]]) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    for r in requests:
        statuses[r.get("status") or "unknown"] = statuses.get(r.get("status") or "unknown", 0) + 1
    success = [r for r in requests if r.get("status") == "success"]
    return {
        "count": len(requests),
        "status": statuses,
        # ssh_error never reached the server, so it is not a server failure.
        "server_failures": sum(n for s, n in statuses.items() if s not in ("success", "ssh_error")),
        "start_delay_s": summarize_values([r["until_start_ms"] / 1000.0 for r in success if r.get("until_start_ms") is not None]),
        "command_s": summarize_values([r["command_ms"] / 1000.0 for r in success if r.get("command_ms") is not None]),
        "ssh_retried": sum(1 for r in requests if (r.get("ssh_attempts") or 1) > 1),
    }


def assignment_metrics(items: list[dict[str, float]]) -> dict[str, Any]:
    waited = [item["compute_wait_ms"] for item in items if item.get("compute_wait_ms", 0) > 0]
    return {
        "assigned": len(items),
        # Requests that found no warm pod and waited for one.
        "compute_wait_ratio": (len(waited) / len(items)) if items else None,
        "compute_wait_s": summarize_values([ms / 1000.0 for ms in waited]),
        "queue_wait_s": summarize_values([item["queue_wait_ms"] / 1000.0 for item in items if "queue_wait_ms" in item]),
        "total_assignment_s": summarize_values(
            [item["total_assignment_ms"] / 1000.0 for item in items if "total_assignment_ms" in item]
        ),
    }


def pod_metrics(pods: list[PodLife], window: tuple[datetime, datetime], users: int) -> dict[str, Any]:
    seconds = _seconds(*window)
    result: dict[str, Any] = {}
    for group in ("compute", "user"):
        members = [p for p in pods if p.group == group]
        cpu = sum(p.cpu_limit * _overlap(p.start, _end(p, window), window) for p in members) / seconds
        memory = sum(p.memory_limit * _overlap(p.start, _end(p, window), window) for p in members) / seconds
        result[group] = {
            # Time-averaged resources held, counted at limits: what is guaranteed to users.
            "cpu_cores_avg": cpu,
            "memory_gib_avg": memory / GIB,
            "cpu_cores_per_user": cpu / users if users else None,
            "memory_gib_per_user": memory / GIB / users if users else None,
        }
    compute = [p for p in pods if p.group == "compute"]
    result["compute_pods"] = _level(
        [(p.start, p.deleting_at or _end(p, window)) for p in compute], window
    )
    result["available_pods"] = _level(
        [(p.ready_at, min(t for t in (p.assigned_at, p.deleting_at, _end(p, window)) if t is not None))
         for p in compute if p.ready_at is not None],
        window,
    )
    result["assigned_pods"] = _level(
        [(p.assigned_at, p.deleting_at or _end(p, window)) for p in compute if p.assigned_at is not None],
        window,
    )
    return result


def build_pod_lives(records: Iterable[dict[str, Any]], run_end: datetime) -> list[PodLife]:
    lives: dict[str, PodLife] = {}

    def observe(pod: dict[str, Any], at: datetime) -> PodLife:
        life = lives.get(pod["uid"])
        if life is None:
            life = PodLife(
                name=pod["name"],
                group=pod["group"],
                cpu_limit=float(pod.get("cpu_limit") or 0.0),
                memory_limit=float(pod.get("memory_limit") or 0.0),
                start=at,
            )
            lives[pod["uid"]] = life
        if pod.get("ready") and life.ready_at is None:
            life.ready_at = at
        if pod.get("pool_status") == "assigned" and life.assigned_at is None:
            life.assigned_at = at
        if pod.get("deleting") and life.deleting_at is None:
            life.deleting_at = at
        return life

    def gone(life: PodLife, at: datetime) -> None:
        if life.gone_at is None:
            life.gone_at = at
            life.deleting_at = life.deleting_at or at

    for record in sorted(records, key=lambda item: item["observed_at"]):
        at = _parse_time(record["observed_at"])
        if record["type"] == "SNAPSHOT":
            present = set()
            for pod in record["pods"]:
                observe(pod, at)
                present.add(pod["uid"])
            # Deleted while the watch was reconnecting.
            for uid, life in lives.items():
                if uid not in present:
                    gone(life, at)
        elif record.get("pod"):
            life = observe(record["pod"], at)
            if record["type"] == "DELETED":
                gone(life, at)
    return list(lives.values())


def read_assignments(logs_dir: Path, request_ids: set[str]) -> dict[str, dict[str, float]]:
    """Controller [Assigned] fields per simulator request_id."""
    label_to_request: dict[str, str] = {}
    assigned: dict[str, dict[str, float]] = {}
    for path in sorted(logs_dir.glob("*.jsonl")) + sorted(logs_dir.glob("*.jsonl.gz")):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if '"controller"' not in line or ("[Request]" not in line and "[Assigned]" not in line):
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                match = CONTROLLER_EVENT_RE.search(record.get("log") or "")
                if not match:
                    continue
                label, event, rest = match.group("label", "event", "rest")
                if event == "Request":
                    found = REQUEST_ID_RE.search(rest)
                    if found and found.group("request_id") in request_ids:
                        label_to_request[label] = found.group("request_id")
                else:
                    assigned[label] = {
                        key: float(value)
                        for key, value in KEY_VALUE_RE.findall(rest)
                        if key.endswith("_ms") and _is_number(value)
                    }
    return {request: assigned[label] for label, request in label_to_request.items() if label in assigned}


def print_report(metrics: dict[str, Any]) -> None:
    overall = metrics["overall"]
    req = overall["requests"]
    print(f"run {metrics['run']}  {metrics['window']['minutes']:.1f} min  users={metrics['users']}  "
          f"pools={metrics['server_pools']}")
    print(f"requests {req['count']}  status={req['status']}  server_failures={req['server_failures']}")
    print(f"start delay s  {_fmt(req['start_delay_s'])}")
    if "controller" in overall:
        ctl = overall["controller"]
        ratio = ctl["compute_wait_ratio"]
        print(f"waited for a warm pod  {ratio:.1%}  wait s {_fmt(ctl['compute_wait_s'])}" if ratio is not None
              else "waited for a warm pod  n/a")
        print(f"queue wait s  {_fmt(ctl['queue_wait_s'])}")
    if "pods" in overall:
        pods = overall["pods"]
        for group in ("compute", "user"):
            g = pods[group]
            print(f"{group} pods held  cpu {g['cpu_cores_avg']:.2f} cores ({g['cpu_cores_per_user']:.3f}/user)  "
                  f"memory {g['memory_gib_avg']:.2f} GiB")
        for key in ("compute_pods", "available_pods", "assigned_pods"):
            level = pods[key]
            print(f"{key}  avg {level['avg']:.2f}  max {level['max']}")


def _level(intervals: list[tuple[datetime, datetime]], window: tuple[datetime, datetime]) -> dict[str, float]:
    """Time-weighted average and maximum of how many intervals overlap."""
    events: list[tuple[datetime, int]] = []
    for start, end in intervals:
        start, end = max(start, window[0]), min(end, window[1])
        if start < end:
            events += [(start, 1), (end, -1)]
    events.sort(key=lambda item: (item[0], item[1]))
    level = peak = 0
    area = 0.0
    previous = window[0]
    for at, delta in events:
        area += level * _seconds(previous, at)
        previous = at
        level += delta
        peak = max(peak, level)
    seconds = _seconds(*window)
    return {"avg": area / seconds if seconds else 0.0, "max": peak}


def _end(pod: PodLife, window: tuple[datetime, datetime]) -> datetime:
    return pod.gone_at or window[1]


def _overlap(start: datetime, end: datetime, window: tuple[datetime, datetime]) -> float:
    return max(0.0, _seconds(max(start, window[0]), min(end, window[1])))


def _fmt(stats: dict[str, Any]) -> str:
    if not stats["count"]:
        return "n=0"
    return (f"n={stats['count']} p50={stats['p50']:.2f} p95={stats['p95']:.2f} "
            f"p99={stats['p99']:.2f} max={stats['max']:.2f}")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _seconds(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds()


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
