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
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

# The simulator already defines how a percentile is computed, and summary.json
# is written with it. Reusing it here keeps the two files from reporting
# slightly different p95s for the same run.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "simulator"))
from kda_simulator.config import SUMMARY_SCHEMA_VERSION  # noqa: E402
from kda_simulator.metrics import summarize_values  # noqa: E402

GIB = 2**30
CONTROLLER_EVENT_RE = re.compile(
    r"\] \[(?P<label>[^\]]+)\] \[(?P<event>Request|Assigned|Released)\] (?P<rest>.*)"
)
REQUEST_ID_RE = re.compile(r"request_id=(?P<request_id>[^\s;'\"]+)")
KEY_VALUE_RE = re.compile(r"(?P<key>[a-z_]+)=(?P<value>\S+)")
# Non-numeric controller fields worth keeping. outcome is written only by an
# arm that can hand a pod back instead of deleting it.
TEXT_FIELDS = {"compute_pod", "outcome"}
# The window opens when the first request is scheduled; a request can reach the
# controller a moment before the simulator records the start.
REQUEST_WINDOW_SLACK = timedelta(minutes=1)


@dataclass
class PodLife:
    name: str
    group: str
    cpu_limit: float
    memory_limit: float
    start: datetime
    ready_at: datetime | None = None
    deleting_at: datetime | None = None
    gone_at: datetime | None = None
    # Seen in the first snapshot, so it was not created during the run.
    preexisting: bool = False
    deletion_grace_seconds: int | None = None
    # One [status, start, end] per stretch spent as "available" or "assigned".
    # A deleted pod is assigned at most once, but a pod handed back for reuse
    # goes assigned -> available -> assigned, and keeping only the first
    # assignment would count it as held the whole time.
    spans: list[list[Any]] = field(default_factory=list)

    @property
    def assigned_at(self) -> datetime | None:
        return next((s[1] for s in self.spans if s[0] == "assigned"), None)

    def stretches(self, status: str) -> list[tuple[datetime, datetime | None]]:
        return [(s[1], s[2]) for s in self.spans if s[0] == status]


def describe_headroom(headroom: dict[str, Any] | None) -> str:
    """One line on the room the cluster had when the run started.

    Requests and usage are both shown because they diverge widely on this shared
    cluster - a node can sit at 14% CPU while the scheduler counts it 54%
    committed - and only the requests figure explains a pod that stayed Pending.
    """
    if not headroom:
        return "node headroom  not recorded"
    if "error" in headroom:
        return f"node headroom  unavailable ({headroom['error']})"
    totals = headroom["eligible"]
    eligible = [n for n in headroom["nodes"] if n["eligible"] and n["schedulable"]]
    used = [n["cpu_used_m"] for n in eligible if n["cpu_used_m"] is not None]
    actual = f"  actual {sum(used) / 1000:.1f} cores" if used else ""
    # for-run counts this run's own pods as room it may have, so the figure does
    # not depend on whether the reading was taken before or after warm-up.
    return (
        f"node headroom  {totals['nodes']} nodes  "
        f"for run {totals['cpu_free_for_run_m'] / 1000:.0f} cores/"
        f"{totals['memory_free_for_run_bytes'] / 1024 ** 3:.0f} GiB  "
        f"(this run held {totals['cpu_ours_m'] / 1000:.0f} cores){actual}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute experiment metrics for one run directory.")
    parser.add_argument("run_dir", help="evaluation/data/<run-id> (the folder that contains simulator/)")
    parser.add_argument("--logs-dir", help="Controller JSONL logs. Default: <run_dir>/log_analysis/raw-jsonl")
    parser.add_argument("--bucket-minutes", type=float, help="Also report metrics per window of this length.")
    args = parser.parse_args()

    run_dir = Path(args.run_dir).resolve()
    simulator_dir = run_dir / "simulator"
    summary = json.loads((simulator_dir / "summary.json").read_text(encoding="utf-8"))
    _require_supported_schema(summary, run_dir)
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
    assignments = (
        read_assignments(logs_dir, {r["request_id"] for r in requests}, window)
        if logs_dir.exists() else None
    )

    metrics: dict[str, Any] = {
        "run": run_dir.name,
        "window": {"start": window[0].isoformat(), "end": window[1].isoformat(),
                   "minutes": _seconds(*window) / 60.0},
        "users": users,
        "server_buffers": [
            {"deployment": p.get("deployment"), "R": p.get("R"), "N": p.get("N")}
            for p in (summary.get("server") or {}).get("buffers") or []
        ],
        # Absent for runs recorded before the field existed; None then, so the
        # report can say "not recorded" instead of reporting no headroom.
        "node_headroom": (summary.get("server") or {}).get("node_headroom"),
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


def _require_supported_schema(summary: dict[str, Any], run_dir: Path) -> None:
    """Stop rather than misread a run written before the fields were renamed.

    Without this the missing keys read as "no data" and the report comes out
    full of zeros that look like a result.
    """
    found = summary.get("schema_version")
    if found == SUMMARY_SCHEMA_VERSION:
        return
    raise SystemExit(
        f"{run_dir.name} was written by an older simulator "
        f"(schema_version={found!r}, this tool expects {SUMMARY_SCHEMA_VERSION})." + "\n"
        "Its field names differ, so these metrics would be wrong. "
        "Read it with the tool from the commit that produced it, for example:" + "\n"
        "  git show b1380a4:evaluation/src/log_analysis/experiment_metrics.py > old_metrics.py"
    )


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
    result: dict[str, Any] = {"requests": request_metrics(in_window, assignments)}
    if assignments is not None:
        result["controller"] = assignment_metrics([assignments[r["request_id"]] for r in in_window
                                                   if r["request_id"] in assignments])
    if pods is not None:
        result["pods"] = pod_metrics(pods, window, users)
    return result


def command_baselines(requests: list[dict[str, Any]]) -> dict[str, float]:
    """How long each command takes, measured by this run's own successes.

    A fixed threshold would have to be guessed and would not survive a change
    of workload; the successful requests already measure it.
    """
    by_command: dict[str, list[float]] = {}
    for r in requests:
        if r.get("status") == "success" and r.get("command_duration_ms"):
            by_command.setdefault(r.get("command_name") or "", []).append(r["command_duration_ms"])
    return {name: statistics.median(values) for name, values in by_command.items() if values}


def split_incomplete(
    requests: list[dict[str, Any]],
    assignments: dict[str, dict[str, float]] | None,
) -> tuple[int, int, int]:
    """Return (server_failed, output_lost, unclassified) among incompletes.

    An incomplete request is one whose command was delivered with no completion
    signal coming back. The pod vanishing mid-command produces that, and so does
    the server finishing the work while the output is lost on the way back. Only
    the first is a server failure, and calling both one overstates the figure the
    comparison rests on.

    The controller writes a single [Released] per session with no reason field,
    so its presence decides nothing on its own. Its session_ms does: a session
    that lasted at least as long as the command takes is one where the command
    ran to the end.
    """
    baselines = command_baselines(requests)
    server_failed = output_lost = unclassified = 0
    for r in requests:
        if r.get("status") != "incomplete":
            continue
        timings = (assignments or {}).get(r.get("request_id")) or {}
        session_ms = timings.get("session_ms")
        baseline = baselines.get(r.get("command_name") or "")
        if session_ms is None:
            # No controller logs exported, or no release recorded. Either way
            # there is nothing to clear the server with, so it stays charged.
            server_failed += 1
        elif baseline is None:
            unclassified += 1
        elif session_ms >= baseline:
            output_lost += 1
        else:
            server_failed += 1
    return server_failed, output_lost, unclassified


def request_metrics(
    requests: list[dict[str, Any]],
    assignments: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    statuses: dict[str, int] = {}
    for r in requests:
        statuses[r.get("status") or "unknown"] = statuses.get(r.get("status") or "unknown", 0) + 1
    success = [r for r in requests if r.get("status") == "success"]
    # ssh_error never reached the server, so it is not a server failure.
    # output_lost is work the server finished, so it is not charged as a failure -
    # the same treatment the incomplete branch gives it when the controller's logs
    # can prove it. baseline_direct has no controller logs and reports it directly.
    charged = sum(n for s, n in statuses.items()
                  if s not in ("success", "ssh_error", "incomplete", "output_lost"))
    incomplete_server, incomplete_output_lost, incomplete_unclassified = split_incomplete(
        requests, assignments
    )
    return {
        "count": len(requests),
        "status": statuses,
        "server_failures": charged + incomplete_server + incomplete_unclassified,
        # Work the server completed and the client never saw. Not a server
        # failure, but not a success either, so it is reported on its own.
        "output_lost": incomplete_output_lost + statuses.get("output_lost", 0),
        "incomplete_unclassified": incomplete_unclassified,
        "start_delay_s": summarize_values([r["since_send_to_start_ms"] / 1000.0 for r in success if r.get("since_send_to_start_ms") is not None]),
        # idle-baseline only. A resume happens before the command is sent, so it
        # is invisible to start_delay_s; count is how many requests paid one.
        "resumed": sum(1 for r in requests if r.get("resumed_from_cull")),
        "resume_s": summarize_values([r["resume_latency_ms"] / 1000.0 for r in requests if isinstance(r.get("resume_latency_ms"), (int, float))]),
        # The whole wait before the command starts, resume included. This is the
        # figure to compare across arms: for every arm but idle-baseline it is
        # just start_delay_s.
        "user_wait_s": summarize_values([
            (r["since_send_to_start_ms"] + (r.get("resume_latency_ms") or 0.0)) / 1000.0
            for r in success if r.get("since_send_to_start_ms") is not None
        ]),
        "command_s": summarize_values([r["command_duration_ms"] / 1000.0 for r in success if r.get("command_duration_ms") is not None]),
        "ssh_retried": sum(1 for r in requests if (r.get("ssh_attempts") or 1) > 1),
        # Requests that needed another attempt because one left nothing to
        # measure - the user came back as their pod was being reclaimed. Not a
        # failure, but it says how often the threshold and the arrivals collide.
        "rerun": sum(1 for r in requests if r.get("reruns")),
        "rerun_attempts": sum(int(r.get("reruns") or 0) for r in requests),
    }


def assignment_metrics(items: list[dict[str, float]]) -> dict[str, Any]:
    waited = [item["since_request_to_compute_ready_ms"] for item in items if item.get("since_request_to_compute_ready_ms", 0) > 0]
    return {
        "assigned": len(items),
        # Requests that found no warm pod and waited for one.
        "compute_wait_ratio": (len(waited) / len(items)) if items else None,
        "compute_wait_s": summarize_values([ms / 1000.0 for ms in waited]),
        "queue_wait_s": summarize_values([item["since_request_to_claim_ms"] / 1000.0 for item in items if "since_request_to_claim_ms" in item]),
        "total_assignment_s": summarize_values(
            [item["since_request_to_assigned_ms"] / 1000.0 for item in items if "since_request_to_assigned_ms" in item]
        ),
        "release_s": summarize_values([item["release_ms"] / 1000.0 for item in items if "release_ms" in item]),
        "duplicate_releases": sum(1 for item in items if item.get("released_events", 0) > 1),
        # Empty unless the arm logs how each release ended (returned or deleted).
        "release_outcomes": _count(item.get("outcome") for item in items if item.get("outcome")),
    }


def _count(values: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


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
        [(max(start, p.ready_at), end or p.deleting_at or _end(p, window))
         for p in compute if p.ready_at is not None
         for start, end in p.stretches("available")],
        window,
    )
    result["assigned_pods"] = _level(
        [(start, end or p.deleting_at or _end(p, window))
         for p in compute for start, end in p.stretches("assigned")],
        window,
    )
    result["compute_churn"] = compute_churn(compute, window)
    return result


def compute_churn(compute: list[PodLife], window: tuple[datetime, datetime]) -> dict[str, Any]:
    """How many compute pods were created per assignment, and who deleted them.

    The reuse comparison rests on creation_saving_ratio. Counting how often a
    pod was assigned again is not enough: with the same R/N rule the Deployment
    backfills the moment a pod is assigned, so a returned pod can be reused
    while the number of pods created stays where it was. Only creations per
    assignment says whether reuse saved any.

    Pods already there when recording began were not created by the run and
    are left out of created. Who deleted a pod is read from its grace period:
    the controller deletes with 0, a ReplicaSet with the pod's default.
    """
    def inside(at: datetime | None) -> bool:
        return at is not None and window[0] <= at < window[1]

    assignments = sum(1 for p in compute for start, _ in p.stretches("assigned") if inside(start))
    created = sum(1 for p in compute if not p.preexisting and inside(p.start))
    deleted = [p for p in compute if inside(p.deleting_at)]
    deleted_by = {"controller": 0, "replicaset": 0, "unknown": 0}
    for p in deleted:
        if p.deletion_grace_seconds is None:
            deleted_by["unknown"] += 1
        elif p.deletion_grace_seconds == 0:
            deleted_by["controller"] += 1
        else:
            deleted_by["replicaset"] += 1
    first_assignments = sum(1 for p in compute if inside(p.assigned_at))
    return {
        "assignments": assignments,
        "created": created,
        # Assignments that landed on a pod already used by an earlier request.
        "reassignments": assignments - first_assignments,
        # Created and deleted without serving anyone: a creation paid for nothing.
        "deleted_never_assigned": sum(1 for p in deleted if p.assigned_at is None and not p.preexisting),
        "deleted_by": deleted_by,
        "creation_saving_ratio": (1.0 - created / assignments) if assignments else None,
    }


def build_pod_lives(records: Iterable[dict[str, Any]], run_end: datetime) -> list[PodLife]:
    lives: dict[str, PodLife] = {}

    def close_span(life: PodLife, at: datetime) -> None:
        if life.spans and life.spans[-1][2] is None:
            life.spans[-1][2] = at

    def observe(pod: dict[str, Any], at: datetime, preexisting: bool = False) -> PodLife:
        life = lives.get(pod["uid"])
        if life is None:
            life = PodLife(
                name=pod["name"],
                group=pod["group"],
                cpu_limit=float(pod.get("cpu_limit") or 0.0),
                memory_limit=float(pod.get("memory_limit") or 0.0),
                start=at,
                preexisting=preexisting,
            )
            lives[pod["uid"]] = life
        if pod.get("ready") and life.ready_at is None:
            life.ready_at = at
        if life.deleting_at is None:
            status = pod.get("compute_status")
            current = life.spans[-1][0] if life.spans and life.spans[-1][2] is None else None
            if status in ("available", "assigned") and status != current:
                close_span(life, at)
                life.spans.append([status, at, None])
        if pod.get("deleting") and life.deleting_at is None:
            life.deleting_at = at
            close_span(life, at)
        if pod.get("deletion_grace_seconds") is not None:
            life.deletion_grace_seconds = int(pod["deletion_grace_seconds"])
        return life

    def gone(life: PodLife, at: datetime) -> None:
        if life.gone_at is None:
            life.gone_at = at
            life.deleting_at = life.deleting_at or at
            close_span(life, life.deleting_at)

    first_snapshot = True
    for record in sorted(records, key=lambda item: item["observed_at"]):
        at = _parse_time(record["observed_at"])
        if record["type"] == "SNAPSHOT":
            present = set()
            for pod in record["pods"]:
                observe(pod, at, preexisting=first_snapshot)
                present.add(pod["uid"])
            first_snapshot = False
            # Deleted while the watch was reconnecting.
            for uid, life in lives.items():
                if uid not in present:
                    gone(life, at)
        elif record.get("pod"):
            life = observe(record["pod"], at)
            if record["type"] == "DELETED":
                gone(life, at)
    return list(lives.values())


def read_assignments(
    logs_dir: Path,
    request_ids: set[str],
    window: tuple[datetime, datetime] | None = None,
) -> dict[str, dict[str, float]]:
    """Controller [Assigned] and [Released] fields per simulator request_id.

    [Released] carries session_ms, which is how long the controller held the
    compute pod for that request. It is the only server-side measure of whether
    the command ran to the end, and classifying an incomplete request needs it.
    """
    label_to_request: dict[str, str] = {}
    timings: dict[str, dict[str, float]] = {}
    events = ("[Request]", "[Assigned]", "[Released]")
    for path in sorted(logs_dir.glob("*.jsonl")) + sorted(logs_dir.glob("*.jsonl.gz")):
        opener = gzip.open if path.name.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if '"controller"' not in line or not any(e in line for e in events):
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
                    # request_id comes from the simulator's experiment name and a
                    # sequence number, so a rerun under the same name repeats every
                    # id. The server keeps older runs' logs, and without this the
                    # last matching run wins. Only this run's window counts.
                    if window is not None and not _within(record.get("time"), window):
                        continue
                    found = REQUEST_ID_RE.search(rest)
                    if found and found.group("request_id") in request_ids:
                        label_to_request[label] = found.group("request_id")
                    continue
                # Assigned and Released describe one session between them, so
                # their fields are merged rather than overwriting each other.
                fields = timings.setdefault(label, {})
                for key, value in KEY_VALUE_RE.findall(rest):
                    if key.endswith("_ms") and _is_number(value):
                        fields[key] = float(value)
                    elif key in TEXT_FIELDS:
                        fields[key] = value.rstrip(",;")
                if event == "Released":
                    # A pod that is deleted makes a second release a no-op; a pod
                    # handed back for reuse does not, so repeats are counted.
                    fields["released_events"] = fields.get("released_events", 0) + 1
    return {request: timings[label] for label, request in label_to_request.items() if label in timings}


def print_report(metrics: dict[str, Any]) -> None:
    overall = metrics["overall"]
    req = overall["requests"]
    print(f"run {metrics['run']}  {metrics['window']['minutes']:.1f} min  users={metrics['users']}  "
          f"buffers={metrics['server_buffers']}")
    print(describe_headroom(metrics.get("node_headroom")))
    line = f"requests {req['count']}  status={req['status']}  server_failures={req['server_failures']}"
    if req.get("output_lost"):
        # Named separately because the server did the work; counting it as a
        # server failure would understate the system being measured.
        line += f"  output_lost={req['output_lost']}"
    if req.get("incomplete_unclassified"):
        line += f"  unclassified={req['incomplete_unclassified']}"
    print(line)
    print(f"start delay s  {_fmt(req['start_delay_s'])}")
    if req.get("rerun"):
        share = 100.0 * req["rerun"] / req["count"] if req["count"] else 0.0
        print(
            f"re-run for want of a measurement  {req['rerun']} ({share:.1f}%)"
            f"  attempts {req['rerun_attempts']}"
        )
    # Only idle-baseline culls, so this stays quiet for every other arm.
    if req.get("resumed"):
        share = 100.0 * req["resumed"] / req["count"] if req["count"] else 0.0
        print(f"resumed from cull  {req['resumed']} ({share:.1f}%)  {_fmt(req['resume_s'])}")
        print(f"user wait s  {_fmt(req['user_wait_s'])}")
    if "controller" in overall:
        ctl = overall["controller"]
        ratio = ctl["compute_wait_ratio"]
        print(f"waited for a warm pod  {ratio:.1%}  wait s {_fmt(ctl['compute_wait_s'])}" if ratio is not None
              else "waited for a warm pod  n/a")
        print(f"queue wait s  {_fmt(ctl['queue_wait_s'])}")
        print(f"release s  {_fmt(ctl['release_s'])}")
        if ctl.get("duplicate_releases"):
            print(f"requests released more than once  {ctl['duplicate_releases']}")
        if ctl.get("release_outcomes"):
            print(f"release outcomes  {ctl['release_outcomes']}")
    if "pods" in overall:
        pods = overall["pods"]
        for group in ("compute", "user"):
            g = pods[group]
            print(f"{group} pods held  cpu {g['cpu_cores_avg']:.2f} cores ({g['cpu_cores_per_user']:.3f}/user)  "
                  f"memory {g['memory_gib_avg']:.2f} GiB")
        for key in ("compute_pods", "available_pods", "assigned_pods"):
            level = pods[key]
            print(f"{key}  avg {level['avg']:.2f}  max {level['max']}")
        churn = pods["compute_churn"]
        saving = churn["creation_saving_ratio"]
        print(
            f"compute churn  assignments {churn['assignments']}  created {churn['created']}  "
            f"reassigned {churn['reassignments']}  deleted unused {churn['deleted_never_assigned']}  "
            f"deleted by {churn['deleted_by']}  "
            f"creation saving {'n/a' if saving is None else f'{saving:.1%}'}"
        )


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


def _within(value: Any, window: tuple[datetime, datetime]) -> bool:
    try:
        at = _parse_time(str(value))
    except ValueError:
        return False
    return window[0] - REQUEST_WINDOW_SLACK <= at <= window[1] + REQUEST_WINDOW_SLACK


def _is_number(value: str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
