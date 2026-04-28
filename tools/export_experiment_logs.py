#!/usr/bin/env python3
"""Fetch logs-pvc JSONL files from Kubernetes and create program-log CSV files.

This is the one-command wrapper for the logging pipeline:

1. Create a temporary pod that mounts the logs PVC.
2. Copy /mnt/logs from that pod into this repository under tools/log_export.
3. Run experiment_log_analysis.py against the copied JSONL files.
4. Delete the temporary pod.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import glob
import json
import re
import subprocess
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO_ROOT / "tools" / "log_export"

EVENT_COLUMNS = [
    "ts_utc",
    "ts_ms",
    "minute_utc",
    "node",
    "namespace",
    "pod",
    "container",
    "app_label",
    "level",
    "logger",
    "request_label",
    "component",
    "event",
    "message",
    "ticket_id",
    "username",
    "frontend_pod",
    "frontend_ip",
    "backend_pod",
    "backend_ip",
    "backend_type",
    "queue_position",
    "retry_count",
    "ingress_ts_ms",
    "queue_wait_ms",
    "backend_unavailable_ms",
    "backend_ready_to_claim_ms",
    "allocation_ms",
    "total_assignment_ms",
    "session_ms",
    "release_ms",
    "source_file",
]

TIMELINE_COLUMNS = ["Time", *EVENT_COLUMNS]

KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\"[^\"]*\"|'[^']*'|[^\s]+)")
EVENT_RE = re.compile(r"^\[(?P<component>[^\]]+)\]\s+(?P<event>[^\s]+)")
DETAILED_TS_RE = r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\s+[+-]\d{4})?)"
DETAILED_WITH_LABEL_RE = re.compile(
    r"^\[" + DETAILED_TS_RE + r"\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+\[(?P<request_label>[^\]]*)\]\s+(?P<message>.*)$"
)
DETAILED_SIMPLE_RE = re.compile(
    r"^\[" + DETAILED_TS_RE + r"\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+(?P<message>.*)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy JSONL logs from logs-pvc into this repo and generate CSV analysis outputs."
    )
    parser.add_argument("--namespace", default="swlabpods", help="Kubernetes namespace containing logs-pvc.")
    parser.add_argument("--pvc", default="logs-pvc", help="PVC name that stores Fluent Bit JSONL logs.")
    parser.add_argument("--reader-image", default="alpine:3.20", help="Temporary pod image. Must include tar.")
    parser.add_argument("--kubectl", default="kubectl", help="kubectl executable name or path.")
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Directory for copied JSONL logs and generated program-log CSV files.",
    )
    parser.add_argument("--bucket-seconds", type=int, default=60, help="Aggregation bucket size.")
    parser.add_argument("--skip-analysis", action="store_true", help="Only copy raw JSONL logs.")
    parser.add_argument("--keep-reader-pod", action="store_true", help="Do not delete the temporary reader pod.")
    return parser.parse_args()


def run_command(args: List[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(args))
    return subprocess.run(args, check=check, text=True)


def iter_input_paths(inputs: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for raw in inputs:
        candidate = Path(raw)
        if any(char in raw for char in "*?[]"):
            paths.extend(Path(match) for match in glob.glob(raw, recursive=True))
        elif candidate.is_dir():
            paths.extend(candidate.rglob("*.jsonl"))
            paths.extend(candidate.rglob("*.jsonl.gz"))
        else:
            paths.append(candidate)
    return sorted({path.resolve() for path in paths if path.exists() and path.is_file()})


def open_log_file(path: Path):
    if path.name.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def parse_datetime_value(value: object) -> Optional[datetime]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def floor_time(dt: datetime, bucket_seconds: int) -> datetime:
    epoch = int(dt.timestamp())
    bucket = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(bucket, timezone.utc)


def format_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_json_maybe(value: object) -> object:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return value
    return value


def parse_detailed_log_line(value: object) -> Optional[Dict[str, object]]:
    if not isinstance(value, str):
        return None
    for pattern in (DETAILED_WITH_LABEL_RE, DETAILED_SIMPLE_RE):
        match = pattern.match(value)
        if not match:
            continue
        parsed = match.groupdict()
        return {
            "ts": parsed.get("ts", ""),
            "level": parsed.get("level", ""),
            "request_label": parsed.get("request_label", ""),
            "message": parsed.get("message", ""),
        }
    return None


def extract_app_log(record: Dict[str, object]) -> Dict[str, object]:
    app_log = parse_json_maybe(record.get("app_log"))
    if isinstance(app_log, dict):
        return dict(app_log)

    log_value = parse_json_maybe(record.get("log"))
    if isinstance(log_value, dict):
        return dict(log_value)
    detailed_log = parse_detailed_log_line(log_value)
    if detailed_log:
        return detailed_log

    app_log = {}
    for key in ("ts", "time", "asctime", "level", "levelname", "logger", "name", "request_label", "message"):
        if key in record:
            app_log[key] = record[key]
    if "message" not in app_log and isinstance(record.get("log"), str):
        app_log["message"] = record.get("log")
    return app_log


def parse_message_fields(message: str) -> Tuple[str, str, Dict[str, str]]:
    component = ""
    event = ""
    match = EVENT_RE.search(message or "")
    if match:
        component = match.group("component")
        event = match.group("event")

    fields: Dict[str, str] = {}
    for item in KEY_VALUE_RE.finditer(message or ""):
        value = item.group("value")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        fields[item.group("key")] = value
    return component, event, fields


def identify_resource(labels: Dict[str, object], pod_name: str, container_name: str) -> str:
    app_label = str(labels.get("app") or "")
    kubessh_label = str(labels.get("kubessh") or "")

    if app_label == "controller":
        return "controller"
    if app_label == "backend-pool":
        return "backend-general"
    if kubessh_label == "swlabssh":
        return "dcusshk8s"
    if pod_name.startswith("controller-"):
        return "controller"
    if pod_name.startswith("backend-general-") or container_name == "backend-agent":
        return "backend-general"
    return app_label or kubessh_label or "unknown"


def event_time(record: Dict[str, object], app_log: Dict[str, object]) -> Optional[datetime]:
    for key in ("time", "@timestamp", "timestamp", "date"):
        parsed = parse_datetime_value(record.get(key))
        if parsed:
            return parsed
    for key in ("ts", "asctime", "time"):
        parsed = parse_datetime_value(app_log.get(key))
        if parsed:
            return parsed
    return None


def parse_fluent_bit_record(line: str, source_file: Path, bucket_seconds: int) -> Optional[Dict[str, str]]:
    stripped = line.strip()
    if not stripped:
        return None
    try:
        record = json.loads(stripped)
    except json.JSONDecodeError:
        record = {"log": stripped}
    if not isinstance(record, dict):
        return None

    app_log = extract_app_log(record)
    message = str(app_log.get("message") or app_log.get("log") or record.get("log") or "")
    if not message:
        return None

    dt = event_time(record, app_log)
    if dt is None:
        return None
    minute = floor_time(dt, bucket_seconds)
    kubernetes = record.get("kubernetes") if isinstance(record.get("kubernetes"), dict) else {}
    labels = kubernetes.get("labels") if isinstance(kubernetes.get("labels"), dict) else {}
    pod_name = str(kubernetes.get("pod_name") or "")
    container_name = str(kubernetes.get("container_name") or "")
    component, event, fields = parse_message_fields(message)

    row = {column: "" for column in EVENT_COLUMNS}
    row.update(
        {
            "ts_utc": dt.isoformat().replace("+00:00", "Z"),
            "ts_ms": str(int(dt.timestamp() * 1000)),
            "minute_utc": format_time(minute),
            "node": str(kubernetes.get("host") or kubernetes.get("node_name") or ""),
            "namespace": str(kubernetes.get("namespace_name") or ""),
            "pod": pod_name,
            "container": container_name,
            "app_label": str(labels.get("app") or ""),
            "level": str(app_log.get("level") or app_log.get("levelname") or ""),
            "logger": str(app_log.get("logger") or app_log.get("name") or ""),
            "request_label": str(app_log.get("request_label") or ""),
            "component": component,
            "event": event,
            "message": message,
            "source_file": str(source_file),
        }
    )
    for key in fields:
        if key in row:
            row[key] = fields[key]
    row["resource"] = identify_resource(labels, pod_name, container_name)
    return row


def read_log_events(paths: List[Path], bucket_seconds: int) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []
    for path in paths:
        with open_log_file(path) as handle:
            for line in handle:
                row = parse_fluent_bit_record(line, path, bucket_seconds)
                if row:
                    events.append(row)
    events.sort(key=lambda item: (item["ts_ms"], item["source_file"]))
    return events


def write_csv(path: Path, rows: List[Dict[str, str]], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def safe_filename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("._") or "unknown"


def clear_resource_timelines(resource_dir: Path) -> None:
    resource_dir.mkdir(parents=True, exist_ok=True)
    for existing in resource_dir.glob("*.csv"):
        existing.unlink()


def write_resource_timelines(events: List[Dict[str, str]], resource_dir: Path) -> None:
    clear_resource_timelines(resource_dir)
    rows_by_resource: Dict[str, List[Dict[str, str]]] = {}
    for event in events:
        resource = event.get("resource") or "unknown"
        row = {column: "" for column in TIMELINE_COLUMNS}
        row["Time"] = event["minute_utc"]
        row.update(event)
        rows_by_resource.setdefault(resource, []).append(row)

    for resource in sorted(rows_by_resource):
        path = resource_dir / f"{safe_filename(resource)}.csv"
        write_csv(path, rows_by_resource[resource], TIMELINE_COLUMNS)
        print(f"wrote {path}")


def write_analysis_outputs(raw_dir: Path, out_dir: Path, bucket_seconds: int) -> None:
    log_paths = iter_input_paths([str(raw_dir)])
    if not log_paths:
        print("No JSONL files were found. Generate a request first, then run this command again.")
        return

    events = read_log_events(log_paths, bucket_seconds)

    timeline_rows = []
    for event in events:
        row = {column: "" for column in TIMELINE_COLUMNS}
        row["Time"] = event["minute_utc"]
        row.update(event)
        timeline_rows.append(row)
    write_csv(out_dir / "timeline.csv", timeline_rows, TIMELINE_COLUMNS)
    write_resource_timelines(events, out_dir / "timeline_by_resource")

    print(f"wrote {out_dir / 'timeline.csv'}")


def reader_pod_overrides(pvc_name: str, reader_image: str) -> str:
    return json.dumps(
        {
            "spec": {
                "containers": [
                    {
                        "name": "log-reader",
                        "image": reader_image,
                        "command": ["sh", "-c", "sleep 3600"],
                        "volumeMounts": [{"name": "logs", "mountPath": "/mnt/logs"}],
                    }
                ],
                "volumes": [{"name": "logs", "persistentVolumeClaim": {"claimName": pvc_name}}],
            }
        },
        separators=(",", ":"),
    )


def create_reader_pod(args: argparse.Namespace, pod_name: str) -> None:
    run_command(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "run",
            pod_name,
            "--image",
            args.reader_image,
            "--restart=Never",
            "--overrides",
            reader_pod_overrides(args.pvc, args.reader_image),
        ]
    )
    run_command(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "wait",
            "--for=condition=Ready",
            f"pod/{pod_name}",
            "--timeout=90s",
        ]
    )


def delete_reader_pod(args: argparse.Namespace, pod_name: str) -> None:
    run_command(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "delete",
            "pod",
            pod_name,
            "--ignore-not-found=true",
            "--wait=true",
        ],
        check=False,
    )


def copy_logs(args: argparse.Namespace, pod_name: str, raw_dir: Path) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    for pattern in ("*.jsonl", "*.jsonl.gz"):
        for existing in raw_dir.glob(pattern):
            existing.unlink()
    run_command(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "cp",
            f"{pod_name}:/mnt/logs/.",
            str(raw_dir),
        ]
    )


def list_log_files(raw_dir: Path) -> List[Path]:
    files = sorted(raw_dir.glob("*.jsonl")) + sorted(raw_dir.glob("*.jsonl.gz"))
    return [path for path in files if path.is_file()]


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir).resolve()
    raw_dir = out_dir / "raw-jsonl"
    csv_dir = out_dir
    pod_name = f"log-export-{datetime.now().strftime('%H%M%S')}"

    print(f"output directory: {out_dir}")
    try:
        delete_reader_pod(args, pod_name)
        create_reader_pod(args, pod_name)
        copy_logs(args, pod_name, raw_dir)
    finally:
        if not args.keep_reader_pod:
            delete_reader_pod(args, pod_name)

    log_files = list_log_files(raw_dir)
    print(f"copied {len(log_files)} JSONL file(s) into {raw_dir}")
    if not log_files:
        print("No JSONL files were found. Generate a request first, then run this command again.")
        return 0

    if not args.skip_analysis:
        write_analysis_outputs(raw_dir, csv_dir, args.bucket_seconds)
        print(f"program log timeline CSV: {csv_dir / 'timeline.csv'}")
    else:
        print("analysis skipped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
