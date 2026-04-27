#!/usr/bin/env python3
"""Merge KDA JSONL logs and optionally join them with Grafana CPU CSV exports.

The logging pipeline stores raw Fluent Bit records as JSONL. This tool keeps
that raw format untouched and creates analysis-friendly CSV files:

- events.csv: one row per parsed log event
- minute_summary.csv: one row per minute of application events
- grafana_summary.csv: one row per Grafana timestamp, when --grafana-csv is used
- joined_minute.csv: minute_summary joined with grafana_summary, when provided
"""

from __future__ import annotations

import argparse
import csv
import gzip
import glob
import json
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


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
    "allocation_ms",
    "total_assignment_ms",
    "session_ms",
    "release_ms",
    "source_file",
]

MINUTE_COLUMNS = [
    "Time",
    "events_total",
    "enqueued_count",
    "assigned_count",
    "released_count",
    "failed_count",
    "cancelled_count",
    "unique_tickets",
    "unique_users",
    "avg_queue_wait_ms",
    "avg_allocation_ms",
    "avg_total_assignment_ms",
    "avg_session_ms",
    "avg_release_ms",
]

GRAFANA_COLUMNS = [
    "Time",
    "grafana_pod_columns",
    "grafana_numeric_values",
    "grafana_active_pods",
    "cpu_sum_cores",
    "cpu_sum_mcores",
    "cpu_avg_mcores",
    "cpu_max_mcores",
    "cpu_max_pod",
]

KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\"[^\"]*\"|'[^']*'|[^\s]+)")
EVENT_RE = re.compile(r"^\[(?P<component>[^\]]+)\]\s+(?P<event>[^\s]+)")
DETAILED_WITH_LABEL_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+\[(?P<request_label>[^\]]*)\]\s+(?P<message>.*)$"
)
DETAILED_SIMPLE_RE = re.compile(
    r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+(?P<message>.*)$"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert raw Fluent Bit JSONL logs into event/minute CSV files and optionally join Grafana CSV data."
    )
    parser.add_argument(
        "--logs",
        nargs="+",
        required=True,
        help="JSONL files, .jsonl.gz files, directories, or glob patterns copied from logs-pvc.",
    )
    parser.add_argument(
        "--grafana-csv",
        help="Optional Grafana CSV export with a Time column and pod CPU columns.",
    )
    parser.add_argument(
        "--out-dir",
        default="outputs/experiment-log-analysis",
        help="Directory for generated CSV files.",
    )
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=60,
        help="Aggregation bucket size. Use 60 to match the current Grafana export.",
    )
    parser.add_argument(
        "--grafana-time-shift-minutes",
        type=int,
        default=0,
        help="Shift Grafana Time values before joining, useful when exported times are local rather than UTC.",
    )
    return parser.parse_args()


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
    unique = sorted({path.resolve() for path in paths if path.exists() and path.is_file()})
    return unique


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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
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
    component, event, fields = parse_message_fields(message)

    row = {column: "" for column in EVENT_COLUMNS}
    row.update(
        {
            "ts_utc": dt.isoformat().replace("+00:00", "Z"),
            "ts_ms": str(int(dt.timestamp() * 1000)),
            "minute_utc": format_time(minute),
            "node": str(kubernetes.get("host") or kubernetes.get("node_name") or ""),
            "namespace": str(kubernetes.get("namespace_name") or ""),
            "pod": str(kubernetes.get("pod_name") or ""),
            "container": str(kubernetes.get("container_name") or ""),
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


def safe_float(value: object) -> Optional[float]:
    if value in (None, "", "NaN"):
        return None
    try:
        parsed = float(str(value))
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def average(values: List[float]) -> str:
    if not values:
        return ""
    return f"{sum(values) / len(values):.6f}"


def summarize_events(events: List[Dict[str, str]]) -> List[Dict[str, str]]:
    buckets: Dict[str, Dict[str, object]] = defaultdict(
        lambda: {
            "events_total": 0,
            "enqueued_count": 0,
            "assigned_count": 0,
            "released_count": 0,
            "failed_count": 0,
            "cancelled_count": 0,
            "tickets": set(),
            "users": set(),
            "queue_wait_ms": [],
            "allocation_ms": [],
            "total_assignment_ms": [],
            "session_ms": [],
            "release_ms": [],
        }
    )
    for event in events:
        bucket = buckets[event["minute_utc"]]
        bucket["events_total"] += 1
        event_name = event.get("event", "").lower()
        if event_name == "enqueued":
            bucket["enqueued_count"] += 1
        elif event_name == "assigned":
            bucket["assigned_count"] += 1
        elif event_name == "released":
            bucket["released_count"] += 1
        elif event_name == "failed":
            bucket["failed_count"] += 1
        elif event_name == "cancelled":
            bucket["cancelled_count"] += 1

        if event.get("ticket_id"):
            bucket["tickets"].add(event["ticket_id"])
        if event.get("username"):
            bucket["users"].add(event["username"])
        for key in ("queue_wait_ms", "allocation_ms", "total_assignment_ms", "session_ms", "release_ms"):
            value = safe_float(event.get(key))
            if value is not None:
                bucket[key].append(value)

    rows: List[Dict[str, str]] = []
    for minute in sorted(buckets):
        bucket = buckets[minute]
        rows.append(
            {
                "Time": minute,
                "events_total": str(bucket["events_total"]),
                "enqueued_count": str(bucket["enqueued_count"]),
                "assigned_count": str(bucket["assigned_count"]),
                "released_count": str(bucket["released_count"]),
                "failed_count": str(bucket["failed_count"]),
                "cancelled_count": str(bucket["cancelled_count"]),
                "unique_tickets": str(len(bucket["tickets"])),
                "unique_users": str(len(bucket["users"])),
                "avg_queue_wait_ms": average(bucket["queue_wait_ms"]),
                "avg_allocation_ms": average(bucket["allocation_ms"]),
                "avg_total_assignment_ms": average(bucket["total_assignment_ms"]),
                "avg_session_ms": average(bucket["session_ms"]),
                "avg_release_ms": average(bucket["release_ms"]),
            }
        )
    return rows


def parse_grafana_time(value: str, shift_minutes: int, bucket_seconds: int) -> str:
    parsed = parse_datetime_value(value)
    if parsed is None:
        parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    parsed = parsed + timedelta(minutes=shift_minutes)
    return format_time(floor_time(parsed, bucket_seconds))


def summarize_grafana_csv(path: Path, shift_minutes: int, bucket_seconds: int) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "Time" not in reader.fieldnames:
            raise ValueError("Grafana CSV must contain a Time column")
        pod_columns = [name for name in reader.fieldnames if name != "Time" and not name.startswith("quota")]
        for raw in reader:
            values = []
            max_value: Optional[float] = None
            max_pod = ""
            for column in pod_columns:
                value = safe_float(raw.get(column))
                if value is None:
                    continue
                values.append(value)
                if max_value is None or value > max_value:
                    max_value = value
                    max_pod = column
            total = sum(values)
            active = sum(1 for value in values if value > 0)
            rows.append(
                {
                    "Time": parse_grafana_time(raw["Time"], shift_minutes, bucket_seconds),
                    "grafana_pod_columns": str(len(pod_columns)),
                    "grafana_numeric_values": str(len(values)),
                    "grafana_active_pods": str(active),
                    "cpu_sum_cores": f"{total:.9f}",
                    "cpu_sum_mcores": f"{total * 1000:.6f}",
                    "cpu_avg_mcores": f"{(total / len(values) * 1000) if values else 0:.6f}",
                    "cpu_max_mcores": f"{(max_value or 0) * 1000:.6f}",
                    "cpu_max_pod": max_pod,
                }
            )
    return rows


def write_csv(path: Path, rows: List[Dict[str, str]], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def join_minute_rows(event_rows: List[Dict[str, str]], grafana_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    by_time: Dict[str, Dict[str, str]] = {}
    for row in grafana_rows:
        by_time.setdefault(row["Time"], {}).update(row)
    for row in event_rows:
        by_time.setdefault(row["Time"], {}).update(row)

    columns = set(MINUTE_COLUMNS + GRAFANA_COLUMNS)
    joined: List[Dict[str, str]] = []
    for time_key in sorted(by_time):
        row = {column: "" for column in columns}
        row.update(by_time[time_key])
        row["Time"] = time_key
        joined.append(row)
    return joined


def main() -> int:
    args = parse_args()
    log_paths = iter_input_paths(args.logs)
    if not log_paths:
        raise SystemExit("No log files matched --logs")

    out_dir = Path(args.out_dir)
    events = read_log_events(log_paths, args.bucket_seconds)
    minute_rows = summarize_events(events)
    write_csv(out_dir / "events.csv", events, EVENT_COLUMNS)
    write_csv(out_dir / "minute_summary.csv", minute_rows, MINUTE_COLUMNS)

    print(f"read {len(log_paths)} log file(s)")
    print(f"wrote {out_dir / 'events.csv'} ({len(events)} event rows)")
    print(f"wrote {out_dir / 'minute_summary.csv'} ({len(minute_rows)} minute rows)")

    if args.grafana_csv:
        grafana_rows = summarize_grafana_csv(
            Path(args.grafana_csv),
            shift_minutes=args.grafana_time_shift_minutes,
            bucket_seconds=args.bucket_seconds,
        )
        joined_rows = join_minute_rows(minute_rows, grafana_rows)
        joined_columns = ["Time"] + [column for column in MINUTE_COLUMNS[1:] + GRAFANA_COLUMNS[1:]]
        write_csv(out_dir / "grafana_summary.csv", grafana_rows, GRAFANA_COLUMNS)
        write_csv(out_dir / "joined_minute.csv", joined_rows, joined_columns)
        print(f"wrote {out_dir / 'grafana_summary.csv'} ({len(grafana_rows)} Grafana rows)")
        print(f"wrote {out_dir / 'joined_minute.csv'} ({len(joined_rows)} joined rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
