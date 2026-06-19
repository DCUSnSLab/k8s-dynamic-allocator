#!/usr/bin/env python3
"""Fetch logs-pvc JSONL files from Kubernetes and create program-log CSV files.

This is the one-command wrapper for the logging pipeline:

1. Create a temporary pod that mounts the logs PVC.
2. Copy /mnt/logs from that pod into a data/log_analysis result directory.
3. Generate thin timeline CSV files from the copied JSONL files.
4. Delete the temporary pod.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import glob
import json
import os
import re
import subprocess
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_DIR_ENV = "KDA_RUN_DIR"
EXPERIMENT_NAME_ENV = "KDA_EXPERIMENT_NAME"
DEFAULT_RESULTS_DIR = REPO_ROOT / "data" / "log_analysis"
OUTPUT_TZ = timezone(timedelta(hours=9))

TIMELINE_COLUMNS = [
    "timestamp",
    "node",
    "pod",
    "compute_pod",
    "module",
    "component",
    "level",
    "event",
    "request_label",
    "request_id",
    "username",
    "command_name",
    "exit_status",
    "session_id",
    "conn",
    "chan",
    "message",
]

KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>\"[^\"]*\"|'[^']*'|[^\s]+)")
EVENT_TAG_RE = re.compile(r"^\[(?P<tag>[^\]]+)\](?:\s+(?P<rest>.*))?$")
LEVEL_PREFIX_RE = re.compile(r"^(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL):\s*(?P<message>.*)$")
DETAILED_TS_RE = r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\s+[+-]\d{4})?)"
DETAILED_WITH_LABEL_RE = re.compile(
    r"^\[" + DETAILED_TS_RE + r"\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+\[(?P<request_label>[^\]]*)\]\s+(?P<message>.*)$"
)
DETAILED_SIMPLE_RE = re.compile(
    r"^\[" + DETAILED_TS_RE + r"\]\s+"
    r"\[(?P<level>[A-Z]+)\]\s+(?P<message>.*)$"
)
REQUEST_LABEL_RE = re.compile(r"^-?$|^[A-Za-z0-9_.]+-[A-Fa-f0-9]{6,}$")
ASYNCSSH_LOG_RE = re.compile(r"^\[asyncssh\]\s+(?:(?P<context>\[[^\]]+\])\s+)?(?P<body>.*)$")
KUBESSH_LOG_RE = re.compile(r"^\[KubeSSH\]\s+(?P<body>.*)$")
SIM_COMMAND_RE = re.compile(r"\brequest_id=(?P<request_id>\S+)\s+command=(?P<command_name>[A-Za-z0-9_.-]+)")
AUTH_USER_RE = re.compile(r"\buser\s+(?P<username>[A-Za-z0-9_.-]+)\b")
LOGIN_ATTEMPT_RE = re.compile(r"\bLogin attempted by (?P<username>[A-Za-z0-9_.-]+)\b")
EXIT_STATUS_RE = re.compile(r"\b(?:exit status\s+|exit_code=)(?P<exit_status>-?\d+)\b")
PORT_RE = re.compile(r"\bport\s+(?P<port>\d+)\b")
SESSION_ID_RE = re.compile(r"\bsession[:=]\s*(?P<session_id>[A-Za-z0-9_.-]+)\b")


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
        default=None,
        help=(
            "Directory for copied JSONL logs and generated program-log CSV files. "
            "Overrides --run-dir and KDA_RUN_DIR."
        ),
    )
    parser.add_argument(
        "--run-dir",
        default=None,
        help=(
            "Result directory name. When set, logs are written to "
            "data/log_analysis/<run-dir>. Can also be provided with KDA_RUN_DIR."
        ),
    )
    parser.add_argument(
        "--experiment-name",
        default=None,
        help=(
            "Name used when creating a new timestamped log result directory. "
            "Can also be provided with KDA_EXPERIMENT_NAME. Defaults to logs."
        ),
    )
    parser.add_argument(
        "--results-dir",
        default=str(DEFAULT_RESULTS_DIR),
        help="Log analysis results root used with --run-dir or KDA_RUN_DIR.",
    )
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=60,
        help="Deprecated compatibility option. Timeline CSV now keeps per-log timestamps.",
    )
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
    text = normalize_fractional_seconds(text)
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
    return parsed


def normalize_fractional_seconds(value: str) -> str:
    """Trim nanosecond log timestamps to Python datetime microsecond precision."""
    return re.sub(r"(\.\d{6})\d+(?=Z|[+-]\d{2}:?\d{2}$|$)", r"\1", value)


def floor_time(dt: datetime, bucket_seconds: int) -> datetime:
    epoch = int(dt.timestamp())
    bucket = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(bucket, timezone.utc)


def format_time(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def format_timestamp(dt: datetime) -> str:
    return dt.astimezone(OUTPUT_TZ).isoformat(timespec="milliseconds")


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
        request_label = parsed.get("request_label", "")
        message = parsed.get("message", "")
        looks_like_legacy_request_label = bool(request_label and message.startswith("Execute request"))
        if request_label and not REQUEST_LABEL_RE.match(request_label) and not looks_like_legacy_request_label:
            message = f"[{request_label}] {message}".strip()
            request_label = ""
        return {
            "ts": parsed.get("ts", ""),
            "level": parsed.get("level", ""),
            "request_label": request_label,
            "message": message,
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


def normalize_message_level(level: str, message: str) -> Tuple[str, str]:
    level_value = (level or "").strip()
    message_value = message or ""
    if level_value:
        return level_value, message_value
    match = LEVEL_PREFIX_RE.match(message_value)
    if not match:
        return level_value, message_value
    return match.group("level"), match.group("message").strip()


def classify_plain_event(message: str, level: str) -> str:
    text = (message or "").strip()
    if text.startswith("Execute request"):
        return "Request"
    if text.startswith("Traceback "):
        return "traceback"
    if text.startswith("File ") or text.startswith('File "'):
        return "traceback"
    if re.match(r"^[A-Za-z_][A-Za-z0-9_.]*(Error|Exception|Warning):", text):
        return "exception"
    level_value = (level or "").strip().lower()
    if level_value in {"warning", "error", "critical"}:
        return level_value
    return ""


def parse_message_fields(message: str, level: str = "") -> Tuple[str, str, str, Dict[str, str]]:
    component = ""
    event = ""
    body = message or ""
    match = EVENT_TAG_RE.search(message or "")
    if match:
        tag = match.group("tag")
        rest = match.group("rest") or ""
        first_token = rest.split(maxsplit=1)[0] if rest else ""
        if tag in {"QUEUE", "SUCCESS", "FAILED"} and first_token and "=" not in first_token:
            component = tag
            event = first_token.rstrip(":")
            body = rest.split(maxsplit=1)[1] if len(rest.split(maxsplit=1)) > 1 else ""
        else:
            event = tag
            body = rest
    else:
        event = classify_plain_event(body, level)
        if event == "Request" and ":" in body:
            body = body.split(":", 1)[1].strip()

    fields: Dict[str, str] = {}
    for item in KEY_VALUE_RE.finditer(body or ""):
        value = item.group("value")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        value = value.rstrip(",")
        fields[item.group("key")] = value
    return component, event, body.strip(), fields


def parse_asyncssh_context(context: str) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    if not context:
        return fields
    for item in KEY_VALUE_RE.finditer(context.strip("[]")):
        fields[item.group("key")] = item.group("value").rstrip(",")
    return fields


def classify_asyncssh_body(body: str) -> str:
    text = body.strip()
    if text.startswith("Creating SSH listener"):
        return "ssh_listener_started"
    if text.startswith("Accepted SSH client connection"):
        return "ssh_connection_accepted"
    if text.startswith("Sending server host keys disabled"):
        return "ssh_host_keys_disabled"
    if text.startswith("Local address:"):
        return "ssh_local_address"
    if text.startswith("Peer address:"):
        return "ssh_peer_address"
    if text.startswith("Beginning auth"):
        return "ssh_auth_started"
    if text.startswith("Auth for user"):
        lowered = text.lower()
        if "succeed" in lowered:
            return "ssh_auth_succeeded"
        if "fail" in lowered:
            return "ssh_auth_failed"
        return "ssh_auth"
    if text.startswith("New SSH session requested"):
        return "ssh_session_requested"
    if text.startswith("PTY created"):
        return "ssh_pty_created"
    if text.startswith("Line editor enabled"):
        return "ssh_line_editor_enabled"
    if text.startswith("Command:"):
        return "ssh_command_started"
    if text.startswith("Sending exit status"):
        return "ssh_exit_status_sent"
    if text.startswith("Closing channel"):
        return "ssh_channel_closing"
    if text.startswith("Received channel close"):
        return "ssh_channel_close_received"
    if text.startswith("Channel closed"):
        return "ssh_channel_closed"
    return "asyncssh"


def classify_kubessh_body(body: str) -> str:
    text = body.strip()
    if text.startswith("Loaded host key"):
        return "kubessh_host_key_loaded"
    if text.startswith("Login attempted by"):
        return "kubessh_login_attempted"
    if text.startswith("PVC ") and " already exists" in text:
        return "kubessh_pvc_exists"
    if text.startswith("Loop exited:"):
        return "kubessh_loop_exited"
    if text.startswith("TTY cleanup completed"):
        return "kubessh_tty_cleanup_completed"
    if text.startswith("TTY shutdown"):
        return "kubessh_tty_shutdown"
    if text.startswith("Non-TTY cleanup completed"):
        return "kubessh_nontty_cleanup_completed"
    if text.startswith("SSH connection disconnected"):
        return "kubessh_connection_disconnected"
    return "kubessh"


def normalize_dcusshk8s_message(
    message: str,
    level: str,
    component: str,
    event: str,
    message_body: str,
    fields: Dict[str, str],
) -> Tuple[str, str, str, str, Dict[str, str]]:
    text = (message or "").strip()
    fields = dict(fields)

    asyncssh_match = ASYNCSSH_LOG_RE.match(text)
    if asyncssh_match:
        component = "asyncssh"
        context = asyncssh_match.group("context") or ""
        body = (asyncssh_match.group("body") or "").strip()
        fields.update(parse_asyncssh_context(context))
        event = classify_asyncssh_body(body)
        message_body = body

        auth_match = AUTH_USER_RE.search(body)
        if auth_match:
            fields["username"] = auth_match.group("username")

        status_match = EXIT_STATUS_RE.search(body)
        if status_match:
            fields["exit_status"] = status_match.group("exit_status")

        port_match = PORT_RE.search(body)
        if port_match:
            fields["port"] = port_match.group("port")

        if body.startswith("Command:"):
            command_text = body.split("Command:", 1)[1].strip()
            fields["remote_command"] = command_text
            command_match = SIM_COMMAND_RE.search(command_text)
            if command_match:
                fields["request_id"] = command_match.group("request_id")
                fields["request_label"] = command_match.group("request_id")
                fields["command_name"] = command_match.group("command_name")
            elif command_text:
                fields.setdefault("command_name", command_text.split(maxsplit=1)[0])

        return component, event, message_body, level, fields

    kubessh_match = KUBESSH_LOG_RE.match(text)
    if kubessh_match:
        component = "KubeSSH"
        body = (kubessh_match.group("body") or "").strip()
        event = classify_kubessh_body(body)
        message_body = body
        if "user" in fields and "username" not in fields:
            fields["username"] = fields["user"]
        if "session" in fields and "session_id" not in fields:
            fields["session_id"] = fields["session"]
        login_match = LOGIN_ATTEMPT_RE.search(body)
        if login_match:
            fields["username"] = login_match.group("username")
        session_match = SESSION_ID_RE.search(body)
        if session_match:
            fields["session_id"] = session_match.group("session_id")
        return component, event, message_body, level, fields

    if "RuntimeWarning:" in text:
        event = "runtime_warning"
        level = level or "WARNING"
    elif text.startswith("Traceback ") or text.startswith("File ") or text.startswith('File "'):
        event = "traceback"
        level = level or "ERROR"
    elif not event and text:
        event = "log"

    return component, event, message_body, level, fields


def identify_module(labels: Dict[str, object], pod_name: str, container_name: str) -> str:
    app_label = str(labels.get("app") or "")
    kubessh_label = str(labels.get("kubessh") or "")
    container_value = container_name.lower()
    pod_value = pod_name.lower()

    if app_label == "controller":
        return "controller"
    if app_label == "warm-pod-pool":
        return "compute"
    if app_label == "controller-queue-redis":
        return "redis"
    if app_label == "fluent-bit":
        return "fluent-bit"
    if kubessh_label in {"swlabssh", "userpods"}:
        return "dcusshk8s"
    if pod_name.startswith("controller-"):
        return "controller"
    if pod_name.startswith("compute-general-") or container_name == "compute-agent":
        return "compute"
    if pod_name.startswith("ssh-") or "kubessh" in container_value or "kubessh" in pod_value:
        return "dcusshk8s"
    if pod_name.startswith("controller-queue-redis-") or container_name == "redis":
        return "redis"
    if pod_name.startswith("fluent-bit-"):
        return "fluent-bit"
    return app_label or kubessh_label or "unknown"


def event_time(record: Dict[str, object], app_log: Dict[str, object]) -> Optional[datetime]:
    for key in ("ts", "asctime", "time", "timestamp", "@timestamp"):
        parsed = parse_datetime_value(app_log.get(key))
        if parsed:
            return parsed
    for key in ("time", "@timestamp", "timestamp", "date"):
        parsed = parse_datetime_value(record.get(key))
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
    kubernetes = record.get("kubernetes") if isinstance(record.get("kubernetes"), dict) else {}
    labels = kubernetes.get("labels") if isinstance(kubernetes.get("labels"), dict) else {}
    pod_name = str(kubernetes.get("pod_name") or "")
    container_name = str(kubernetes.get("container_name") or "")
    level, message = normalize_message_level(str(app_log.get("level") or app_log.get("levelname") or ""), message)
    component, event, message_body, fields = parse_message_fields(message, level)
    module = identify_module(labels, pod_name, container_name)
    if module == "dcusshk8s":
        component, event, message_body, level, fields = normalize_dcusshk8s_message(
            message,
            level,
            component,
            event,
            message_body,
            fields,
        )
    compute_pod = fields.get("compute_pod") or ""
    if module == "compute" and not compute_pod:
        compute_pod = pod_name

    row = {column: "" for column in TIMELINE_COLUMNS}
    row.update(
        {
            "timestamp": format_timestamp(dt),
            "_ts_ms": str(int(dt.timestamp() * 1000)),
            "node": str(kubernetes.get("host") or kubernetes.get("node_name") or ""),
            "pod": pod_name,
            "compute_pod": compute_pod,
            "module": module,
            "level": level,
            "request_label": str(app_log.get("request_label") or ""),
            "component": component,
            "event": event,
            "message": message_body,
            "source_file": str(source_file),
        }
    )
    row.update(fields)
    row["resource"] = module
    return row


def enrich_dcusshk8s_events(events: List[Dict[str, str]]) -> None:
    users_by_connection: Dict[Tuple[str, str], Dict[str, str]] = {}
    requests_by_channel: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    for event in events:
        if event.get("module") != "dcusshk8s":
            continue

        pod = event.get("pod") or ""
        conn = event.get("conn") or ""
        chan = event.get("chan") or ""
        if conn:
            connection_key = (pod, conn)
            if event.get("username"):
                users_by_connection[connection_key] = {"username": event["username"]}
            elif connection_key in users_by_connection:
                event.update(users_by_connection[connection_key])

        if not (conn and chan):
            continue

        channel_key = (pod, conn, chan)
        if event.get("request_id") or event.get("command_name"):
            stored = {
                key: value
                for key in ("request_id", "request_label", "command_name", "username")
                if (value := event.get(key))
            }
            if stored:
                requests_by_channel[channel_key] = stored
            continue

        stored = requests_by_channel.get(channel_key)
        if not stored:
            continue
        for key, value in stored.items():
            if not event.get(key):
                event[key] = value


def read_log_events(paths: List[Path], bucket_seconds: int) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []
    for path in paths:
        with open_log_file(path) as handle:
            for line in handle:
                row = parse_fluent_bit_record(line, path, bucket_seconds)
                if row:
                    events.append(row)
    events.sort(key=lambda item: (int(item.get("_ts_ms") or 0), item.get("source_file") or ""))
    enrich_dcusshk8s_events(events)
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
        row.update(event)
        rows_by_resource.setdefault(resource, []).append(row)

    for resource in sorted(rows_by_resource):
        path = resource_dir / f"{safe_filename(resource)}.csv"
        write_csv(path, rows_by_resource[resource], TIMELINE_COLUMNS)
        print(f"wrote {path}")


def remove_stale_event_csvs(out_dir: Path) -> None:
    for name in ("controller_events.csv", "compute_events.csv"):
        path = out_dir / name
        if path.exists():
            path.unlink()


def write_analysis_outputs(raw_dir: Path, out_dir: Path, bucket_seconds: int) -> None:
    log_paths = iter_input_paths([str(raw_dir)])
    remove_stale_event_csvs(out_dir)
    if not log_paths:
        print("No JSONL files were found. Generate a request first, then run this command again.")
        return

    events = read_log_events(log_paths, bucket_seconds)

    timeline_rows = []
    for event in events:
        row = {column: "" for column in TIMELINE_COLUMNS}
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


def resolve_output_dir(args: argparse.Namespace) -> Path:
    if args.out_dir:
        return Path(args.out_dir).resolve()

    results_dir = Path(args.results_dir).resolve()
    run_dir_name = str(args.run_dir or os.getenv(RUN_DIR_ENV) or "").strip()
    if run_dir_name:
        safe_name = safe_filename(run_dir_name)
        if safe_name != run_dir_name or safe_name in {"", ".", ".."}:
            raise SystemExit(f"Unsafe run directory name: {run_dir_name!r}")
        return (results_dir / safe_name).resolve()

    experiment_name = str(args.experiment_name or os.getenv(EXPERIMENT_NAME_ENV) or "logs")
    result_name = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_{safe_filename(experiment_name)}"
    return (results_dir / result_name).resolve()


def main() -> int:
    args = parse_args()
    out_dir = resolve_output_dir(args)
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
