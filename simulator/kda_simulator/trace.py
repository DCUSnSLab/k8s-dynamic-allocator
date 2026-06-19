from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import SimulatorConfig
from .scheduler import iter_schedule
from .workload import WeightedCommandSelector


TRACE_COLUMNS = [
    "sequence",
    "planned_offset_seconds",
    "username",
    "command_name",
    "command",
]


@dataclass(frozen=True)
class TraceEntry:
    sequence: int
    planned_offset_seconds: float
    username: str
    command_name: str
    command: str


def generate_trace_entries(
    config: SimulatorConfig,
    rng: random.Random,
    selector: WeightedCommandSelector,
) -> list[TraceEntry]:
    if config.workload.duration_minutes is None and config.workload.max_requests is None:
        raise ValueError("trace generation requires workload.duration_minutes or workload.max_requests")

    entries: list[TraceEntry] = []
    for scheduled in iter_schedule(config, rng):
        request_id = f"trace-{scheduled.sequence:06d}"
        command = selector.choose(request_id)
        entries.append(
            TraceEntry(
                sequence=scheduled.sequence,
                planned_offset_seconds=scheduled.planned_offset_seconds,
                username=scheduled.username,
                command_name=command.name,
                command=command.command,
            )
        )
    return entries


def read_trace_csv(path: str | Path) -> list[TraceEntry]:
    path = Path(path)
    entries: list[TraceEntry] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = sorted(set(TRACE_COLUMNS) - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"trace file is missing column(s): {', '.join(missing)}")
        for row_number, row in enumerate(reader, start=2):
            try:
                entries.append(
                    TraceEntry(
                        sequence=int(row["sequence"]),
                        planned_offset_seconds=float(row["planned_offset_seconds"]),
                        username=str(row["username"]),
                        command_name=str(row["command_name"]),
                        command=str(row["command"]),
                    )
                )
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid trace row {row_number}: {row}") from exc
    return entries


def write_trace_csv(path: str | Path, entries: Iterable[TraceEntry]) -> int:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=TRACE_COLUMNS)
        writer.writeheader()
        for entry in entries:
            writer.writerow(
                {
                    "sequence": entry.sequence,
                    "planned_offset_seconds": f"{entry.planned_offset_seconds:.6f}",
                    "username": entry.username,
                    "command_name": entry.command_name,
                    "command": entry.command,
                }
            )
            count += 1
    return count
