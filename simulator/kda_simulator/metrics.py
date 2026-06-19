from __future__ import annotations

import asyncio
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class AsyncJsonlWriter:
    def __init__(self, path: Path, queue_size: int = 10000) -> None:
        self._path = path
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=queue_size)
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._task = asyncio.create_task(self._run())

    async def write(self, record: dict[str, Any]) -> None:
        await self._queue.put(record)

    async def close(self) -> None:
        await self._queue.put(None)
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        with self._path.open("a", encoding="utf-8") as file:
            while True:
                record = await self._queue.get()
                if record is None:
                    break
                file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


@dataclass
class SummaryCollector:
    total: int = 0
    status_counts: Counter[str] = field(default_factory=Counter)
    command_counts: Counter[str] = field(default_factory=Counter)
    durations_ms: list[float] = field(default_factory=list)
    schedule_lag_ms: list[float] = field(default_factory=list)
    client_queue_delay_ms: list[float] = field(default_factory=list)
    per_user_queue_delay_ms: list[float] = field(default_factory=list)
    ticket_missing: int = 0
    allocation_missing: int = 0
    by_command_status: dict[str, Counter[str]] = field(default_factory=lambda: defaultdict(Counter))

    def add(self, record: dict[str, Any]) -> None:
        self.total += 1
        status = str(record.get("status") or "unknown")
        command_name = str(record.get("command_name") or "unknown")
        self.status_counts[status] += 1
        self.command_counts[command_name] += 1
        self.by_command_status[command_name][status] += 1

        for target, key in (
            (self.durations_ms, "duration_ms"),
            (self.schedule_lag_ms, "schedule_lag_ms"),
            (self.client_queue_delay_ms, "client_queue_delay_ms"),
            (self.per_user_queue_delay_ms, "per_user_queue_delay_ms"),
        ):
            value = record.get(key)
            if isinstance(value, (int, float)) and math.isfinite(value):
                target.append(float(value))

        if record.get("ticket_expected", True) and not record.get("ticket_id"):
            self.ticket_missing += 1
        if record.get("compute_allocation_expected", True) and not record.get("compute_pod"):
            self.allocation_missing += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "status_counts": dict(self.status_counts),
            "command_counts": dict(self.command_counts),
            "by_command_status": {
                name: dict(counter) for name, counter in self.by_command_status.items()
            },
            "duration_ms": summarize_values(self.durations_ms),
            "schedule_lag_ms": summarize_values(self.schedule_lag_ms),
            "client_queue_delay_ms": summarize_values(self.client_queue_delay_ms),
            "per_user_queue_delay_ms": summarize_values(self.per_user_queue_delay_ms),
            "ticket_missing": self.ticket_missing,
            "allocation_missing": self.allocation_missing,
        }


def summarize_values(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "p99": None, "max": None, "avg": None}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "max": ordered[-1],
        "avg": sum(ordered) / len(ordered),
    }


def percentile(ordered_values: list[float], p: float) -> float:
    if not ordered_values:
        raise ValueError("ordered_values must not be empty")
    if len(ordered_values) == 1:
        return ordered_values[0]
    pos = (len(ordered_values) - 1) * p
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered_values[int(pos)]
    fraction = pos - lower
    return ordered_values[lower] * (1.0 - fraction) + ordered_values[upper] * fraction
