from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from .config import SimulatorConfig


@dataclass(frozen=True)
class ScheduledRequest:
    sequence: int
    planned_offset_seconds: float
    username: str


def generate_schedule(
    config: SimulatorConfig, rng: random.Random, limit: int | None = None
) -> list[ScheduledRequest]:
    events: list[ScheduledRequest] = []
    for event in iter_schedule(config, rng):
        events.append(event)
        if limit is not None and len(events) >= limit:
            break
    return events


def iter_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    if config.workload.mode == "constant":
        yield from _iter_constant_schedule(config, rng)
        return
    if config.workload.mode == "nhpp":
        yield from _iter_nhpp_schedule(config, rng)
        return
    raise ValueError(f"Unsupported workload mode: {config.workload.mode}")


def _apply_scope(config: SimulatorConfig, lambda_per_hour: float) -> float:
    if config.workload.lambda_scope == "per_user":
        return lambda_per_hour * config.users.count
    return lambda_per_hour


def _assign_user(config: SimulatorConfig, rng: random.Random) -> str:
    users = config.user_names()
    return users[rng.randrange(len(users))]


def _duration_seconds(config: SimulatorConfig) -> float | None:
    if config.workload.duration_minutes is None:
        return None
    return config.workload.duration_minutes * 60.0


def _iter_constant_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    lambda_per_hour = _apply_scope(config, config.workload.lambda_per_hour)
    lambda_per_second = lambda_per_hour / 3600.0
    duration = _duration_seconds(config)
    planned = 0.0
    sequence = 0

    while True:
        planned += rng.expovariate(lambda_per_second)
        if duration is not None and planned > duration:
            break
        sequence += 1
        yield ScheduledRequest(
            sequence=sequence,
            planned_offset_seconds=planned,
            username=_assign_user(config, rng),
        )
        if config.workload.max_requests is not None and sequence >= config.workload.max_requests:
            break


def _iter_nhpp_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    duration = _duration_seconds(config)
    segment_seconds = 3600.0 / config.workload.nhpp_time_compression
    profile = config.workload.nhpp_profile_per_hour
    start_hour = datetime.now().hour
    sequence = 0
    segment_index = 0

    while True:
        hour = (start_hour + segment_index) % 24
        segment_start = segment_index * segment_seconds
        if duration is not None and segment_start >= duration:
            break

        lambda_per_hour = _apply_scope(config, profile[hour])
        lambda_for_segment = lambda_per_hour
        if lambda_for_segment <= 0:
            segment_index += 1
            continue

        rate_per_run_second = lambda_for_segment / segment_seconds
        segment_end = segment_start + segment_seconds
        planned = segment_start

        while True:
            planned += rng.expovariate(rate_per_run_second)
            if planned >= segment_end:
                break
            if duration is not None and planned > duration:
                return
            sequence += 1
            yield ScheduledRequest(
                sequence=sequence,
                planned_offset_seconds=planned,
                username=_assign_user(config, rng),
            )
            if config.workload.max_requests is not None and sequence >= config.workload.max_requests:
                return
        segment_index += 1
