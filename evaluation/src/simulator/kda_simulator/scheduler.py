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


def iter_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    if config.workload.mode == "constant":
        yield from _iter_constant_schedule(config, rng)
        return
    if config.workload.mode == "nhpp":
        yield from _iter_nhpp_schedule(config, rng)
        return
    if config.workload.mode == "piecewise":
        yield from _iter_piecewise_schedule(config, rng)
        return
    raise ValueError(f"Unsupported workload mode: {config.workload.mode}")


def _iter_piecewise_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    """설계한 부하 모양(계단·산·파도). 계획의 각 상수 구간 안에서 포아송 도착을 만든다.

    구간마다 시작점에서 다시 지수 간격을 뽑는다. 포아송 과정은 기억이 없으므로
    구간 경계에서 끊었다 이어도 각 구간의 도착은 그 구간의 lambda 를 따른다.
    seed 가 같으면 모든 비교군이 같은 도착을 받는다(같은 rng 순서).
    """
    sequence = 0
    for piece in config.workload.piecewise_plan:
        start = piece["start_min"] * 60.0
        end = piece["end_min"] * 60.0
        rate_per_second = piece["lambda_per_minute"] / 60.0
        if rate_per_second <= 0:
            continue
        planned = start
        while True:
            planned += rng.expovariate(rate_per_second)
            if planned >= end:
                break
            sequence += 1
            yield ScheduledRequest(
                sequence=sequence,
                planned_offset_seconds=planned,
                username=_assign_user(config, rng),
            )
            if config.workload.max_requests is not None and sequence >= config.workload.max_requests:
                return


def _apply_scope(config: SimulatorConfig, lambda_per_minute: float) -> float:
    if config.workload.lambda_scope == "per_user":
        return lambda_per_minute * config.users.count
    return lambda_per_minute


def _assign_user(config: SimulatorConfig, rng: random.Random) -> str:
    users = config.user_names()
    return users[rng.randrange(len(users))]


def _duration_seconds(config: SimulatorConfig) -> float | None:
    if config.workload.duration_minutes is None:
        return None
    return config.workload.duration_minutes * 60.0


def _iter_constant_schedule(config: SimulatorConfig, rng: random.Random) -> Iterator[ScheduledRequest]:
    lambda_per_minute = _apply_scope(config, config.workload.lambda_per_minute)
    lambda_per_second = lambda_per_minute / 60.0
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
    profile = config.workload.nhpp_profile_per_minute
    start_hour = (
        config.workload.nhpp_start_hour
        if config.workload.nhpp_start_hour is not None
        else datetime.now().hour
    )
    sequence = 0
    segment_index = 0

    while True:
        hour = (start_hour + segment_index) % 24
        segment_start = segment_index * segment_seconds
        if duration is not None and segment_start >= duration:
            break

        lambda_per_minute = _apply_scope(config, profile[hour]) * config.workload.nhpp_rate_scale
        lambda_for_segment = lambda_per_minute * 60.0
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
