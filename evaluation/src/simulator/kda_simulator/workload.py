from __future__ import annotations

import math
import random
import re
import shlex
from dataclasses import dataclass, replace

from .config import (
    EXECUTION_MODE_BASELINE_DIRECT,
    EXECUTION_MODE_KDA,
    MARKER_PREFIX,
    RUN_COMMAND,
    CommandItem,
    CommandsConfig,
    JobLengthConfig,
)


SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class SelectedCommand:
    name: str
    command: str
    remote_command: str
    # 이 요청의 작업 길이(1 코어 기준 초). 작업 길이를 쓰지 않는 실행은 None.
    work_seconds: float | None = None


class WeightedCommandSelector:
    def __init__(
        self,
        config: CommandsConfig,
        rng: random.Random,
        execution_mode: str = EXECUTION_MODE_KDA,
        job_length: JobLengthConfig | None = None,
    ) -> None:
        self._config = config
        self._rng = rng
        self._execution_mode = execution_mode
        self._job_length = job_length
        self._items = list(config.items)
        self._weights = [item.weight for item in self._items]

    def choose(self, request_id: str) -> SelectedCommand:
        item = self._rng.choices(self._items, weights=self._weights, k=1)[0]
        work_seconds = None
        if self._job_length is not None:
            # 명령 종류를 고른 뒤 같은 rng 로 길이를 뽑는다. seed 가 같으면 모든 비교군이
            # 같은 길이를 받는다.
            work_seconds = draw_work_seconds(self._job_length, self._rng)
            item = replace(item, command=render_job(item, self._job_length, work_seconds))
        return SelectedCommand(
            name=item.name,
            command=item.command,
            remote_command=build_remote_command(
                self._config,
                item,
                request_id,
                self._execution_mode,
            ),
            work_seconds=work_seconds,
        )


def draw_work_seconds(job: JobLengthConfig, rng: random.Random) -> float:
    """잘린 로그정규에서 작업 길이(초)를 뽑는다. 상한을 넘으면 버리고 다시 뽑는다."""
    mu = math.log(job.median_seconds)
    while True:
        value = rng.lognormvariate(mu, job.sigma)
        if value <= job.cap_seconds:
            return round(value, 3)


def render_job(item: CommandItem, job: JobLengthConfig, work_seconds: float) -> str:
    """명령 틀의 {units}(일의 양)와 {limit}(실행 시간 제한, 초)을 채운다.

    str.format 을 쓰지 않는다 - 파이썬 코드 안의 다른 중괄호를 건드리지 않게.
    """
    units = max(1, round(work_seconds * float(item.units_per_second or 0)))
    limit = math.ceil(work_seconds * job.time_limit_factor + job.time_limit_slack_seconds)
    return item.command.replace("{units}", str(units)).replace("{limit}", str(limit))


def sanitize_id(value: str) -> str:
    value = SAFE_ID_RE.sub("-", value).strip("-")
    return value or "request"


def build_remote_command(
    _config: CommandsConfig,
    item: CommandItem,
    request_id: str,
    execution_mode: str = EXECUTION_MODE_KDA,
) -> str:
    marker_label = sanitize_id(MARKER_PREFIX)
    safe_request_id = sanitize_id(request_id)
    safe_command_name = sanitize_id(item.name)
    script = (
        f"echo {marker_label}_START request_id={safe_request_id} command={safe_command_name}; "
        f"{item.command}; "
        "exit_code=$?; "
        f"echo {marker_label}_END request_id={safe_request_id} command={safe_command_name} exit_code=$exit_code; "
        "exit $exit_code"
    )

    if execution_mode == EXECUTION_MODE_BASELINE_DIRECT:
        argv = ["bash", "-lc", script]
    elif execution_mode == EXECUTION_MODE_KDA:
        argv = [RUN_COMMAND, "bash", "-lc", script]
    else:
        raise ValueError(f"Unsupported execution mode: {execution_mode}")
    return shlex.join(argv)
