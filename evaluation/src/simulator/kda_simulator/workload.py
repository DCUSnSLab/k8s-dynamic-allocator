from __future__ import annotations

import random
import re
import shlex
from dataclasses import dataclass

from .config import (
    EXECUTION_MODE_BASELINE_DIRECT,
    EXECUTION_MODE_KDA,
    MARKER_PREFIX,
    RUN_COMMAND,
    CommandItem,
    CommandsConfig,
)


SAFE_ID_RE = re.compile(r"[^A-Za-z0-9_.-]+")


@dataclass(frozen=True)
class SelectedCommand:
    name: str
    command: str
    remote_command: str


class WeightedCommandSelector:
    def __init__(
        self,
        config: CommandsConfig,
        rng: random.Random,
        execution_mode: str = EXECUTION_MODE_KDA,
    ) -> None:
        self._config = config
        self._rng = rng
        self._execution_mode = execution_mode
        self._items = list(config.items)
        self._weights = [item.weight for item in self._items]

    def choose(self, request_id: str) -> SelectedCommand:
        item = self._rng.choices(self._items, weights=self._weights, k=1)[0]
        return SelectedCommand(
            name=item.name,
            command=item.command,
            remote_command=build_remote_command(
                self._config,
                item,
                request_id,
                self._execution_mode,
            ),
        )


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
