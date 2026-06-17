from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


CONSTANT_WORKLOAD_PROFILES = {
    "avg": 8.7232,
    "p95_high": 41.0,
    "p99_peak": 150.68,
    "max_stress": 903.0,
}

NHPP_DAILY_PROFILE = [
    5.0525,
    4.1479,
    2.6615,
    1.4066,
    1.2451,
    0.7393,
    0.5136,
    0.6245,
    1.0895,
    4.0447,
    11.3696,
    8.5253,
    7.2043,
    13.6031,
    33.8560,
    38.0992,
    20.0525,
    6.3521,
    7.3891,
    7.7179,
    8.2471,
    8.7388,
    8.6881,
    7.9864,
]

# Internal simulator behavior. These are intentionally not YAML fields.
RUN_COMMAND = "run"
EXECUTION_MODE_KDA = "kda"
EXECUTION_MODE_BASELINE_DIRECT = "baseline-direct"
EXECUTION_MODES = {EXECUTION_MODE_KDA, EXECUTION_MODE_BASELINE_DIRECT}
MARKER_PREFIX = "KDA_SIM"
CLIENT_MAX_INFLIGHT = 100
PER_USER_MAX_INFLIGHT = 1
CONNECT_TIMEOUT_SECONDS = 30.0
COMMAND_TIMEOUT_SECONDS = 1800.0
WRITER_QUEUE_SIZE = 10000
PTY_TERM_TYPE = "xterm"
PTY_WIDTH = 120
PTY_HEIGHT = 40


@dataclass
class ExperimentConfig:
    name: str
    pool_policy: str = ""
    pool_size: int | None = None
    controller_replicas: int | None = None


@dataclass
class SSHConfig:
    host: str
    port: int


@dataclass
class UsersConfig:
    prefix: str
    count: int


@dataclass
class WorkloadConfig:
    profile: str | float
    lambda_scope: str
    duration_minutes: float | None
    max_requests: int | None
    random_seed: int | None
    nhpp_time_compression: float
    nhpp_start_hour: int | None
    mode: str
    scenario: str
    lambda_per_hour: float
    nhpp_profile_per_hour: list[float] = field(default_factory=list)


@dataclass
class CommandItem:
    name: str
    weight: float
    command: str


@dataclass
class CommandsConfig:
    items: list[CommandItem]


@dataclass
class ExecutionConfig:
    mode: str = EXECUTION_MODE_KDA


@dataclass
class OutputConfig:
    dir: str
    command_output_chars: int


@dataclass
class SimulatorConfig:
    experiment: ExperimentConfig
    ssh: SSHConfig
    users: UsersConfig
    workload: WorkloadConfig
    commands: CommandsConfig
    execution: ExecutionConfig
    output: OutputConfig

    def user_names(self) -> list[str]:
        return [f"{self.users.prefix}{idx}" for idx in range(1, self.users.count + 1)]

    def password_for(self, username: str) -> str:
        return username

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)

    def expects_ticket(self) -> bool:
        return self.execution.mode == EXECUTION_MODE_KDA

    def expects_compute_allocation(self) -> bool:
        return self.execution.mode == EXECUTION_MODE_KDA


def load_config(path: str | Path) -> SimulatorConfig:
    path = Path(path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data = _mapping(raw, "root")
    _ensure_allowed(
        "root",
        data,
        {"experiment", "ssh", "users", "workload", "commands", "execution", "output"},
    )

    config = SimulatorConfig(
        experiment=_load_experiment(_required_section(data, "experiment")),
        ssh=_load_ssh(_required_section(data, "ssh")),
        users=_load_users(_required_section(data, "users")),
        workload=_load_workload(_required_section(data, "workload")),
        commands=_load_commands(_required_section(data, "commands")),
        execution=_load_execution(_optional_section(data, "execution")),
        output=_load_output(_required_section(data, "output")),
    )
    validate_config(config)
    return config


def _load_experiment(data: dict[str, Any]) -> ExperimentConfig:
    _ensure_allowed(
        "experiment",
        data,
        {"name", "pool_policy", "pool_size", "controller_replicas"},
    )
    return ExperimentConfig(
        name=str(_required_value(data, "name", "experiment")),
        pool_policy=str(data.get("pool_policy") or ""),
        pool_size=_optional_int(data.get("pool_size"), "experiment.pool_size"),
        controller_replicas=_optional_int(
            data.get("controller_replicas"), "experiment.controller_replicas"
        ),
    )


def _load_ssh(data: dict[str, Any]) -> SSHConfig:
    _ensure_allowed("ssh", data, {"host", "port"})
    return SSHConfig(
        host=str(_required_value(data, "host", "ssh")),
        port=int(_required_value(data, "port", "ssh")),
    )


def _load_users(data: dict[str, Any]) -> UsersConfig:
    _ensure_allowed("users", data, {"prefix", "count"})
    return UsersConfig(
        prefix=str(data.get("prefix") or "test"),
        count=int(_required_value(data, "count", "users")),
    )


def _load_workload(data: dict[str, Any]) -> WorkloadConfig:
    _ensure_allowed(
        "workload",
        data,
        {
            "profile",
            "lambda_scope",
            "duration_minutes",
            "max_requests",
            "random_seed",
            "nhpp_time_compression",
            "nhpp_start_hour",
        },
    )
    profile = _required_value(data, "profile", "workload")
    mode = "nhpp" if profile == "nhpp_daily" else "constant"
    scenario = str(profile)
    lambda_per_hour = _lambda_for_profile(profile)
    return WorkloadConfig(
        profile=profile,
        lambda_scope=str(data.get("lambda_scope") or "system"),
        duration_minutes=_optional_float(
            data.get("duration_minutes"), "workload.duration_minutes"
        ),
        max_requests=_optional_int(data.get("max_requests"), "workload.max_requests"),
        random_seed=_optional_int(data.get("random_seed"), "workload.random_seed"),
        nhpp_time_compression=float(data.get("nhpp_time_compression", 12.0)),
        nhpp_start_hour=_optional_int(data.get("nhpp_start_hour"), "workload.nhpp_start_hour"),
        mode=mode,
        scenario=scenario,
        lambda_per_hour=lambda_per_hour,
        nhpp_profile_per_hour=list(NHPP_DAILY_PROFILE) if mode == "nhpp" else [],
    )


def _load_commands(data: dict[str, Any]) -> CommandsConfig:
    _ensure_allowed("commands", data, {"items"})
    raw_items = _required_value(data, "items", "commands")
    if not isinstance(raw_items, list):
        raise ValueError("commands.items must be a list")
    items: list[CommandItem] = []
    for index, raw_item in enumerate(raw_items):
        item = _mapping(raw_item, f"commands.items[{index}]")
        _ensure_allowed(f"commands.items[{index}]", item, {"name", "weight", "command"})
        items.append(
            CommandItem(
                name=str(_required_value(item, "name", f"commands.items[{index}]")),
                weight=float(_required_value(item, "weight", f"commands.items[{index}]")),
                command=str(
                    _required_value(item, "command", f"commands.items[{index}]")
                ),
            )
        )
    return CommandsConfig(items=items)


def _load_execution(data: dict[str, Any]) -> ExecutionConfig:
    _ensure_allowed("execution", data, {"mode"})
    return ExecutionConfig(mode=str(data.get("mode") or EXECUTION_MODE_KDA))


def _load_output(data: dict[str, Any]) -> OutputConfig:
    _ensure_allowed("output", data, {"dir", "command_output_chars"})
    return OutputConfig(
        dir=str(data.get("dir") or "simulator/results"),
        command_output_chars=int(data.get("command_output_chars", 0)),
    )


def _lambda_for_profile(profile: str | float | int) -> float:
    if isinstance(profile, (int, float)):
        value = float(profile)
        if value <= 0:
            raise ValueError("numeric workload.profile must be positive")
        return value
    if isinstance(profile, str):
        try:
            value = float(profile)
        except ValueError:
            pass
        else:
            if value <= 0:
                raise ValueError("numeric workload.profile must be positive")
            return value

    if profile in CONSTANT_WORKLOAD_PROFILES:
        return CONSTANT_WORKLOAD_PROFILES[str(profile)]
    if profile == "nhpp_daily":
        return CONSTANT_WORKLOAD_PROFILES["avg"]
    choices = ", ".join([*CONSTANT_WORKLOAD_PROFILES.keys(), "nhpp_daily", "<number>"])
    raise ValueError(f"Unsupported workload.profile: {profile}. Choices: {choices}")


def validate_config(config: SimulatorConfig) -> None:
    if not config.experiment.name:
        raise ValueError("experiment.name must be set")
    if not config.ssh.host:
        raise ValueError("ssh.host must be set before running the simulator")
    if config.ssh.port <= 0:
        raise ValueError("ssh.port must be a positive integer")
    if config.users.count <= 0:
        raise ValueError("users.count must be positive")
    if config.workload.lambda_scope not in {"system", "per_user"}:
        raise ValueError("workload.lambda_scope must be 'system' or 'per_user'")
    if config.workload.duration_minutes is not None and config.workload.duration_minutes <= 0:
        raise ValueError("workload.duration_minutes must be positive or null")
    if config.workload.max_requests is not None and config.workload.max_requests <= 0:
        raise ValueError("workload.max_requests must be positive or null")
    if config.workload.mode == "constant":
        if config.workload.lambda_per_hour <= 0:
            raise ValueError("workload.profile must resolve to a positive lambda")
    elif config.workload.mode == "nhpp":
        profile = config.workload.nhpp_profile_per_hour
        if len(profile) != 24:
            raise ValueError("nhpp_daily profile must contain 24 hourly lambdas")
        if any(rate < 0 for rate in profile):
            raise ValueError("nhpp_daily profile cannot contain negative rates")
        if config.workload.nhpp_time_compression <= 0:
            raise ValueError("workload.nhpp_time_compression must be positive")
        if config.workload.nhpp_start_hour is not None and not (
            0 <= config.workload.nhpp_start_hour <= 23
        ):
            raise ValueError("workload.nhpp_start_hour must be between 0 and 23 or null")
    else:
        raise ValueError("workload.profile must resolve to constant or nhpp mode")
    if not config.commands.items:
        raise ValueError("commands.items must contain at least one command")
    if any(item.weight <= 0 for item in config.commands.items):
        raise ValueError("Every command weight must be positive")
    if config.execution.mode not in EXECUTION_MODES:
        choices = ", ".join(sorted(EXECUTION_MODES))
        raise ValueError(f"execution.mode must be one of: {choices}")


def _mapping(value: Any, section: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{section} must be a mapping")
    return value


def _required_section(data: dict[str, Any], name: str) -> dict[str, Any]:
    return _mapping(_required_value(data, name, "root"), name)


def _optional_section(data: dict[str, Any], name: str) -> dict[str, Any]:
    value = data.get(name)
    if value is None:
        return {}
    return _mapping(value, name)


def _required_value(data: dict[str, Any], name: str, section: str) -> Any:
    if name not in data:
        raise ValueError(f"{section}.{name} is required")
    return data[name]


def _ensure_allowed(section: str, data: dict[str, Any], allowed: set[str]) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        joined = ", ".join(unknown)
        raise ValueError(f"Unsupported config field(s) in {section}: {joined}")


def _optional_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer or null") from exc


def _optional_float(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a number or null") from exc
