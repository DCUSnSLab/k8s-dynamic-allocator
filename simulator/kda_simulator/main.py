from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .config import (
    CLIENT_MAX_INFLIGHT,
    COMMAND_TIMEOUT_SECONDS,
    NHPP_DAILY_PROFILE,
    SimulatorConfig,
    WRITER_QUEUE_SIZE,
    _lambda_for_profile,
    load_config,
    validate_config,
)
from .metrics import AsyncJsonlWriter, SummaryCollector
from .scheduler import ScheduledRequest, generate_schedule, iter_schedule
from .workload import SelectedCommand, WeightedCommandSelector, sanitize_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a real-cluster KDA SSH workload simulation.")
    parser.add_argument("--config", default="simulator/scenario_config.yaml", help="Path to simulator YAML config.")
    parser.add_argument("--dry-run", action="store_true", help="Validate config and generate schedule without SSH.")
    parser.add_argument("--experiment-name", help="Override experiment.name.")
    parser.add_argument("--users", type=int, help="Override users.count.")
    parser.add_argument("--profile", help="Override workload.profile.")
    parser.add_argument("--random-seed", type=int, help="Override workload.random_seed.")
    parser.add_argument("--duration-minutes", type=float, help="Override workload duration in minutes.")
    parser.add_argument("--max-requests", type=int, help="Override workload.max_requests.")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    apply_overrides(config, args)
    validate_config(config)

    rng = random.Random(config.workload.random_seed)
    selector = WeightedCommandSelector(config.commands, rng)

    if args.dry_run:
        preview_limit = config.workload.max_requests or 10
        schedule = generate_schedule(config, rng, limit=preview_limit)
        plans = [build_request_plan(config, item, selector) for item in schedule]
        print_dry_run(config, plans)
        return

    await run_simulation(config, iter_schedule(config, rng), selector)


def apply_overrides(config: SimulatorConfig, args: argparse.Namespace) -> None:
    if args.experiment_name:
        config.experiment.name = args.experiment_name
    if args.users is not None:
        config.users.count = args.users
    if args.profile is not None:
        profile = parse_profile_override(args.profile)
        config.workload.profile = profile
        config.workload.scenario = str(profile)
        config.workload.mode = "nhpp" if profile == "nhpp_daily" else "constant"
        config.workload.lambda_per_hour = _lambda_for_profile(profile)
        config.workload.nhpp_profile_per_hour = (
            list(NHPP_DAILY_PROFILE)
            if config.workload.mode == "nhpp"
            else []
        )
    if args.random_seed is not None:
        config.workload.random_seed = args.random_seed
    if isinstance(config.workload.profile, (int, float)):
        config.workload.scenario = str(config.workload.profile)
        config.workload.mode = "constant"
    if args.duration_minutes is not None:
        config.workload.duration_minutes = args.duration_minutes
    if args.max_requests is not None:
        config.workload.max_requests = args.max_requests


def parse_profile_override(value: str) -> str | float:
    try:
        return float(value)
    except ValueError:
        return value


def build_request_plan(
    config: SimulatorConfig,
    item: ScheduledRequest,
    selector: WeightedCommandSelector,
) -> dict[str, Any]:
    experiment_id = sanitize_id(config.experiment.name)
    request_id = f"{experiment_id}-{item.sequence:06d}"
    selected = selector.choose(request_id)
    return {
        "request_id": request_id,
        "sequence": item.sequence,
        "planned_offset_seconds": item.planned_offset_seconds,
        "username": item.username,
        "command": selected,
    }


def print_dry_run(config: SimulatorConfig, plans: list[dict[str, Any]]) -> None:
    print(f"experiment_name={config.experiment.name}")
    print(f"mode={config.workload.mode}")
    print(f"scenario={config.workload.scenario}")
    print(f"lambda_scope={config.workload.lambda_scope}")
    print(f"users={config.users.count}")
    max_requests = (
        str(config.workload.max_requests)
        if config.workload.max_requests is not None
        else "unlimited"
    )
    print(f"max_requests={max_requests}")
    print(f"preview_requests={len(plans)}")
    for plan in plans[:10]:
        command: SelectedCommand = plan["command"]
        print(
            f"{plan['sequence']:06d} "
            f"t+{plan['planned_offset_seconds']:.3f}s "
            f"user={plan['username']} "
            f"command={command.name} "
            f"request_id={plan['request_id']}"
        )
    if len(plans) > 10:
        print(f"... {len(plans) - 10} more")


async def run_simulation(
    config: SimulatorConfig,
    scheduled_requests: Iterator[ScheduledRequest],
    selector: WeightedCommandSelector,
) -> None:
    from .ssh_runner import SSHSessionPool, strip_ansi

    output_dir = make_output_dir(config)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "config.json").write_text(config.to_json() + "\n", encoding="utf-8")

    writer = AsyncJsonlWriter(output_dir / "requests.jsonl", WRITER_QUEUE_SIZE)
    summary = SummaryCollector()
    pool = SSHSessionPool(config)
    tasks: list[asyncio.Task[None]] = []
    stopped_by_interrupt = False

    print(f"Output: {output_dir}")
    if config.workload.duration_minutes is None and config.workload.max_requests is None:
        print("Running until Ctrl+C")
    elif config.workload.max_requests is not None:
        print(f"Running up to {config.workload.max_requests} scheduled requests")
    else:
        print(f"Running for {config.workload.duration_minutes} minutes of scheduled workload")

    await writer.start()
    try:
        await warmup_users(config, pool)

        started_mono = time.monotonic()
        started_wall = now_iso()
        global_sem = asyncio.Semaphore(CLIENT_MAX_INFLIGHT)

        for scheduled in scheduled_requests:
            plan = build_request_plan(config, scheduled, selector)
            target_mono = started_mono + float(plan["planned_offset_seconds"])
            sleep_for = target_mono - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            tasks.append(
                asyncio.create_task(
                    execute_request(
                        config=config,
                        pool=pool,
                        writer=writer,
                        summary=summary,
                        global_sem=global_sem,
                        experiment_started_wall=started_wall,
                        experiment_started_mono=started_mono,
                        plan=plan,
                        strip_output=strip_ansi,
                    )
                )
            )

        if tasks:
            await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        stopped_by_interrupt = True
    finally:
        pending = [task for task in tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await pool.close()
        await writer.close()

    summary_payload = {
        "experiment": config.experiment.__dict__,
        "ssh": {"host": config.ssh.host, "port": config.ssh.port},
        "users": {"count": config.users.count, "prefix": config.users.prefix},
        "workload": {
            "mode": config.workload.mode,
            "scenario": config.workload.scenario,
            "lambda_scope": config.workload.lambda_scope,
        },
        "summary": summary.to_dict(),
        "stopped_by_interrupt": stopped_by_interrupt,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"Completed {summary.total} requests")
    print(f"Summary: {output_dir / 'summary.json'}")


async def warmup_users(config: SimulatorConfig, pool: Any) -> None:
    print(f"Warming up {config.users.count} user pods...")
    sem = asyncio.Semaphore(CLIENT_MAX_INFLIGHT)

    async def _warm(username: str) -> tuple[str, Any]:
        async with sem:
            return username, await pool.get(username).warmup()

    results = await asyncio.gather(*(_warm(username) for username in config.user_names()))
    failed = [
        (username, result.error or result.exit_status)
        for username, result in results
        if result.error or result.timed_out or result.exit_status not in (0, None)
    ]
    if failed:
        preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
        raise RuntimeError(f"Warm-up failed for {len(failed)} users: {preview}")
    print("Warm-up complete")


async def execute_request(
    *,
    config: SimulatorConfig,
    pool: Any,
    writer: AsyncJsonlWriter,
    summary: SummaryCollector,
    global_sem: asyncio.Semaphore,
    experiment_started_wall: str,
    experiment_started_mono: float,
    plan: dict[str, Any],
    strip_output: Any,
) -> None:
    command: SelectedCommand = plan["command"]
    scheduled_mono = experiment_started_mono + float(plan["planned_offset_seconds"])
    task_created_mono = time.monotonic()
    schedule_lag_ms = max(0.0, (task_created_mono - scheduled_mono) * 1000.0)
    queue_started_mono = task_created_mono

    async with global_sem:
        acquired_mono = time.monotonic()
        client_queue_delay_ms = (acquired_mono - queue_started_mono) * 1000.0
        started_at = now_iso()
        result = await pool.get(plan["username"]).run_remote(
            command.remote_command,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
        finished_at = now_iso()

    status = classify_result(result)
    record = {
        "schema_version": 1,
        "experiment_name": config.experiment.name,
        "experiment_started_at": experiment_started_wall,
        "request_id": plan["request_id"],
        "sequence": plan["sequence"],
        "username": plan["username"],
        "planned_offset_seconds": plan["planned_offset_seconds"],
        "started_at": started_at,
        "finished_at": finished_at,
        "command_name": command.name,
        "command": command.command,
        "remote_command": command.remote_command,
        "status": status,
        "exit_status": result.exit_status,
        "duration_ms": result.elapsed_ms,
        "schedule_lag_ms": schedule_lag_ms,
        "client_queue_delay_ms": client_queue_delay_ms,
        "per_user_queue_delay_ms": result.per_user_queue_delay_ms,
        "ticket_id": result.ticket_id,
        "compute_pod": result.compute_pod,
        "compute_pod_ip": result.compute_pod_ip,
        "timed_out": result.timed_out,
        "error": result.error,
    }
    add_output_tails(config, record, result, strip_output)
    summary.add(record)
    await writer.write(record)


def classify_result(result: Any) -> str:
    if result.timed_out:
        return "timeout"
    if result.error:
        return "error"
    if result.exit_status == 0:
        return "success"
    return "exit_nonzero"


def add_output_tails(config: SimulatorConfig, record: dict[str, Any], result: Any, strip_output: Any) -> None:
    limit = config.output.command_output_chars
    if limit < 0:
        return
    stdout = strip_output(result.stdout or "")
    stderr = strip_output(result.stderr or "")
    if limit == 0:
        record["stdout"] = stdout
        record["stderr"] = stderr
        return
    record["stdout_tail"] = stdout[-limit:]
    record["stderr_tail"] = stderr[-limit:]


def make_output_dir(config: SimulatorConfig) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    experiment_id = sanitize_id(config.experiment.name)
    return Path(config.output.dir) / f"{timestamp}_{experiment_id}"


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")
