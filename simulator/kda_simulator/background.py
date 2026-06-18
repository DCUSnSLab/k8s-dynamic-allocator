from __future__ import annotations

import asyncio
import base64
import shlex
from typing import Any

from .config import CLIENT_MAX_INFLIGHT, SimulatorConfig


BACKGROUND_COMMAND_TIMEOUT_SECONDS = 60.0

BACKGROUND_SCRIPT = r"""#!/usr/bin/env python3
import argparse
import math
import signal
import sys
import time


running = True


def stop(_signum, _frame):
    global running
    running = False


def allocate_memory(memory_mb):
    if memory_mb <= 0:
        return bytearray()
    block = bytearray(memory_mb * 1024 * 1024)
    for index in range(0, len(block), 4096):
        block[index] = 1
    return block


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-mb", type=int, default=0)
    parser.add_argument("--cpu-period-seconds", type=float, default=10.0)
    parser.add_argument("--cpu-busy-seconds", type=float, default=0.0)
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    memory = allocate_memory(args.memory_mb)
    cursor = 0
    checksum = 0.0

    print(
        "KDA_BG_READY "
        f"memory_mb={args.memory_mb} "
        f"cpu_period_seconds={args.cpu_period_seconds} "
        f"cpu_busy_seconds={args.cpu_busy_seconds}",
        flush=True,
    )

    while running:
        cycle_started = time.monotonic()
        busy_until = cycle_started + args.cpu_busy_seconds
        i = 0
        while running and time.monotonic() < busy_until:
            checksum += math.sqrt(i % 1000000)
            i += 1

        if memory:
            memory[cursor] = (memory[cursor] + 1) % 256
            cursor = (cursor + 4096) % len(memory)

        elapsed = time.monotonic() - cycle_started
        sleep_for = max(0.0, args.cpu_period_seconds - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)

    print(f"KDA_BG_STOPPED checksum={checksum}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
"""


async def start_background_activity(config: SimulatorConfig, pool: Any) -> None:
    if not config.background_activity.enabled:
        return

    print(
        "Starting background activity "
        f"for {config.users.count} user pods "
        f"(memory={config.background_activity.memory_mb}Mi, "
        f"cpu={config.background_activity.cpu_busy_seconds}/"
        f"{config.background_activity.cpu_period_seconds}s)..."
    )
    command = build_start_command(config)
    results = await _run_for_users(config, pool, command)
    failed = [
        (username, result.error or result.exit_status)
        for username, result in results
        if result.error or result.timed_out or result.exit_status not in (0, None)
    ]
    if failed:
        preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
        raise RuntimeError(f"Background activity failed for {len(failed)} users: {preview}")
    print("Background activity started")


async def stop_background_activity(config: SimulatorConfig, pool: Any) -> None:
    if not config.background_activity.enabled:
        return

    print("Stopping background activity...")
    results = await _run_for_users(config, pool, build_stop_command(), return_exceptions=True)
    failed = []
    for username, result in results:
        if isinstance(result, BaseException):
            failed.append((username, result))
            continue
        if (
            getattr(result, "error", None)
            or getattr(result, "timed_out", False)
            or getattr(result, "exit_status", 0) not in (0, None)
        ):
            failed.append(
                (username, getattr(result, "error", None) or getattr(result, "exit_status", None))
            )
    if failed:
        preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
        print(f"Background activity stop warnings: {preview}")
    print("Background activity stopped")


async def _run_for_users(
    config: SimulatorConfig,
    pool: Any,
    command: str,
    *,
    return_exceptions: bool = False,
) -> list[tuple[str, Any]]:
    sem = asyncio.Semaphore(CLIENT_MAX_INFLIGHT)

    async def _run(username: str) -> tuple[str, Any]:
        async with sem:
            try:
                result = await pool.get(username).run_remote(
                    command,
                    timeout=BACKGROUND_COMMAND_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                if return_exceptions:
                    return username, exc
                raise
            return username, result

    return await asyncio.gather(
        *(_run(username) for username in config.user_names()),
        return_exceptions=False,
    )


def build_start_command(config: SimulatorConfig) -> str:
    payload = base64.b64encode(BACKGROUND_SCRIPT.encode("utf-8")).decode("ascii")
    activity = config.background_activity
    script = f"""
set -eu
mkdir -p "$HOME/.kda-sim"
python3 -c 'import base64, pathlib; pathlib.Path.home().joinpath(".kda-sim").mkdir(exist_ok=True); pathlib.Path.home().joinpath(".kda-sim/background_activity.py").write_bytes(base64.b64decode({payload!r}))'
if [ -f "$HOME/.kda-sim/background_activity.pid" ]; then
  old_pid="$(cat "$HOME/.kda-sim/background_activity.pid" 2>/dev/null || true)"
  if [ -n "$old_pid" ]; then
    kill "$old_pid" 2>/dev/null || true
  fi
fi
nohup python3 "$HOME/.kda-sim/background_activity.py" \
  --memory-mb {int(activity.memory_mb)} \
  --cpu-period-seconds {float(activity.cpu_period_seconds)} \
  --cpu-busy-seconds {float(activity.cpu_busy_seconds)} \
  > "$HOME/.kda-sim/background_activity.log" 2>&1 < /dev/null &
echo "$!" > "$HOME/.kda-sim/background_activity.pid"
sleep 0.2
pid="$(cat "$HOME/.kda-sim/background_activity.pid")"
if kill -0 "$pid" 2>/dev/null; then
  echo "KDA_BG_STARTED pid=$pid"
else
  echo "KDA_BG_FAILED"
  tail -20 "$HOME/.kda-sim/background_activity.log" 2>/dev/null || true
  exit 1
fi
"""
    return _bash_command(script)


def build_stop_command() -> str:
    script = """
set +e
if [ -f "$HOME/.kda-sim/background_activity.pid" ]; then
  pid="$(cat "$HOME/.kda-sim/background_activity.pid" 2>/dev/null || true)"
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    sleep 0.2
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$HOME/.kda-sim/background_activity.pid"
  echo "KDA_BG_STOPPED pid=$pid"
else
  echo "KDA_BG_NOT_RUNNING"
fi
"""
    return _bash_command(script)


def _bash_command(script: str) -> str:
    return "bash -lc " + shlex.quote(script)
