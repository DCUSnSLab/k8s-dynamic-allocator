from __future__ import annotations

import asyncio
import base64
import shlex
from typing import Any

from .config import SimulatorConfig


BACKGROUND_COMMAND_TIMEOUT_SECONDS = 60.0

BACKGROUND_SCRIPT = r"""#!/usr/bin/env python3
import argparse
import math
import signal
import sys
import time


running = True


# 리눅스 CFS 가 CPU 할당량을 나눠 주는 주기. 커널 기본값이고 우리가 고른 수가 아니다.
# 부하를 이 주기에 맞춰 나누면 한 조각이 요구하는 CPU 가 limit 아래로 내려간다.
CFS_PERIOD_SECONDS = 0.1

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
        # 벽시계가 아니라 CPU 시간으로 센다. user pod 의 CPU limit 은 200m 이고 이
        # 루프는 단일 스레드라 1코어를 요구하므로, 도는 내내 throttle 된다. 벽시계로
        # 재면 0.2초가 지나는 동안 CPU 는 40ms 밖에 못 써서 의도한 부하의 1/5 만
        # 걸렸다 (의도 20m, 실측 평균 4~8m). CPU 시간으로 세면 throttle 되는 만큼
        # 벽시계가 늘어날 뿐 걸리는 부하는 설정한 값 그대로다.
        # CPU 시간을 한 번에 몰아 쓰지 않고 커널 스케줄 주기(100ms)에 맞춰 나눈다.
        # 단일 스레드 루프는 도는 동안 1코어를 요구하는데 user pod 의 limit 은 200m
        # 이라, 몰아 쓰면 그 구간 내내 throttle 된다. 평균 부하는 같은데 (0.2초 /
        # 10초 = 20m) 알림만 뜨고 명령 중계까지 같이 밀린다. 조각으로 나누면 한
        # 조각이 요구하는 양이 limit 아래로 내려가 throttle 없이 같은 양이 걸린다.
        slices = max(1, int(args.cpu_period_seconds / CFS_PERIOD_SECONDS))
        slice_cpu = args.cpu_busy_seconds / slices
        slice_wall = args.cpu_period_seconds / slices
        i = 0
        for slice_index in range(slices):
            if not running:
                break
            slice_started_cpu = time.process_time()
            slice_started_wall = time.monotonic()
            while running and time.process_time() - slice_started_cpu < slice_cpu:
                checksum += math.sqrt(i % 1000000)
                i += 1
            rest = slice_wall - (time.monotonic() - slice_started_wall)
            if rest > 0 and slice_index + 1 < slices:
                time.sleep(rest)

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


async def start_background_activity(config: SimulatorConfig, sessions: Any) -> None:
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
    remaining = list(config.user_names())
    failed: list[tuple[str, str]] = []
    for attempt in range(1, config.setup.retry_attempts + 1):
        results = await _run_for_users(config, sessions, command, usernames=remaining)
        failed = [
            (username, _result_error_summary(result))
            for username, result in results
            if result.error or result.timed_out or result.exit_status not in (0, None)
        ]
        if not failed:
            print("Background activity started")
            return

        remaining = [username for username, _error in failed]
        preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
        if attempt < config.setup.retry_attempts:
            print(
                "Background activity retry "
                f"{attempt}/{config.setup.retry_attempts}: "
                f"{len(failed)} users not ready yet ({preview})"
            )
            await asyncio.sleep(config.setup.retry_delay_seconds)

    preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
    raise RuntimeError(f"Background activity failed for {len(failed)} users: {preview}")


async def stop_background_activity(config: SimulatorConfig, sessions: Any) -> None:
    if not config.background_activity.enabled:
        return

    print("Stopping background activity...")
    # A culled user has no pod and no process. Sending the stop command would
    # reconnect, which makes swlabssh build the pod again just to stop something
    # that is not running - and leaves a burst of pod creations in the records
    # right after the experiment ended.
    remaining = [
        username
        for username in config.user_names()
        if not getattr(sessions.get(username), "culled", False)
    ]
    culled = config.users.count - len(remaining)
    if culled:
        print(f"  skipping {culled} culled users: their pod and process are gone")
    if not remaining:
        print("Background activity stopped")
        return
    results = await _run_for_users(
        config,
        sessions,
        build_stop_command(),
        usernames=remaining,
        return_exceptions=True,
    )
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
            failed.append((username, _result_error_summary(result)))
    if failed:
        preview = ", ".join(f"{username}={error}" for username, error in failed[:5])
        print(f"Background activity stop warnings: {preview}")
    print("Background activity stopped")


async def _run_for_users(
    config: SimulatorConfig,
    sessions: Any,
    command: str,
    *,
    usernames: list[str] | None = None,
    return_exceptions: bool = False,
) -> list[tuple[str, Any]]:
    sem = asyncio.Semaphore(config.setup.max_inflight)

    async def _run(username: str) -> tuple[str, Any]:
        async with sem:
            try:
                result = await sessions.get(username).run_remote(
                    command,
                    timeout=BACKGROUND_COMMAND_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                if return_exceptions:
                    return username, exc
                raise
            return username, result

    return await asyncio.gather(
        *(_run(username) for username in (usernames or config.user_names())),
        return_exceptions=False,
    )


def build_start_command(config: SimulatorConfig) -> str:
    payload = base64.b64encode(BACKGROUND_SCRIPT.encode("utf-8")).decode("ascii")
    activity = config.background_activity
    script = f"""
set -eu
mkdir -p "$HOME/.kda-sim"
python3 -c "import base64, pathlib; pathlib.Path.home().joinpath('.kda-sim').mkdir(exist_ok=True); pathlib.Path.home().joinpath('.kda-sim/background_activity.py').write_bytes(base64.b64decode('{payload}'))"
if [ -f "$HOME/.kda-sim/background_activity.pid" ]; then
  old_pid="$(cat "$HOME/.kda-sim/background_activity.pid" 2>/dev/null || true)"
  if [ -n "$old_pid" ]; then
    kill "$old_pid" 2>/dev/null || true
  fi
fi
launcher="env -u KUBESSH_SESSION_ID"
if command -v setsid >/dev/null 2>&1; then
  launcher="setsid env -u KUBESSH_SESSION_ID"
fi
$launcher nohup python3 "$HOME/.kda-sim/background_activity.py" \
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


def _result_error_summary(result: Any) -> str:
    if isinstance(result, BaseException):
        return str(result)
    parts: list[str] = []
    error = getattr(result, "error", None)
    if error:
        parts.append(str(error))
    if getattr(result, "timed_out", False):
        parts.append("timed_out")
    exit_status = getattr(result, "exit_status", None)
    if exit_status not in (0, None):
        parts.append(f"exit={exit_status}")
    stdout = _tail(getattr(result, "stdout", ""))
    stderr = _tail(getattr(result, "stderr", ""))
    if stdout:
        parts.append(f"stdout={stdout!r}")
    if stderr:
        parts.append(f"stderr={stderr!r}")
    return "; ".join(parts) or "unknown"


def _tail(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if len(text) <= limit:
        return text
    return text[-limit:]
