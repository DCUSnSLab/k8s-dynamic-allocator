"""Compare several runs at once: repeats of one arm, and arms against each other.

experiment_metrics.py reports one run. A comparison needs more than that report
repeated by hand, for two reasons.

A single run does not say how much the same configuration moves between runs, and
without that number a difference between arms cannot be called a difference. So
repeats of one arm are grouped and their spread is reported next to their value.

And the cluster is shared: pod readiness has been seen to differ by nearly an
order of magnitude between times of day, so two arms measured hours apart are not
measured under the same conditions. Runs are therefore paired into blocks by the
order they were run, and the arm difference is taken inside a block where the
time of day is common to both.

Usage:
    compare_runs.py <run dir> [<run dir> ...]
    compare_runs.py evaluation/data/2026*VERIFY_*        (a glob works)
    compare_runs.py --json out.json <run dirs>

Each run directory needs analysis/metrics.json; it is built on demand from the
run's own files if it is missing.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean
from typing import Any

HERE = Path(__file__).resolve().parent
# Percentiles worth carrying into a comparison. p99 is left out of the spread
# table: at a few hundred requests per run it is a handful of samples and moves
# for reasons that have nothing to do with the arm.
LATENCY_KEYS = [
    ("start_delay_s", "시작 지연", ["p50", "p95"]),
    ("user_wait_s", "사용자 대기", ["p50", "p95"]),
    ("resume_s", "재개 지연", ["p50", "p95"]),
]


@dataclass
class Run:
    path: Path
    metrics: dict[str, Any]
    summary: dict[str, Any]
    arm: str
    label: str
    order: int = 0
    block: int = 0
    conditions: dict[str, Any] = field(default_factory=dict)

    @property
    def requests(self) -> dict[str, Any]:
        return self.metrics["overall"]["requests"]

    @property
    def pods(self) -> dict[str, Any]:
        return self.metrics["overall"].get("pods") or {}

    def stat(self, key: str, q: str) -> float | None:
        block = self.requests.get(key) or {}
        return block.get(q) if block.get("count") else None

    def occupancy(self, group: str) -> float | None:
        return (self.pods.get(group) or {}).get("cpu_cores_avg")


def ensure_metrics(path: Path) -> dict[str, Any] | None:
    """Return the run's metrics, computing them if the run has not been analysed."""
    metrics_file = path / "analysis" / "metrics.json"
    if not metrics_file.exists():
        subprocess.run(
            [sys.executable, str(HERE / "experiment_metrics.py"), str(path)],
            capture_output=True, text=True,
        )
    if not metrics_file.exists():
        return None
    return json.loads(metrics_file.read_text(encoding="utf-8"))


def load(path: Path) -> Run | None:
    summary_file = path / "simulator" / "summary.json"
    if not summary_file.exists():
        print(f"  건너뜀 {path.name}: summary.json 없음 (중단된 실행)")
        return None
    metrics = ensure_metrics(path)
    if metrics is None:
        print(f"  건너뜀 {path.name}: 지표를 만들 수 없음")
        return None
    summary = json.loads(summary_file.read_text(encoding="utf-8"))

    experiment = summary.get("experiment") or {}
    # The arm is what the config called the system; the run name carries the
    # repeat suffix and would split one arm into three.
    arm = experiment.get("system") or experiment.get("name") or path.name
    culling = summary.get("idle_culling") or {}
    workload = summary.get("workload") or {}
    conditions = {
        "mode": (summary.get("execution") or {}).get("mode"),
        "users": (summary.get("users") or {}).get("count"),
        "lambda": workload.get("scenario"),
        "R": experiment.get("buffer_reserve"),
        "N": experiment.get("buffer_capacity"),
        "T": culling.get("idle_minutes") if culling.get("enabled") else None,
        "trace": Path(workload["trace_file"]).name if workload.get("trace_file") else None,
    }
    return Run(path=path, metrics=metrics, summary=summary, arm=arm,
               label=experiment.get("name") or path.name, conditions=conditions)


def spread(values: list[float]) -> str:
    """Range as a share of the mean - the number that says whether a gap is real."""
    present = [v for v in values if v is not None]
    if len(present) < 2:
        return "-"
    centre = mean(present)
    if not centre:
        return "0%"
    return f"{(max(present) - min(present)) / centre * 100:.0f}%"


def fmt(value: float | None, unit: str = "") -> str:
    return "-" if value is None else f"{value:.2f}{unit}"


def print_runs(runs: list[Run]) -> None:
    print("=== 실행 목록 ===")
    print(f"  {'#':>2} {'비교군':<16} {'이름':<26} {'요청':>6} {'실패':>5} {'측정없음':>8} {'블록':>4}")
    for r in runs:
        status = r.requests["status"]
        failed = sum(n for s, n in status.items() if s != "success")
        nomeas = r.requests["count"] - (r.requests.get("command_s") or {}).get("count", 0)
        print(f"  {r.order:>2} {r.arm:<16} {r.label:<26} {r.requests['count']:>6} "
              f"{failed:>5} {nomeas:>8} {r.block:>4}")


def print_conditions(runs: list[Run]) -> None:
    print()
    print("=== 조건 ===")
    keys = ["mode", "users", "lambda", "R", "N", "T", "trace"]
    print("  " + " ".join(f"{k:>10}" for k in ["비교군"] + keys))
    seen = set()
    for r in runs:
        row = tuple(r.conditions.get(k) for k in keys)
        if (r.arm, row) in seen:
            continue
        seen.add((r.arm, row))
        print("  " + f"{r.arm:>10} " + " ".join(f"{str(v):>10}" for v in row))
    varying = [k for k in keys
               if len({str(r.conditions.get(k)) for r in runs}) > 1 and k != "trace"]
    if varying:
        print(f"  비교군 간 다른 조건: {', '.join(varying)}")


def print_per_arm(runs: list[Run]) -> None:
    print()
    print("=== 비교군별 값과 반복 간 산포 ===")
    arms: dict[str, list[Run]] = {}
    for r in runs:
        arms.setdefault(r.arm, []).append(r)

    for arm, group in arms.items():
        print(f"  {arm}  ({len(group)}회)")
        for key, title, quantiles in LATENCY_KEYS:
            for q in quantiles:
                values = [r.stat(key, q) for r in group]
                if not any(v is not None for v in values):
                    continue
                shown = "  ".join(fmt(v) for v in values)
                print(f"    {title + ' ' + q:<18} {shown:<28} 산포 {spread(values)}")
        for group_name, title in (("user", "user pod 점유"), ("compute", "compute pod 점유")):
            values = [r.occupancy(group_name) for r in group]
            if not any(values):
                continue
            shown = "  ".join(fmt(v, "코어") for v in values)
            print(f"    {title:<18} {shown:<28} 산포 {spread(values)}")
        reruns = [r.requests.get("rerun", 0) for r in group]
        if any(reruns):
            print(f"    {'재실행':<18} {'  '.join(str(v) for v in reruns)}")


def print_blocks(runs: list[Run]) -> None:
    blocks: dict[int, list[Run]] = {}
    for r in runs:
        blocks.setdefault(r.block, []).append(r)
    usable = {b: rs for b, rs in blocks.items() if len({r.arm for r in rs}) > 1}
    if not usable:
        return

    print()
    print("=== 블록 안에서 본 비교군 차이 (시간대 상쇄) ===")
    for block in sorted(usable):
        group = usable[block]
        base = group[0]
        print(f"  블록{block}  기준 {base.arm}")
        for other in group[1:]:
            for key, title, _ in LATENCY_KEYS:
                a, b = base.stat(key, "p50"), other.stat(key, "p50")
                if a is None or b is None:
                    continue
                print(f"    {title + ' p50':<16} {fmt(a)} -> {fmt(b)}   {b - a:+.2f}초")
            a, b = base.occupancy("user"), other.occupancy("user")
            if a and b:
                print(f"    {'user pod 점유':<16} {fmt(a, '코어')} -> {fmt(b, '코어')}"
                      f"   {(b / a - 1) * 100:+.0f}%")


def print_checks(runs: list[Run]) -> None:
    print()
    print("=== 정합성 ===")
    counts = {r.requests["count"] for r in runs}
    print(f"  요청 수 {'모두 동일' if len(counts) == 1 else '다름: ' + str(sorted(counts))}"
          f"  ({sorted(counts)[0] if len(counts) == 1 else ''})")

    traces = {r.conditions.get("trace") for r in runs}
    print(f"  트레이스 {'동일' if len(traces) == 1 else '다름: ' + str(traces)}"
          f"  ({next(iter(traces)) if len(traces) == 1 else ''})")

    for r in runs:
        problems = []
        status = r.requests["status"]
        failed = sum(n for s, n in status.items() if s != "success")
        if failed:
            problems.append(f"비성공 {failed}건 {status}")
        if r.requests.get("server_failures"):
            problems.append(f"서버 실패 {r.requests['server_failures']}건")
        if r.requests.get("output_lost"):
            problems.append(f"출력 손실 {r.requests['output_lost']}건")
        if problems:
            print(f"  {r.label}: {', '.join(problems)}")
    if all(sum(n for s, n in r.requests["status"].items() if s != "success") == 0
           for r in runs):
        print("  전 실행 비성공 0건")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="여러 실행 디렉터리를 비교군별로 묶어 비교한다.")
    parser.add_argument("runs", nargs="+", help="실행 디렉터리들")
    parser.add_argument("--json", help="결과를 JSON 으로도 저장할 경로")
    parser.add_argument("--block-size", type=int, default=0,
                        help="블록당 실행 수. 0이면 비교군 수로 자동 (교대 실행 가정)")
    args = parser.parse_args()

    print("=== 읽는 중 ===")
    runs = [r for r in (load(Path(p)) for p in sorted(args.runs)) if r is not None]
    if not runs:
        print("  분석할 실행이 없습니다")
        return 1

    runs.sort(key=lambda r: r.metrics["window"]["start"])
    arms = len({r.arm for r in runs})
    size = args.block_size or max(arms, 1)
    for index, run in enumerate(runs):
        run.order = index + 1
        run.block = index // size + 1
    print(f"  실행 {len(runs)}개, 비교군 {arms}개, 블록 크기 {size}")

    print()
    print_runs(runs)
    print_conditions(runs)
    print_per_arm(runs)
    print_blocks(runs)
    print_checks(runs)

    if args.json:
        payload = {
            "runs": [
                {"name": r.label, "arm": r.arm, "block": r.block,
                 "conditions": r.conditions,
                 "requests": r.requests, "pods": r.pods}
                for r in runs
            ]
        }
        Path(args.json).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print()
        print(f"  JSON 저장: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
