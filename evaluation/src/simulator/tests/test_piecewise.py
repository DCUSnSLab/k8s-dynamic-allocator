"""piecewise 부하(계단·산·파도) 생성 검증.

실행 (evaluation/src/simulator 에서):
    python -m unittest tests/test_piecewise.py
"""

import math
import random
import tempfile
import unittest
from pathlib import Path

import yaml

from kda_simulator.config import build_piecewise_plan, load_config, validate_config
from kda_simulator.scheduler import iter_schedule

N = 40
S = 68.0
LAMBDA_PER_RHO = N * 60.0 / S  # rho 1.0 = 35.29/분


def config_with(workload, seed=20261011):
    base = {
        "experiment": {"name": "T", "system": "warm_buffer", "buffer_reserve": 2, "buffer_capacity": 40},
        "ssh": {"host": "127.0.0.1", "port": 22},
        "users": {"prefix": "test", "count": 100, "max_concurrent_requests": 3},
        "workload": {
            "profile": "piecewise",
            "lambda_scope": "system",
            "capacity_slots": N,
            "slot_seconds": S,
            "random_seed": seed,
            **workload,
        },
        "commands": {"items": [{"name": "cpu", "weight": 1, "command": "true"}]},
        "output": {"dir": "evaluation/data"},
    }
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "c.yaml"
        path.write_text(yaml.safe_dump(base), encoding="utf-8")
        config = load_config(path)
    validate_config(config)
    return config


def arrivals(config):
    return list(iter_schedule(config, random.Random(config.workload.random_seed)))


STEP = [{"minutes": 7, "rho": r} for r in (0.2, 0.4, 0.6, 0.8, 0.9, 1.1)] + [{"minutes": 8, "rho": 0.2}]
MOUNTAIN = [
    {"minutes": 5, "rho": 0.3},
    {"minutes": 20, "rho_start": 0.3, "rho_end": 1.2},
    {"minutes": 5, "rho": 1.2},
    {"minutes": 15, "rho_start": 1.2, "rho_end": 0.3},
    {"minutes": 5, "rho": 0.3},
]
WAVE = [{"repeat": 5, "segments": [{"minutes": 6, "rho": 0.3}, {"minutes": 4, "rho": 1.2}]}]


class PlanTests(unittest.TestCase):
    def test_lambda_follows_rho_definition(self):
        plan = build_piecewise_plan([{"minutes": 1, "rho": 1.0}], N, S)
        self.assertAlmostEqual(plan[0]["lambda_per_minute"], 35.294, places=3)

    def test_linear_segment_splits_into_minute_steps_at_midpoints(self):
        plan = build_piecewise_plan([{"minutes": 20, "rho_start": 0.3, "rho_end": 1.2}], N, S)
        self.assertEqual(len(plan), 20)
        # 첫 계단 중앙(0.5분) = 0.3 + 0.9 * 0.5/20, 계단마다 +0.045.
        self.assertAlmostEqual(plan[0]["rho"], 0.3225, places=6)
        for a, b in zip(plan, plan[1:]):
            self.assertAlmostEqual(b["rho"] - a["rho"], 0.045, places=6)
            self.assertAlmostEqual(a["end_min"], b["start_min"])
        self.assertAlmostEqual(plan[-1]["end_min"], 20.0)

    def test_partial_last_minute_uses_its_own_midpoint(self):
        plan = build_piecewise_plan([{"minutes": 2.5, "rho_start": 0.0, "rho_end": 1.0}], N, S)
        self.assertEqual([p["end_min"] for p in plan], [1.0, 2.0, 2.5])
        self.assertAlmostEqual(plan[-1]["rho"], 2.25 / 2.5)

    def test_repeat_expands_in_order(self):
        plan = build_piecewise_plan(WAVE, N, S)
        self.assertEqual(len(plan), 10)
        self.assertEqual([p["rho"] for p in plan[:4]], [0.3, 1.2, 0.3, 1.2])
        self.assertAlmostEqual(plan[-1]["end_min"], 50.0)

    def test_three_scenarios_are_fifty_minutes(self):
        for name, segments in (("step", STEP), ("mountain", MOUNTAIN), ("wave", WAVE)):
            with self.subTest(name):
                self.assertAlmostEqual(build_piecewise_plan(segments, N, S)[-1]["end_min"], 50.0)

    def test_invalid_segments_are_rejected(self):
        for bad in (
            [],
            [{"minutes": 0, "rho": 1}],
            [{"minutes": 5, "rho": -0.1}],
            [{"minutes": 5}],
            [{"minutes": 5, "rho": 1, "lambda": 3}],
            [{"repeat": 0, "segments": [{"minutes": 1, "rho": 1}]}],
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                build_piecewise_plan(bad, N, S)


class ScheduleTests(unittest.TestCase):
    def test_same_seed_gives_identical_arrivals(self):
        """모든 비교군이 같은 도착을 받는 근거. seed 만 같으면 된다."""
        a = arrivals(config_with({"segments": MOUNTAIN}))
        b = arrivals(config_with({"segments": MOUNTAIN}))
        self.assertEqual(
            [(x.sequence, x.planned_offset_seconds, x.username) for x in a],
            [(x.sequence, x.planned_offset_seconds, x.username) for x in b],
        )
        c = arrivals(config_with({"segments": MOUNTAIN}, seed=1))
        self.assertNotEqual([x.planned_offset_seconds for x in a], [x.planned_offset_seconds for x in c])

    def test_counts_per_segment_match_lambda_times_duration(self):
        """구간별 생성 건수가 lambda x 시간과 맞는다(포아송 3 시그마 안)."""
        config = config_with({"segments": STEP})
        got = arrivals(config)
        for piece in config.workload.piecewise_plan:
            start, end = piece["start_min"] * 60, piece["end_min"] * 60
            n = sum(1 for x in got if start <= x.planned_offset_seconds < end)
            expected = piece["lambda_per_minute"] * (piece["end_min"] - piece["start_min"])
            with self.subTest(piece=piece):
                self.assertLess(abs(n - expected), 3 * math.sqrt(expected) + 1)

    def test_total_over_many_seeds_is_unbiased(self):
        config = config_with({"segments": WAVE})
        expected = sum(
            p["lambda_per_minute"] * (p["end_min"] - p["start_min"])
            for p in config.workload.piecewise_plan
        )
        totals = [
            len(list(iter_schedule(config, random.Random(seed)))) for seed in range(40)
        ]
        mean = sum(totals) / len(totals)
        self.assertLess(abs(mean - expected), 3 * math.sqrt(expected / len(totals)) + 1)

    def test_arrivals_stay_inside_their_run(self):
        config = config_with({"segments": MOUNTAIN})
        got = arrivals(config)
        self.assertTrue(all(0 <= x.planned_offset_seconds < 50 * 60 for x in got))
        self.assertEqual([x.sequence for x in got], list(range(1, len(got) + 1)))

    def test_duration_must_match_segments(self):
        self.assertEqual(config_with({"segments": WAVE}).workload.duration_minutes, 50.0)
        config_with({"segments": WAVE, "duration_minutes": 50})
        with self.assertRaises(ValueError):
            config_with({"segments": WAVE, "duration_minutes": 40})

    def test_piecewise_needs_slot_definition(self):
        with self.assertRaises(ValueError):
            config_with({"segments": WAVE, "slot_seconds": None})

    def test_segments_are_rejected_outside_piecewise(self):
        with self.assertRaises(ValueError):
            config_with({"profile": 2.0, "segments": WAVE, "capacity_slots": None, "slot_seconds": None})


if __name__ == "__main__":
    unittest.main()
