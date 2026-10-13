"""요청마다 다른 작업 길이(잘린 로그정규)와 명령 틀 채우기 검증.

실행 (evaluation/src/simulator 에서):
    python -m unittest tests/test_job_length.py
"""

import math
import random
import statistics
import tempfile
import unittest
from pathlib import Path

import yaml

from kda_simulator.config import JobLengthConfig, load_config
from kda_simulator.trace import generate_trace_entries, read_trace_csv, write_trace_csv
from kda_simulator.workload import WeightedCommandSelector, draw_work_seconds

JOB = JobLengthConfig(
    median_seconds=245.2, sigma=0.935, cap_seconds=2158.6,
    time_limit_factor=10.0, time_limit_slack_seconds=60.0,
)
TEMPLATE = "python3 -c \"import signal; signal.alarm({limit}); exec('for i in range({units}):\\n    pass')\""


def write_config(tmp, job_length=True, command=TEMPLATE, units=1000.0):
    data = {
        "experiment": {"name": "T", "system": "warm_buffer", "buffer_reserve": 2, "buffer_capacity": 40},
        "ssh": {"host": "127.0.0.1", "port": 22},
        "users": {"prefix": "test", "count": 10},
        "workload": {
            "profile": "piecewise", "lambda_scope": "system", "capacity_slots": 40,
            "slot_seconds": 352.0, "random_seed": 7,
            "segments": [{"minutes": 60, "rho": 1.0}],
        },
        "commands": {"items": [{"name": "cpu", "weight": 1, "command": command, "units_per_second": units}]},
        "output": {"dir": "evaluation/data"},
    }
    if job_length:
        data["job_length"] = dict(JOB.__dict__)
    path = Path(tmp) / "c.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


class JobLengthTest(unittest.TestCase):
    def test_draws_follow_truncated_lognormal(self):
        rng = random.Random(1)
        values = [draw_work_seconds(JOB, rng) for _ in range(20000)]
        self.assertTrue(all(0 < v <= JOB.cap_seconds for v in values))
        # 잘린 분포라 중앙값은 245 보다 조금 작다(위 1% 를 버림).
        self.assertAlmostEqual(statistics.median(values), JOB.median_seconds, delta=15)
        logs = [math.log(v) for v in values]
        self.assertAlmostEqual(statistics.pstdev(logs), JOB.sigma, delta=0.05)

    def test_template_is_filled_per_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = load_config(write_config(tmp))
        selector = WeightedCommandSelector(config.commands, random.Random(3), "kda", config.job_length)
        chosen = selector.choose("r1")
        self.assertIsNotNone(chosen.work_seconds)
        units = max(1, round(chosen.work_seconds * 1000.0))
        limit = math.ceil(chosen.work_seconds * 10.0 + 60.0)
        self.assertIn(f"range({units})", chosen.command)
        self.assertIn(f"signal.alarm({limit})", chosen.command)
        self.assertNotIn("{units}", chosen.remote_command)

    def test_same_seed_same_trace_and_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = load_config(write_config(tmp))

            def entries():
                rng = random.Random(config.workload.random_seed)
                selector = WeightedCommandSelector(config.commands, rng, "kda", config.job_length)
                return generate_trace_entries(config, rng, selector)

            first, second = entries(), entries()
            self.assertEqual(first, second)
            path = Path(tmp) / "t.csv"
            write_trace_csv(path, first)
            again = read_trace_csv(path)
        self.assertEqual([e.work_seconds for e in again], [round(e.work_seconds, 3) for e in first])
        self.assertEqual([e.command for e in again], [e.command for e in first])

    def test_template_without_placeholders_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                load_config(write_config(tmp, command="true"))

    def test_old_config_without_job_length_still_loads(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = load_config(write_config(tmp, job_length=False, command="true", units=None))
        self.assertIsNone(config.job_length)
        chosen = WeightedCommandSelector(config.commands, random.Random(1)).choose("r1")
        self.assertIsNone(chosen.work_seconds)
        self.assertEqual(chosen.command, "true")


if __name__ == "__main__":
    unittest.main()
