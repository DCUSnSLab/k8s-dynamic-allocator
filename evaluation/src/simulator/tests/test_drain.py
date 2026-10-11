"""정리 마감(workload.drain_minutes) 검증.

실행 (evaluation/src/simulator 에서):
    python -m unittest tests/test_drain.py
"""

import asyncio
import time
import unittest

from kda_simulator.main import wait_until_drain_deadline
from tests.test_piecewise import config_with


class DrainConfigTests(unittest.TestCase):
    def test_default_is_the_run_length(self):
        config = config_with({"segments": [{"minutes": 25, "rho": 0.5}]})
        self.assertEqual(config.workload.drain_minutes, 25)

    def test_explicit_value_is_kept(self):
        config = config_with({"segments": [{"minutes": 25, "rho": 0.5}], "drain_minutes": 10})
        self.assertEqual(config.workload.drain_minutes, 10)

    def test_non_positive_is_rejected(self):
        with self.assertRaises(ValueError):
            config_with({"segments": [{"minutes": 25, "rho": 0.5}], "drain_minutes": 0})


class DrainWaitTests(unittest.TestCase):
    def run_async(self, coro):
        return asyncio.run(coro)

    def test_all_finished_before_deadline(self):
        async def go():
            tasks = [asyncio.create_task(asyncio.sleep(0.01)) for _ in range(3)]
            return await wait_until_drain_deadline(tasks, time.monotonic() + 5)
        self.assertEqual(self.run_async(go()), [])

    def test_slow_task_is_returned_not_cancelled(self):
        async def go():
            fast = asyncio.create_task(asyncio.sleep(0.01))
            slow = asyncio.create_task(asyncio.sleep(10))
            left = await wait_until_drain_deadline([fast, slow], time.monotonic() + 0.1)
            state = (left == [slow], slow.cancelled(), slow.done())
            slow.cancel()
            await asyncio.gather(slow, return_exceptions=True)
            return state
        self.assertEqual(self.run_async(go()), (True, False, False))

    def test_past_deadline_returns_immediately(self):
        async def go():
            slow = asyncio.create_task(asyncio.sleep(10))
            started = time.monotonic()
            left = await wait_until_drain_deadline([slow], time.monotonic() - 1)
            elapsed = time.monotonic() - started
            slow.cancel()
            await asyncio.gather(slow, return_exceptions=True)
            return left == [slow], elapsed < 1.0
        self.assertEqual(self.run_async(go()), (True, True))

    def test_exception_is_raised_without_waiting_for_others(self):
        async def boom():
            await asyncio.sleep(0.01)
            raise RuntimeError("boom")

        async def go():
            slow = asyncio.create_task(asyncio.sleep(10))
            bad = asyncio.create_task(boom())
            started = time.monotonic()
            try:
                await wait_until_drain_deadline([slow, bad], time.monotonic() + 30)
            except RuntimeError:
                elapsed = time.monotonic() - started
            slow.cancel()
            await asyncio.gather(slow, return_exceptions=True)
            return elapsed < 5
        self.assertTrue(self.run_async(go()))

    def test_no_deadline_waits_for_all(self):
        async def go():
            tasks = [asyncio.create_task(asyncio.sleep(0.05)) for _ in range(2)]
            left = await wait_until_drain_deadline(tasks, None)
            return left, all(t.done() for t in tasks)
        self.assertEqual(self.run_async(go()), ([], True))


if __name__ == "__main__":
    unittest.main()
