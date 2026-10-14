"""사용자당 동시 요청 상한(UserConcurrencyLimiter) 검증.

실행 (evaluation/src/simulator 에서):
    python -m unittest tests/test_user_limit.py
"""

import random
import unittest

from kda_simulator.scheduler import UserConcurrencyLimiter

USERS = [f"test{i}" for i in range(1, 6)]


class UserLimitTest(unittest.TestCase):
    def test_no_limit_keeps_planned_user(self):
        limiter = UserConcurrencyLimiter(USERS, None, random.Random(1))
        for _ in range(10):
            user, substituted, overflow = limiter.pick("test1")
            limiter.started(user)
            self.assertEqual((user, substituted, overflow), ("test1", False, False))
        self.assertEqual(limiter.inflight("test1"), 10)

    def test_planned_user_under_limit_is_kept(self):
        limiter = UserConcurrencyLimiter(USERS, 2, random.Random(1))
        limiter.started("test1")
        self.assertEqual(limiter.pick("test1"), ("test1", False, False))

    def test_full_user_is_replaced_by_one_under_limit(self):
        limiter = UserConcurrencyLimiter(USERS, 2, random.Random(1))
        for user in ("test1", "test1", "test2", "test2"):
            limiter.started(user)
        for _ in range(20):
            user, substituted, overflow = limiter.pick("test1")
            self.assertTrue(substituted)
            self.assertFalse(overflow)
            self.assertIn(user, {"test3", "test4", "test5"})

    def test_finishing_frees_a_slot(self):
        limiter = UserConcurrencyLimiter(USERS, 2, random.Random(1))
        limiter.started("test1")
        limiter.started("test1")
        self.assertTrue(limiter.pick("test1")[1])
        limiter.finished("test1")
        self.assertEqual(limiter.pick("test1"), ("test1", False, False))

    def test_all_full_goes_to_least_loaded_and_reports_overflow(self):
        limiter = UserConcurrencyLimiter(USERS, 1, random.Random(1))
        for user in USERS:
            limiter.started(user)
        limiter.started("test1")  # test1 은 2 개, 나머지는 1 개
        user, substituted, overflow = limiter.pick("test1")
        self.assertTrue(substituted)
        self.assertTrue(overflow)
        self.assertNotEqual(user, "test1")

    def test_limit_never_exceeded_while_free_users_exist(self):
        # 무작위로 보내고 끝내기를 반복해도, 상한 아래 사용자가 있는 한 아무도 상한을 넘지 않는다.
        rng = random.Random(7)
        users = [f"test{i}" for i in range(1, 101)]
        limiter = UserConcurrencyLimiter(users, 2, random.Random(3))
        running = []
        for _ in range(5000):
            if running and rng.random() < 0.5:
                limiter.finished(running.pop(rng.randrange(len(running))))
                continue
            user, _, overflow = limiter.pick(rng.choice(users))
            self.assertFalse(overflow)
            limiter.started(user)
            running.append(user)
            self.assertLessEqual(limiter.inflight(user), 2)

    def test_same_seed_same_choices(self):
        def picks():
            limiter = UserConcurrencyLimiter(USERS, 1, random.Random(5))
            limiter.started("test1")
            return [limiter.pick("test1")[0] for _ in range(10)]

        self.assertEqual(picks(), picks())


if __name__ == "__main__":
    unittest.main()
