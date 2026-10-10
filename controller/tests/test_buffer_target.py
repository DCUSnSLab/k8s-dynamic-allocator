"""Regression checks for the dynamic available-Pod target.

Load the reconciler directly so this pure calculation can be checked without
the controller's HTTP and Redis runtime dependencies.
"""

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import threading
import time
import types
import unittest
from unittest.mock import patch


def load_reconciler():
    source = (
        Path(__file__).resolve().parents[1]
        / "services"
        / "compute"
        / "buffer_capacity_reconciler.py"
    )
    package_names = (
        "controller",
        "controller.services",
        "controller.services.compute",
    )
    packages = {}
    for name in package_names:
        package = types.ModuleType(name)
        package.__path__ = []
        packages[name] = package

    config = types.ModuleType("config")
    config.settings = types.SimpleNamespace(
        WAIT_QUEUE_BATCH_LIMIT=10,
        COMPUTE_NOT_READY_GRACE_SECONDS=30,
    )
    queue = types.ModuleType("controller.services.queue")
    queue.QueueUnavailableError = type("QueueUnavailableError", (Exception,), {})
    modules = {**packages, "config": config, queue.__name__: queue}

    name = "controller.services.compute.buffer_capacity_reconciler"
    spec = importlib.util.spec_from_file_location(name, source)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


reconciler_module = load_reconciler()
BufferCapacityReconciler = reconciler_module.BufferCapacityReconciler


class BufferTargetTests(unittest.TestCase):
    def test_saturated_queue_uses_all_unassigned_capacity(self):
        target = BufferCapacityReconciler.desired_replicas_detail(2, 40, 20, 183)
        self.assertEqual(target, {"desired": 20, "bound_by": "room"})

    def test_queue_and_reserve_are_respected_below_capacity(self):
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 20, 5), 5)
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 20, 0), 2)

    def test_no_room_never_creates_available_pods(self):
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 40, 183), 0)
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 41, 183), 0)

    def test_fixed_policy_still_targets_reserve_clamped_by_room(self):
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 20), 2)
        self.assertEqual(BufferCapacityReconciler.desired_replicas(2, 40, 39), 1)

    def test_high_queue_cannot_be_capped_below_room(self):
        for assigned in range(41):
            room = 40 - assigned
            with self.subTest(assigned=assigned):
                self.assertEqual(
                    BufferCapacityReconciler.desired_replicas(2, 40, assigned, 1000),
                    room,
                )

    def test_demand_scale_down_waits_for_scheduled_pods(self):
        decide = BufferCapacityReconciler._defer_demand_scale_down
        # 들인 파드가 아직 유예 안에 있으면 수요 축소를 미룬다.
        self.assertTrue(decide(10, 20, 2, 10, 3, 5.0, 30.0))
        self.assertFalse(decide(10, 20, 2, 10, 0, 5.0, 30.0))
        # N correction remains mandatory even while ungated Pods start.
        self.assertFalse(decide(10, 9, 2, 10, 3, 5.0, 30.0))

    def test_stuck_pod_cannot_defer_scale_down_forever(self):
        """끝내 Ready 가 안 되는 파드가 축소를 영구히 막지 못한다.

        이것이 없으면 교착이 셋 겹친다. 축소가 막히면 replicas 가 고정되고,
        그러면 게이트 해제 예산(min(replicas, N-assigned, desired))도 고정되어
        남은 게이트 파드가 영구히 안 풀리고, maxUnavailable 이 회복되지 않아
        롤링 업데이트까지 멈춘다.

        cleanup 이 치워 줄 것이라고 기대할 수 없다 - 스케줄되지 못한 파드는
        status.startTime 이 nil 이라 _pod_not_ready_since 가 None 을 돌려주고,
        그 함수는 "한 번도 Ready 가 된 적 없는 파드" 를 의도적으로 제외한다.
        """
        decide = BufferCapacityReconciler._defer_demand_scale_down
        # 유예 경계 직전/직후.
        self.assertTrue(decide(10, 20, 2, 10, 1, 29.9, 30.0))
        self.assertFalse(decide(10, 20, 2, 10, 1, 30.1, 30.0))
        # 나이를 모르면 미루지 않는다. 상한 없는 보류가 바로 위의 교착이고,
        # 미루지 않아서 생기는 손해는 되돌릴 수 있는 재생성뿐이다.
        self.assertFalse(decide(10, 20, 2, 10, 1, None, 30.0))

    def test_starting_grace_takes_the_larger_of_observed_and_configured(self):
        """유예는 관측값과 회수 기준 중 큰 쪽이다. 고정 상수가 아니다."""
        reconciler = object.__new__(BufferCapacityReconciler)
        reconciler.create_time_seconds = lambda compute_type: 7.0
        with patch.object(reconciler_module.settings,
                          "COMPUTE_NOT_READY_GRACE_SECONDS", 30):
            self.assertEqual(reconciler._starting_grace_seconds("general"), 30.0)
        # 혼잡 구간에서 관측 p90 이 회수 기준을 넘으면 관측값을 쓴다 - 정상적으로
        # 늦게 뜨는 파드를 지워 10/9 의 취소를 되살리지 않기 위해서다.
        reconciler.create_time_seconds = lambda compute_type: 45.0
        with patch.object(reconciler_module.settings,
                          "COMPUTE_NOT_READY_GRACE_SECONDS", 30):
            self.assertEqual(reconciler._starting_grace_seconds("general"), 45.0)
        # 표본이 모자라면 0 이고 회수 기준만 남는다.
        reconciler.create_time_seconds = lambda compute_type: 0.0
        with patch.object(reconciler_module.settings,
                          "COMPUTE_NOT_READY_GRACE_SECONDS", 30):
            self.assertEqual(reconciler._starting_grace_seconds("general"), 30.0)

    def test_held_available_excludes_gated_pods(self):
        """목표와 견주는 값에서 게이트 파드를 빼야 한다.

        게이트 파드는 노드 자원을 0 쓰는데, ReplicaSet 은 selector 가 replicas
        보다 적으면 곧바로 하나를 만든다. 그 값을 목표와 비교하면 포화에서
        available = room + backfill > desired = room 이 거의 항상 참이 되어,
        줄일 것이 없는데 _scale_down 이 축소 게이트를 쥔 채 최대 10초를 기다린다.
        그 10초 동안 모든 복제본의 할당이 막힌다.
        """
        held = BufferCapacityReconciler._held_available
        self.assertEqual(held({"buffer_available": 21, "ungated_available": 20}), 20)
        # 게이트 이전 형태의 스냅샷은 buffer_available 로 물러선다.
        self.assertEqual(held({"buffer_available": 21}), 21)


class GateQueues:
    def __init__(self, queued=0):
        self.queued = queued
        self.renew_count = 0
        self.lose_on_renew = None
        self.scale_down_gated = False
        self.policy = {"R": 2, "N": 40, "deployment_name": "compute-general"}
        self.events = []
        self.supply_locked = False

    def normalize_compute_type(self, value):
        return value or "general"

    def renew_supply_lock(self, compute_type, token):
        assert self.supply_locked
        self.renew_count += 1
        self.events.append("renew")
        return self.renew_count != self.lose_on_renew

    def release_supply_lock(self, compute_type, token):
        self.events.append("supply_unlock")
        self.supply_locked = False
        return True

    def is_scale_down_gated(self, compute_type):
        return self.scale_down_gated

    def get_buffer_policy(self, compute_type):
        return self.policy

    def queued_count(self, compute_type):
        if isinstance(self.queued, Exception):
            raise self.queued
        return self.queued


class GateProvider:
    """게이트 파드·replicas 를 메모리에 둔 가짜. 노드 위 파드 수를 매번 검사한다."""

    def __init__(self, *, current, assigned, ungated, gated, total=None, unowned=0, n=40):
        self.current = current
        self.assigned = assigned
        self.ungated = ungated
        self.gated_pods = [f"gated-{i}" for i in range(gated)]
        self.versions = {name: "1" for name in self.gated_pods}
        self.total = total
        self.unowned = unowned
        self.n = n
        self.released = []
        self.raises = []
        self.raise_conflicts = 0
        self.lowers = []
        self.before_raise = None
        self.fail_release = {}
        self.after_release = None
        self.peak_on_node = assigned + ungated

    def list_buffer_snapshot(self, compute_type):
        available = self.ungated + len(self.gated_pods)
        pods = [
            {"name": name, "scheduling_gated": True, "terminating": False,
             "resource_version": self.versions[name]}
            for name in self.gated_pods
        ]
        return {
            "buffer_assigned": self.assigned,
            "buffer_available": available,
            "buffer_total": self.total if self.total is not None else self.assigned + available,
            "ungated_available": self.ungated,
            "unowned_available": self.unowned,
            "gated_available_candidates": [
                {"name": name, "resource_version": self.versions[name], "gate_index": 0}
                for name in self.gated_pods
            ],
            "pods": pods,
        }

    def read_deployment_replicas(self, name):
        return self.current

    def raise_deployment_replicas(self, name, expected, desired):
        if self.raise_conflicts:
            # 그사이 할당기가 낮췄다(CAS 실패).
            self.raise_conflicts -= 1
            self.current -= 1
            if self.gated_pods:
                self.gated_pods.pop()       # ReplicaSet 이 낮춘 만큼 지운다
            return False
        if self.before_raise:
            hook, self.before_raise = self.before_raise, None
            hook(self)
        if expected != self.current or desired <= expected:
            return False
        self.raises.append((expected, desired))
        added = desired - self.current
        self.current = desired
        start = len(self.versions)
        for i in range(added):
            name = f"backfill-{start + i}"
            self.gated_pods.append(name)
            self.versions[name] = "1"
        return True

    def lower_deployment_replicas(self, name, expected, desired):
        if expected != self.current or desired >= expected:
            return False
        self.lowers.append((expected, desired))
        # ReplicaSet 은 노드에 없는 게이트 파드부터 지운다.
        for _ in range(expected - desired):
            if self.gated_pods:
                self.gated_pods.pop()
        self.current = desired
        return True

    def release_pod_scheduling_gate(self, name, resource_version, gate_index):
        failure = self.fail_release.pop(name, None)
        if failure is not None:
            raise failure
        if name not in self.gated_pods or self.versions[name] != resource_version:
            return False
        self.released.append(name)
        self.gated_pods.remove(name)
        self.ungated += 1
        self.peak_on_node = max(self.peak_on_node, self.assigned + self.ungated)
        if self.after_release:
            self.after_release(self)
        return True


def gate_reconciler(provider, queues):
    reconciler = object.__new__(BufferCapacityReconciler)
    reconciler.provider = provider
    reconciler.queues = queues
    reconciler.dynamic_reserve = True
    reconciler.wait_timeout_seconds = 1.0
    reconciler._stop_event = threading.Event()
    reconciler._leadership_validator = lambda: True

    def acquire_supply(*args):
        assert not queues.supply_locked
        queues.supply_locked = True
        queues.events.append("supply_lock")
        return "supply-token"

    def no_alloc_lock(*args):
        raise AssertionError("공급 경로가 할당 락을 잡았다")

    reconciler._acquire_supply_lock_until = acquire_supply
    reconciler._acquire_allocator_lock_until = no_alloc_lock
    reconciler._condition = threading.Condition()
    reconciler._refill_condition = threading.Condition()
    reconciler._gate_unknown = {}
    reconciler._gate_unknown_since = {}
    reconciler.gate_unknown_max_age_seconds = 60.0
    reconciler._refill_not_before = 0.0
    reconciler._refill_generation = 1
    reconciler.refill_batch_limit = 4
    reconciler.refill_retry_seconds = 0.5
    reconciler.supply_lock_wait_seconds = 0.5
    reconciler._last_queued_counts = {}
    reconciler._known_policy_types = {"general"}
    reconciler.request_reconcile = lambda compute_type, **kwargs: None
    return reconciler


def refill(reconciler):
    return reconciler._refill_impl("general", time.monotonic())


class SchedulingGateTests(unittest.TestCase):
    def test_startup_releases_reserve(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["reason"], "released")
        self.assertEqual(provider.ungated, 2)

    def test_saturation_releases_up_to_room(self):
        """포화에서 한 번의 공급이 room 까지 다 푼다.

        이 단정이 10/10 회귀의 재발 방지선이다. 그때 목표가 폭 추정치에 묶여 3 에
        고정됐고 자리는 평균 19.64 개가 비어 있었다. 대기 183 건에 자리 20 개면
        예산은 20 이어야 한다. 묶음 상한(4)은 목록을 다시 읽는 간격일 뿐이고
        한 번의 공급 안에서 끝까지 푼다.
        """
        queues = GateQueues(queued=183)
        provider = GateProvider(current=20, assigned=20, ungated=0, gated=20)
        reconciler = gate_reconciler(provider, queues)
        self.assertEqual(refill(reconciler)["reason"], "released")
        self.assertEqual(provider.ungated, 20)
        # 두 번째는 예산을 이미 채웠으므로 아무것도 풀지 않는다.
        released_before = len(provider.released)
        self.assertEqual(refill(reconciler)["reason"], "budget_full")
        self.assertEqual(len(provider.released), released_before)

    def test_refill_never_takes_allocator_lock(self):
        """공급은 할당 락을 잡지 않는다 - gate_reconciler 의 할당 락은 잡으면 실패한다."""
        queues = GateQueues(queued=183)
        provider = GateProvider(current=10, assigned=30, ungated=0, gated=10)
        refill(gate_reconciler(provider, queues))
        self.assertNotIn("lock", queues.events)
        self.assertEqual(queues.events[0], "supply_lock")
        self.assertEqual(queues.events[-1], "supply_unlock")

    def test_claims_and_releases_during_refill_never_exceed_n(self):
        """게이트를 푸는 도중 claim 과 반납이 끼어들어도 노드 위 파드는 N 이하다.

        claim 은 게이트 풀린 available 하나를 assigned 로 옮길 뿐이고, 반납은
        assigned 를 줄인다. 공급은 묶음마다 목록을 다시 읽어 예산을 센다.
        """
        import random

        rng = random.Random(20261011)
        for trial in range(200):
            queues = GateQueues(queued=183)
            queues.policy["N"] = 10
            assigned = rng.randint(0, 10)
            provider = GateProvider(
                current=10 - assigned, assigned=assigned, ungated=0,
                gated=10 - assigned, n=10,
            )

            def interleave(p):
                roll = rng.random()
                if roll < 0.4 and p.ungated > 0:
                    p.ungated -= 1      # claim
                    p.assigned += 1
                    if p.current > 0:
                        p.current -= 1  # 할당기의 replicas 낮추기
                elif roll < 0.6 and p.assigned > 0:
                    p.assigned -= 1     # 세션 종료 후 삭제

            provider.after_release = interleave
            refill(gate_reconciler(provider, queues))
            with self.subTest(trial=trial):
                self.assertLessEqual(provider.peak_on_node, 10)

    def test_replicas_above_room_are_trimmed_before_release(self):
        """replicas 가 room 을 넘으면 공급이 직접 room 까지 CAS 로 내리고 나서 푼다.

        기다리면 reconcile 의 용량 트림이 축소 게이트를 세워 그동안 claim 이 막힌다.
        """
        queues = GateQueues(queued=183)
        provider = GateProvider(current=20, assigned=21, ungated=0, gated=20)
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(provider.lowers, [(20, 19)])
        self.assertEqual(result["trimmed"], 1)
        self.assertEqual(provider.ungated, 19)
        self.assertLessEqual(provider.peak_on_node, 40)

    def test_claim_between_list_and_raise_does_not_leave_replicas_above_room(self):
        """목록을 읽은 뒤 claim 이 끼어들어 room 보다 높게 올라가도 같은 공급이 바로잡는다."""
        queues = GateQueues(queued=183)
        queues.policy["N"] = 10
        provider = GateProvider(current=2, assigned=7, ungated=1, gated=1, n=10)

        def claim(p):
            p.ungated -= 1
            p.assigned += 1   # room 3 -> 2. 할당기는 current(2) <= room(2) 이라 안 낮춘다.

        provider.before_raise = claim
        refill(gate_reconciler(provider, queues))
        self.assertEqual(provider.raises, [(2, 3)])   # 낡은 room(3)으로 올렸다
        self.assertEqual(provider.lowers, [(3, 2)])   # 다음 목록에서 room(2)으로
        self.assertLessEqual(provider.current, 10 - provider.assigned)
        self.assertLessEqual(provider.peak_on_node, 10)

    def test_unowned_objects_do_not_block_safe_owned_gate_release(self):
        queues = GateQueues(queued=183)
        provider = GateProvider(
            current=2, assigned=38, ungated=1, gated=2,
            unowned=1, total=41,
        )
        refill(gate_reconciler(provider, queues))
        self.assertEqual(provider.ungated, 2)
        self.assertEqual(38 + provider.ungated, 40)

    def test_rollout_old_ungated_pod_leaves_budget_for_new_pod(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=1, gated=1)
        refill(gate_reconciler(provider, queues))
        self.assertEqual(provider.released, ["gated-0"])

    def test_lost_supply_lock_prevents_gate_patch(self):
        queues = GateQueues(queued=0)
        queues.lose_on_renew = 2
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["reason"], "lost_or_gated")
        self.assertEqual(provider.released, [])

    def test_leadership_loss_prevents_gate_patch(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        reconciler = gate_reconciler(provider, queues)
        calls = []

        def authority():
            calls.append(1)
            return len(calls) < 3

        reconciler._leadership_validator = authority
        refill(reconciler)
        self.assertEqual(provider.released, [])

    def test_scale_down_gate_stops_refill_before_any_write(self):
        queues = GateQueues(queued=183)
        queues.scale_down_gated = True
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["reason"], "scale_down_gated")
        self.assertIsNotNone(result["retry_after"])
        self.assertEqual(provider.released, [])
        self.assertEqual(queues.events, [])

    def test_new_leader_waits_before_releasing(self):
        queues = GateQueues(queued=183)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        reconciler = gate_reconciler(provider, queues)
        reconciler._refill_not_before = time.monotonic() + 5.0
        result = refill(reconciler)
        self.assertEqual(result["reason"], "leader_grace")
        self.assertGreater(result["retry_after"], 4.0)
        self.assertEqual(provider.released, [])

    def test_unknown_patch_counts_as_released_until_settled(self):
        """응답 없는 게이트 PATCH 는 반영됐다고 치고 같은 자리를 다시 풀지 않는다."""
        queues = GateQueues(queued=0)  # 목표 = R = 2
        provider = GateProvider(current=3, assigned=0, ungated=0, gated=3)
        provider.fail_release["gated-0"] = TimeoutError("read timed out")
        reconciler = gate_reconciler(provider, queues)
        result = refill(reconciler)
        self.assertEqual(result["reason"], "patch_unknown")
        self.assertEqual(result["unknown"], 1)
        self.assertEqual(reconciler._gate_unknown, {"general": {"gated-0": "1"}})

        # 요청이 서버에 닿지도 않았다면 버전이 그대로다. 같은 버전으로 다시 보내
        # 결판을 낸다 - 두 요청 중 최대 하나만 적용되므로 이중으로 세지 않는다.
        # 그 자리는 이미 예산에서 빠져 있으므로 새 예산으로는 하나만 더 푼다.
        result = refill(reconciler)
        self.assertEqual(provider.released, ["gated-0", "gated-1"])
        self.assertEqual(reconciler._gate_unknown, {})
        self.assertEqual(provider.ungated, 2)
        self.assertEqual(refill(reconciler)["reason"], "budget_full")

    def test_unknown_patch_that_landed_late_is_not_double_counted(self):
        """늦게 적용된 원래 PATCH 가 버전을 바꾸면 다시 보낸 요청은 test 에 걸린다."""
        queues = GateQueues(queued=0)
        provider = GateProvider(current=3, assigned=0, ungated=0, gated=3)
        provider.fail_release["gated-0"] = TimeoutError("read timed out")
        reconciler = gate_reconciler(provider, queues)
        refill(reconciler)
        # 원래 PATCH 가 지금 도착했다: 게이트가 풀리고 버전이 바뀐다.
        provider.gated_pods.remove("gated-0")
        provider.ungated += 1
        result = refill(reconciler)
        self.assertEqual(reconciler._gate_unknown, {})
        # 예산 2 = 늦게 풀린 gated-0 + gated-1. 셋째는 풀지 않는다.
        self.assertEqual(provider.released, ["gated-1"])
        self.assertEqual(provider.ungated, 2)
        self.assertEqual(result["reason"], "released")

    def test_unknown_that_keeps_failing_expires_after_request_timeout(self):
        """재전송이 계속 거절돼도 apiserver 요청 timeout 뒤에는 미상을 정리한다.

        그 뒤로는 첫 PATCH 가 서버에 남아 있을 수 없으므로 더 적용되지 않는다.
        정리하지 않으면 그 파드가 지워질 때까지 예산 하나가 샌다.
        """
        class Rejected(Exception):
            status = 403

        queues = GateQueues(queued=0)   # 목표 = R = 2
        provider = GateProvider(current=3, assigned=0, ungated=0, gated=3)
        provider.fail_release["gated-0"] = TimeoutError("read timed out")
        reconciler = gate_reconciler(provider, queues)
        refill(reconciler)
        provider.fail_release["gated-0"] = Rejected()
        refill(reconciler)
        # 아직 60초 안이다. gated-0 은 미상으로 남고 예산 하나를 잡는다.
        self.assertIn("gated-0", reconciler._gate_unknown["general"])
        self.assertEqual(provider.released, ["gated-1"])
        reconciler._gate_unknown_since[("general", "gated-0")] -= 61.0
        refill(reconciler)
        self.assertEqual(reconciler._gate_unknown, {})
        self.assertEqual(reconciler._gate_unknown_since, {})
        # 정리된 자리로 하나를 더 푼다(예산 2 = gated-1 + 하나).
        self.assertEqual(provider.ungated, 2)

    def test_definite_rejection_is_not_counted_as_unknown(self):
        """403 같은 확정 거절은 적용되지 않은 것이다. 미상으로 쌓여 예산이 새지 않는다."""
        class Rejected(Exception):
            status = 403

        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=3)
        provider.current = 3
        provider.fail_release["gated-0"] = Rejected()
        reconciler = gate_reconciler(provider, queues)
        refill(reconciler)
        self.assertEqual(reconciler._gate_unknown, {})
        self.assertEqual(provider.released, ["gated-1", "gated-2"])

    def test_old_generation_refill_stops_writing(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        reconciler = gate_reconciler(provider, queues)
        reconciler._refill_generation = 2
        result = reconciler._refill_impl("general", time.monotonic(), 1)
        self.assertEqual(result["reason"], "lost_or_gated")
        self.assertEqual(provider.released, [])

    def test_raise_uses_cas_and_rereads_on_conflict(self):
        """올리기는 CAS 다. 할당기가 그사이 낮췄으면 덮어쓰지 않고 다시 읽는다.

        GateProvider 에는 조건 없는 patch_deployment_replicas 가 없다. 부르면
        AttributeError 로 실패한다.
        """
        queues = GateQueues(queued=183)
        provider = GateProvider(current=5, assigned=30, ungated=0, gated=5)
        provider.raise_conflicts = 1
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["cas_conflicts"], 1)
        # 충돌 뒤 다시 읽은 값(4)에서 목표(10)까지 올렸다.
        self.assertEqual(provider.raises, [(4, 10)])
        self.assertEqual(provider.current, 10)
        # 올린 뒤 생긴 게이트 파드를 같은 공급에서 푼다.
        self.assertEqual(provider.ungated, 10)

    def test_repeated_cas_conflicts_back_off(self):
        queues = GateQueues(queued=183)
        provider = GateProvider(current=5, assigned=30, ungated=0, gated=5)
        provider.raise_conflicts = 5
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["reason"], "cas_conflict")
        self.assertIsNotNone(result["retry_after"])

    def test_queue_failure_cannot_collapse_dynamic_target_to_reserve(self):
        queues = GateQueues(queued=OSError("Redis unavailable"))
        provider = GateProvider(current=20, assigned=20, ungated=0, gated=20)
        reconciler = gate_reconciler(provider, queues)
        with self.assertRaises(Exception):
            reconciler._queued_for_target("general")
        with self.assertRaises(Exception):
            refill(reconciler)
        self.assertEqual(provider.released, [])
        self.assertFalse(queues.supply_locked)

    def test_demand_poll_keeps_its_own_comparison_baseline(self):
        queues = GateQueues(queued=1)
        reconciler = gate_reconciler(GateProvider(current=2, assigned=0, ungated=2, gated=0), queues)
        self.assertEqual(reconciler._poll_demand_changes(), {"general"})
        # Ordinary target and gate reads must not consume the next poll change.
        queues.queued = 2
        self.assertEqual(reconciler._queued_for_target("general"), 2)
        self.assertEqual(reconciler._poll_demand_changes(), {"general"})
        self.assertEqual(reconciler._poll_demand_changes(), set())

    def test_ready_sample_starts_when_gate_was_removed(self):
        queues = GateQueues()
        provider = types.SimpleNamespace(
            ANNOTATION_WARM_SLOT_RELEASED_AT="k8s-dynamic-allocator/warm-slot-released-at",
            _pod_is_ready=lambda pod: True,
            _pod_ready_at=lambda pod: datetime(2026, 10, 10, 1, 0, 8, tzinfo=timezone.utc),
        )
        reconciler = gate_reconciler(provider, queues)
        reconciler._churn_lock = threading.Lock()
        reconciler._churn = {}
        reconciler.cooldown_samples = 10
        samples = []
        reconciler._share_sample = lambda compute_type, kind, seconds: samples.append(seconds)
        pod = types.SimpleNamespace(metadata=types.SimpleNamespace(
            uid="new-pod",
            creation_timestamp=datetime(2026, 10, 10, 1, 0, 0, tzinfo=timezone.utc),
            annotations={provider.ANNOTATION_WARM_SLOT_RELEASED_AT: "2026-10-10T01:00:05Z"},
        ))
        reconciler._note_create_time("general", pod)
        self.assertEqual(samples, [3.0])


class RefillWakeTests(unittest.TestCase):
    def reconciler(self):
        reconciler = gate_reconciler(GateProvider(current=2, assigned=0, ungated=2, gated=0), GateQueues())
        reconciler.provider.LABEL_COMPUTE_TYPE = "compute-type"
        reconciler._note_create_time = lambda compute_type, pod: None
        reconciler.refill_debounce_seconds = 0.05
        reconciler._refill_due = {}
        reconciler._refill_requested_at = {}
        reconciler._refill_trigger = {}
        return reconciler

    def pod(self, deleting=False):
        return types.SimpleNamespace(metadata=types.SimpleNamespace(
            labels={"compute-type": "general"},
            deletion_timestamp=datetime(2026, 10, 11, tzinfo=timezone.utc) if deleting else None,
        ))

    def test_pod_events_that_change_supply_wake_refill(self):
        for event_type, deleting in (("ADDED", False), ("DELETED", False), ("MODIFIED", True)):
            reconciler = self.reconciler()
            reconciler.on_buffer_event(event_type, self.pod(deleting))
            with self.subTest(event=event_type, deleting=deleting):
                self.assertIn("general", reconciler._refill_due)
                self.assertEqual(reconciler._refill_trigger["general"], "event")

    def test_ready_transition_does_not_wake_refill(self):
        reconciler = self.reconciler()
        reconciler.on_buffer_event("MODIFIED", self.pod(False))
        self.assertEqual(reconciler._refill_due, {})

    def test_requests_coalesce_to_the_earliest(self):
        reconciler = self.reconciler()
        reconciler.request_refill("general", delay=5.0, trigger="retry")
        reconciler.request_refill("general", trigger="event")
        due = reconciler._refill_due["general"]
        self.assertLess(due - time.monotonic(), 1.0)
        # 처음 깨운 원인과 시각을 유지한다(wake_to_patch 측정용).
        self.assertEqual(reconciler._refill_trigger["general"], "retry")


class LockOrderQueues(GateQueues):
    """락 획득·해제 순서를 기록한다."""

    def __init__(self):
        super().__init__(queued=0)
        self.alloc_locked = False

    def acquire_allocator_lock(self, compute_type):
        if self.alloc_locked:
            self.events.append("alloc_miss")
            return None
        self.alloc_locked = True
        self.events.append("alloc_lock")
        return "alloc-token"

    def release_allocator_lock(self, compute_type, token):
        self.alloc_locked = False
        self.events.append("alloc_unlock")
        return True

    def acquire_supply_lock(self, compute_type):
        assert self.alloc_locked, "공급 락을 할당 락 없이 잡았다(축소/정책 경로)"
        self.supply_locked = True
        self.events.append("supply_lock")
        return "supply-token"

    def acquire_scale_down_gate(self, compute_type):
        self.scale_down_gated = True
        return "gate-token"

    def release_scale_down_gate(self, compute_type, token):
        self.scale_down_gated = False
        self.events.append("gate_release")
        return True

    def renew_scale_down_gate(self, compute_type, token):
        return True

    def get_buffer_policy(self, compute_type):
        return self.current_policy

    def clear_buffer_policy(self, compute_type):
        self.current_policy = None

    def set_buffer_policy(self, compute_type, deployment_name, R, N, resource_version=""):
        self.current_policy = {"deployment_name": deployment_name, "R": R, "N": N}


class LockOrderProvider(GateProvider):
    def __init__(self):
        super().__init__(current=6, assigned=0, ungated=6, gated=0)
        self.scale_patches = []

    def list_buffer_snapshot(self, compute_type):
        snapshot = super().list_buffer_snapshot(compute_type)
        snapshot["assigned_with_replicaset_owner"] = 0
        snapshot["schedulable_not_ready"] = 0
        return snapshot

    def patch_deployment_replicas(self, name, replicas):
        self.scale_patches.append(replicas)
        self.current = replicas
        self.ungated = min(self.ungated, replicas)
        return replicas


def lock_order_reconciler(queues, provider):
    reconciler = object.__new__(BufferCapacityReconciler)
    reconciler.provider = provider
    reconciler.queues = queues
    reconciler.dynamic_reserve = True
    reconciler.wait_timeout_seconds = 1.0
    reconciler.gate_renew_seconds = 5.0
    reconciler._stop_event = threading.Event()
    reconciler._leadership_validator = lambda: True
    reconciler._condition = threading.Condition()
    reconciler._last_queued_counts = {}
    reconciler._known_policy_types = {"general"}
    reconciler._policy_ready_holds = {}
    reconciler._starting_grace_seconds = lambda compute_type: 30.0
    reconciler.on_capacity_available = None
    return reconciler


class LockOrderTests(unittest.TestCase):
    def test_scale_down_takes_alloc_then_supply_and_releases_in_reverse(self):
        queues = LockOrderQueues()
        queues.current_policy = {"R": 2, "N": 40, "deployment_name": "compute-general"}
        provider = LockOrderProvider()
        reconciler = lock_order_reconciler(queues, provider)
        result = reconciler._scale_down_impl(
            "general",
            queues.current_policy,
            {"desired_replicas": 2, "current_replicas": 6},
        )
        self.assertEqual(result["status"], "scaled_down")
        self.assertEqual(provider.scale_patches, [2])
        order = [e for e in queues.events if e in ("alloc_lock", "supply_lock", "supply_unlock", "alloc_unlock", "gate_release")]
        # barrier(잡았다 바로 놓음) 뒤, 할당 락 -> 공급 락, 해제는 반대 순서, 축소 게이트는 마지막.
        self.assertEqual(
            order,
            ["alloc_lock", "alloc_unlock", "alloc_lock", "supply_lock",
             "supply_unlock", "alloc_unlock", "gate_release"],
        )

    def test_scale_down_patch_failure_holds_policy_and_releases_locks(self):
        """축소 PATCH 를 보낸 뒤 예외가 나도 안전 정지(보류 + 깃발 해제)가 걸린다."""
        queues = LockOrderQueues()
        queues.current_policy = {"R": 2, "N": 40, "deployment_name": "compute-general"}
        provider = LockOrderProvider()

        def broken_patch(name, replicas):
            raise TimeoutError("read timed out")

        provider.patch_deployment_replicas = broken_patch
        reconciler = lock_order_reconciler(queues, provider)
        cleared = []
        reconciler._clear_policy_ready_best_effort = lambda reason="-": cleared.append(reason)
        reconciler.request_policy_refresh = lambda retry=False: None
        with self.assertRaises(TimeoutError):
            reconciler._scale_down_impl(
                "general",
                queues.current_policy,
                {"desired_replicas": 2, "current_replicas": 6},
            )
        self.assertEqual(reconciler._policy_ready_holds, {"general": 2})
        self.assertEqual(cleared, ["scale_down_unobserved"])
        self.assertFalse(queues.alloc_locked)
        self.assertFalse(queues.supply_locked)
        # 깃발을 내린 뒤에 축소 게이트를 푼다.
        self.assertEqual(queues.events[-1], "gate_release")

    def test_policy_publish_takes_alloc_then_supply(self):
        queues = LockOrderQueues()
        queues.current_policy = {"R": 2, "N": 40, "deployment_name": "compute-general"}
        reconciler = lock_order_reconciler(queues, LockOrderProvider())
        new_policy = {"R": 2, "N": 20, "deployment_name": "compute-general"}
        self.assertTrue(reconciler._publish_policy_serialized("general", new_policy))
        self.assertEqual(queues.current_policy["N"], 20)
        order = [e for e in queues.events if e in ("alloc_lock", "supply_lock", "supply_unlock", "alloc_unlock")]
        self.assertEqual(order, ["alloc_lock", "supply_lock", "supply_unlock", "alloc_unlock"])


class SupplyOscillationTests(unittest.TestCase):
    """사전 점검(20261011-071750_DEVSMOKE)에서 본 진동의 재발 방지.

    공급이 목록을 먼저 읽고 replicas 를 나중에 읽던 시절, 두 읽기 사이에 claim 과
    할당기의 낮추기가 끼어들면 낡은 assigned 로 room 위까지 다시 올렸다. 그러면
    reconcile 이 축소 게이트로 바로잡으며 할당과 공급을 멈췄다(포화 6분에 42번).
    """

    def interleave_between_reads(self, provider, action):
        """먼저 불린 읽기(replicas 또는 목록)가 끝난 직후 action 을 끼워 넣는다."""
        state = {"done": False}
        read_replicas = provider.read_deployment_replicas
        list_snapshot = provider.list_buffer_snapshot

        def once():
            if not state["done"]:
                state["done"] = True
                action(provider)

        def replicas(name):
            value = read_replicas(name)
            once()
            return value

        def snapshot(compute_type):
            value = list_snapshot(compute_type)
            once()
            return value

        provider.read_deployment_replicas = replicas
        provider.list_buffer_snapshot = snapshot

    def test_claim_and_lowering_between_reads_never_raise_above_room(self):
        queues = GateQueues(queued=183)
        queues.policy["N"] = 10
        # assigned 6, room 4, replicas 4 = room (포화). Ready 1 + 게이트 1 - ReplicaSet 이
        # 아직 backfill 을 다 만들지 않아 headroom(N - total = 2)이 올리기를 막지 않는다.
        # (headroom 이 0 이면 옛 순서에서도 우연히 막혀 이 테스트가 판별력을 잃는다.)
        provider = GateProvider(current=4, assigned=6, ungated=1, gated=1, n=10)

        def claim_and_lower(p):
            p.ungated -= 1
            p.assigned += 1          # room 4 -> 3
            p.current -= 1           # 할당기: replicas 4 > room 3 이라 낮춘다

        self.interleave_between_reads(provider, claim_and_lower)
        refill(gate_reconciler(provider, queues))
        # 낡은 room(4)으로 다시 올리지 않는다.
        self.assertEqual(provider.raises, [])
        self.assertLessEqual(provider.current, 10 - provider.assigned)

    def test_lowered_n_is_left_to_gated_scale_down(self):
        """N 을 낮춘 직후(게이트 풀린 파드 > room)에는 게이트 없이 내리지 않는다.

        그때 내리면 ReplicaSet 이 Ready 파드(claim 중일 수 있다)를 지울 수 있다.
        """
        queues = GateQueues(queued=183)
        queues.policy["N"] = 10
        provider = GateProvider(current=8, assigned=5, ungated=7, gated=1, n=10)
        result = refill(gate_reconciler(provider, queues))
        self.assertEqual(result["reason"], "needs_scale_down")
        self.assertEqual(provider.lowers, [])
        self.assertEqual(provider.released, [])

    def test_supply_lock_is_released_between_rounds(self):
        """여러 바퀴를 돌 때 바퀴마다 공급 락을 놓는다(축소·반납이 끼어들 수 있게)."""
        queues = GateQueues(queued=183)
        provider = GateProvider(current=20, assigned=20, ungated=0, gated=20)
        refill(gate_reconciler(provider, queues))
        locks = queues.events.count("supply_lock")
        unlocks = queues.events.count("supply_unlock")
        self.assertGreaterEqual(locks, 5)            # 20개를 4개씩 -> 5바퀴 이상
        self.assertEqual(locks, unlocks)
        self.assertEqual(provider.ungated, 20)

    def test_resend_timeout_restarts_unknown_age(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=3, assigned=0, ungated=0, gated=3)
        provider.fail_release["gated-0"] = TimeoutError("read timed out")
        reconciler = gate_reconciler(provider, queues)
        refill(reconciler)
        key = ("general", "gated-0")
        reconciler._gate_unknown_since[key] -= 59.0
        provider.fail_release["gated-0"] = TimeoutError("read timed out again")
        refill(reconciler)
        # 재전송도 응답이 없었으므로 나이를 다시 잰다 - 60초가 지나도 바로 빼지 않는다.
        self.assertLess(time.monotonic() - reconciler._gate_unknown_since[key], 5.0)
        self.assertIn("gated-0", reconciler._gate_unknown["general"])


class ReconcileCapacityTrimTests(unittest.TestCase):
    """replicas > room 인 용량 교정은 축소 게이트 없이 공급 작업으로 넘긴다.

    축소 게이트를 세우던 경로가 셋이었다: 시작 중 파드가 있을 때, 쿨다운 대기 중일 때,
    그리고 목표가 room 과 같은 포화의 일반 축소.
    """

    def harness(self, provider, queues, *, starting=0, waiting=None):
        reconciler = gate_reconciler(provider, queues)
        reconciler._record_status = lambda *args, **kwargs: None
        reconciler.pipeline_width = lambda compute_type, n: None
        reconciler.cooldown_seconds = lambda compute_type: 0.0
        reconciler._note_target = lambda *args: None
        reconciler.create_time_seconds = lambda compute_type, quantile=0.9: 0.0
        reconciler.work_time_seconds = lambda compute_type, quantile=0.5: 0.0
        reconciler._sample_count = lambda compute_type, kind: 0
        reconciler.cooldown_min_samples = 10
        reconciler._starting_grace_seconds = lambda compute_type: 30.0
        reconciler._cooldown_blocks_scale_down = lambda compute_type, desired, cooldown: waiting
        reconciler._schedule_after = lambda *args, **kwargs: None
        self.refills = []
        self.scale_downs = []
        reconciler.request_refill = lambda compute_type, **kwargs: self.refills.append(kwargs)
        reconciler._scale_down = lambda compute_type, policy, base: (
            self.scale_downs.append(base) or {"status": "scaled_down"}
        )
        snapshot = provider.list_buffer_snapshot

        def with_starting(compute_type):
            value = snapshot(compute_type)
            value["schedulable_not_ready"] = starting
            value["oldest_schedulable_not_ready_since"] = None
            return value

        provider.list_buffer_snapshot = with_starting
        return reconciler

    def test_all_three_trim_paths_go_to_refill_without_gate(self):
        for label, kwargs in (
            ("starting", {"starting": 2}),
            ("cooldown", {"waiting": 5.0}),
            ("plain", {}),
        ):
            with self.subTest(path=label):
                queues = GateQueues(queued=183)
                # assigned 30, room 10, replicas 11 (room + 1), 게이트 풀린 8 <= room.
                provider = GateProvider(current=11, assigned=30, ungated=8, gated=3)
                result = self.harness(provider, queues, **kwargs).reconcile_type("general")
                self.assertEqual(result["status"], "refill_requested")
                self.assertEqual(result["reason"], "capacity_trim")
                self.assertEqual(self.scale_downs, [])
                self.assertEqual(len(self.refills), 1)
                self.assertFalse(result.get("retry"))

    def test_lowered_n_still_uses_gated_scale_down(self):
        queues = GateQueues(queued=183)
        # 게이트 풀린 11 > room 10: N 을 낮춘 직후. 축소 게이트를 세우는 경로로 간다.
        provider = GateProvider(current=11, assigned=30, ungated=11, gated=0)
        result = self.harness(provider, queues).reconcile_type("general")
        self.assertEqual(result["status"], "scaled_down")
        self.assertEqual(len(self.scale_downs), 1)
        self.assertEqual(self.refills, [])


if __name__ == "__main__":
    unittest.main()
