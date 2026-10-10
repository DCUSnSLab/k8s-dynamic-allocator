"""Regression checks for the dynamic available-Pod target.

Load the reconciler directly so this pure calculation can be checked without
the controller's HTTP and Redis runtime dependencies.
"""

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import threading
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
        self.locked = False
        self.on_unlock = None

    def renew_allocator_lock(self, compute_type, token):
        assert self.locked
        self.renew_count += 1
        self.events.append("renew")
        return self.renew_count != self.lose_on_renew

    def release_allocator_lock(self, compute_type, token):
        self.events.append("unlock")
        self.locked = False
        if self.on_unlock:
            self.on_unlock()
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
    def __init__(self, *, current, assigned, ungated, gated, total=None, unowned=0):
        self.current = current
        self.assigned = assigned
        self.ungated = ungated
        self.gated_pods = [f"gated-{i}" for i in range(gated)]
        self.total = total
        self.unowned = unowned
        self.released = []

    def list_buffer_snapshot(self, compute_type):
        available = self.ungated + len(self.gated_pods)
        return {
            "buffer_assigned": self.assigned,
            "buffer_available": available,
            "buffer_total": self.total if self.total is not None else self.assigned + available,
            "ungated_available": self.ungated,
            "unowned_available": self.unowned,
            "gated_available_candidates": [
                {"name": name, "resource_version": "1", "gate_index": 0}
                for name in self.gated_pods
            ],
        }

    def read_deployment_replicas(self, name):
        return self.current

    def release_pod_scheduling_gate(self, name, resource_version, gate_index):
        if name not in self.gated_pods:
            return False
        self.released.append(name)
        self.gated_pods.remove(name)
        self.ungated += 1
        return True


def gate_reconciler(provider, queues):
    reconciler = object.__new__(BufferCapacityReconciler)
    reconciler.provider = provider
    reconciler.queues = queues
    reconciler.dynamic_reserve = True
    reconciler.wait_timeout_seconds = 1.0
    reconciler._stop_event = threading.Event()
    reconciler._leadership_validator = lambda: True
    def acquire(*args):
        assert not queues.locked
        queues.locked = True
        queues.events.append("lock")
        return "lock-token"
    reconciler._acquire_allocator_lock_until = acquire
    reconciler._condition = threading.Condition()
    reconciler._last_queued_counts = {}
    reconciler._known_policy_types = {"general"}
    reconciler.request_reconcile = lambda compute_type: None
    return reconciler


class SchedulingGateTests(unittest.TestCase):
    def test_startup_releases_reserve(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        self.assertTrue(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.ungated, 2)

    def test_saturation_releases_up_to_room(self):
        """포화에서 한 패스가 room 까지 다 푼다.

        이 단정이 10/10 회귀의 재발 방지선이다. 그때 목표가 폭 추정치에 묶여 3 에
        고정됐고 자리는 평균 19.64 개가 비어 있었다. 대기 183 건에 자리 20 개면
        예산은 20 이어야 하고, 패스당 일부만 푸는 규칙을 넣으면 같은 공급 부족이
        다시 생긴다. 파드마다 락을 놓으므로 한 패스에 다 풀어도 할당은 끼어든다
        (test_claim_can_interleave_between_gate_patches 가 그것을 고정한다).
        """
        queues = GateQueues(queued=183)
        provider = GateProvider(current=20, assigned=20, ungated=0, gated=20)
        reconciler = gate_reconciler(provider, queues)
        self.assertTrue(reconciler._release_scheduling_gates("general"))
        self.assertEqual(provider.ungated, 20)
        # 두 번째 패스는 예산을 이미 채웠으므로 아무것도 풀지 않고 성공한다.
        released_before = len(provider.released)
        self.assertTrue(reconciler._release_scheduling_gates("general"))
        self.assertEqual(provider.ungated, 20)
        self.assertEqual(len(provider.released), released_before)

    def test_claim_can_interleave_between_gate_patches(self):
        queues = GateQueues(queued=183)
        queues.policy["N"] = 2
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)

        def claim_after_first_unlock():
            queues.on_unlock = None
            queues.events.append("claim")
            provider.ungated -= 1
            provider.assigned += 1

        queues.on_unlock = claim_after_first_unlock
        self.assertFalse(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.released, ["gated-0"])
        self.assertEqual(queues.events[:5], ["lock", "renew", "renew", "unlock", "claim"])
        self.assertEqual(queues.events[5], "lock")

    def test_overshoot_waits_for_replica_correction(self):
        queues = GateQueues(queued=183)
        provider = GateProvider(current=20, assigned=21, ungated=0, gated=20)
        self.assertFalse(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.released, [])

    def test_unowned_objects_do_not_block_safe_owned_gate_release(self):
        queues = GateQueues(queued=183)
        provider = GateProvider(
            current=2, assigned=38, ungated=1, gated=2,
            unowned=1, total=41,
        )
        self.assertTrue(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.ungated, 2)
        self.assertEqual(38 + provider.ungated, 40)

    def test_rollout_old_ungated_pod_leaves_budget_for_new_pod(self):
        queues = GateQueues(queued=0)
        provider = GateProvider(current=2, assigned=0, ungated=1, gated=1)
        self.assertTrue(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.released, ["gated-0"])

    def test_lost_lock_prevents_gate_patch(self):
        queues = GateQueues(queued=0)
        queues.lose_on_renew = 2
        provider = GateProvider(current=2, assigned=0, ungated=0, gated=2)
        self.assertFalse(gate_reconciler(provider, queues)._release_scheduling_gates("general"))
        self.assertEqual(provider.released, [])

    def test_queue_failure_cannot_collapse_dynamic_target_to_reserve(self):
        queues = GateQueues(queued=OSError("Redis unavailable"))
        provider = GateProvider(current=20, assigned=20, ungated=0, gated=20)
        reconciler = gate_reconciler(provider, queues)
        with self.assertRaises(Exception):
            reconciler._queued_for_target("general")
        self.assertFalse(reconciler._release_scheduling_gates("general"))
        self.assertEqual(provider.released, [])

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


if __name__ == "__main__":
    unittest.main()
