"""정책 깃발(buffer policy ready) 규칙의 회귀 테스트.

10/11 계측에서 포화 구간의 약 3분의 1 동안 할당이 policy_not_ready 로 멈췄다.
원인은 (1) replicas 변경마다 정책 새로고침을 하고 (2) 새로고침 바퀴의 reconcile
이 락 경합으로 미뤄지면 깃발을 내렸으며 (3) 깃발 갱신이 reconcile 바퀴에 묶여
긴 바퀴에서 TTL 이 만료된 것이다. 이 파일은 세 가지가 돌아오지 않는지 본다.
"""

import threading
import time
import types
import unittest

from test_buffer_target import reconciler_module

BufferCapacityReconciler = reconciler_module.BufferCapacityReconciler
QueueUnavailableError = reconciler_module.QueueUnavailableError


class FakeProvider:
    LABEL_COMPUTE_TYPE = "compute-type"
    ANNOTATION_BUFFER_RESERVE = "k8s-dynamic-allocator/buffer-reserve"
    ANNOTATION_BUFFER_CAPACITY = "k8s-dynamic-allocator/buffer-capacity"


class FakeQueues:
    def __init__(self):
        self.ready_token = None
        self.publishes = 0
        self.clears = 0
        self.renew_result = True
        self.renew_error = False

    def normalize_compute_type(self, value):
        return value or "general"

    def publish_buffer_policy_ready(self, token):
        self.ready_token = token
        self.publishes += 1
        return True

    def renew_buffer_policy_ready(self, token):
        if self.renew_error:
            raise QueueUnavailableError("redis down")
        return self.renew_result and self.ready_token == token

    def clear_buffer_policy_ready(self, token=None):
        self.clears += 1
        if token is None or self.ready_token == token:
            self.ready_token = None
        return True


def make_reconciler(queues=None):
    reconciler = object.__new__(BufferCapacityReconciler)
    reconciler.provider = FakeProvider()
    reconciler.queues = queues or FakeQueues()
    reconciler.on_capacity_available = None
    reconciler.on_periodic_cleanup = None
    reconciler.debounce_seconds = 0.0
    reconciler.resync_seconds = 60.0
    reconciler.policy_ready_renew_seconds = 5.0
    reconciler.policy_ready_ttl_seconds = 15.0
    reconciler.dynamic_reserve = False
    reconciler._condition = threading.Condition()
    reconciler._stop_event = threading.Event()
    reconciler._thread = None
    reconciler._pending_types = set()
    reconciler._policy_refresh_pending = True
    reconciler._next_run_at = None
    reconciler._known_policy_types = set()
    reconciler._retry_counts = {}
    reconciler._policy_retry_count = 0
    reconciler._policy_ready_token = "token-1"
    reconciler._policy_ready = False
    reconciler._last_policy_sync_succeeded = False
    reconciler._leadership_validator = lambda: True
    reconciler._policy_ready_lock = threading.Lock()
    reconciler._policy_ready_renewed_at = None
    reconciler._renewer_generation = 0
    reconciler._policy_fingerprints = {}
    reconciler._initial_publish_pending = True
    reconciler._policy_ready_holds = {}
    return reconciler


def deployment(
    name="compute-general",
    compute_type="general",
    r="2",
    n="40",
    template_status="available",
):
    return types.SimpleNamespace(
        metadata=types.SimpleNamespace(
            name=name,
            labels={"compute-type": compute_type},
            annotations={
                "k8s-dynamic-allocator/buffer-reserve": r,
                "k8s-dynamic-allocator/buffer-capacity": n,
            },
        ),
        spec=types.SimpleNamespace(
            selector=types.SimpleNamespace(
                match_labels={"app": "compute-pod", "compute-type": compute_type}
            ),
            template=types.SimpleNamespace(
                metadata=types.SimpleNamespace(
                    labels={"app": "compute-pod", "compute-status": template_status}
                )
            ),
        ),
    )


class RefreshFilterTests(unittest.TestCase):
    def test_replicas_only_modification_does_not_refresh(self):
        reconciler = make_reconciler()
        self.assertTrue(reconciler._policy_inputs_changed("MODIFIED", deployment()))
        # replicas·status 만 바뀐 MODIFIED 는 metadata 의 세 값이 같다.
        self.assertFalse(reconciler._policy_inputs_changed("MODIFIED", deployment()))

    def test_policy_annotation_or_label_change_refreshes(self):
        reconciler = make_reconciler()
        reconciler._policy_inputs_changed("MODIFIED", deployment())
        self.assertTrue(reconciler._policy_inputs_changed("MODIFIED", deployment(n="20")))
        self.assertTrue(reconciler._policy_inputs_changed("MODIFIED", deployment(r="4", n="20")))
        self.assertTrue(
            reconciler._policy_inputs_changed(
                "MODIFIED", deployment(compute_type="gpu", r="4", n="20")
            )
        )

    def test_template_label_change_refreshes(self):
        """parse_deployment_policy 가 유효성 판정에 쓰는 템플릿 라벨."""
        reconciler = make_reconciler()
        reconciler._policy_inputs_changed("MODIFIED", deployment())
        self.assertTrue(
            reconciler._policy_inputs_changed(
                "MODIFIED", deployment(template_status="broken")
            )
        )

    def test_non_modified_events_always_refresh(self):
        reconciler = make_reconciler()
        reconciler._policy_inputs_changed("MODIFIED", deployment())
        for event_type in ("ADDED", "SYNC", "DELETED"):
            self.assertTrue(reconciler._policy_inputs_changed(event_type, deployment()))

    def test_deleted_forgets_fingerprint(self):
        reconciler = make_reconciler()
        reconciler._policy_inputs_changed("MODIFIED", deployment())
        reconciler._policy_inputs_changed("DELETED", deployment())
        # 지운 뒤 같은 이름이 다시 나타나면 처음 보는 Deployment 로 다룬다.
        self.assertTrue(reconciler._policy_inputs_changed("MODIFIED", deployment()))


class RunPassHarness:
    """_run 을 실제 스레드로 돌리고 첫 바퀴(주기 resync = 새로고침 바퀴)만 본다."""

    def __init__(self, reconciler, results):
        self.reconciler = reconciler
        self.results = list(results)
        self.calls = 0
        reconciler._last_policy_sync_succeeded = True
        reconciler.sync_policies = lambda: {"general"}
        reconciler.reconcile_type = self._reconcile
        reconciler._release_scheduling_gates = lambda compute_type: True
        reconciler._poll_demand_changes = lambda: set()

    def _reconcile(self, compute_type):
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        if result.get("hold"):
            with self.reconciler._condition:
                self.reconciler._policy_ready_holds[compute_type] = 0
        return {key: value for key, value in result.items() if key != "hold"}

    def run_passes(self, passes=1):
        thread = threading.Thread(target=self.reconciler._run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5.0
        while self.calls < passes and time.monotonic() < deadline:
            # 다음 바퀴를 바로 돌게 하되 새로고침도 함께 일어나게 한다.
            with self.reconciler._condition:
                if self.reconciler._next_run_at is None or self.calls < passes:
                    self.reconciler._policy_refresh_pending = True
                    self.reconciler._pending_types.add("general")
                    self.reconciler._next_run_at = time.monotonic()
                    self.reconciler._condition.notify_all()
            time.sleep(0.02)
        time.sleep(0.05)
        self.reconciler._stop_event.set()
        with self.reconciler._condition:
            self.reconciler._condition.notify_all()
        thread.join(timeout=2.0)


LOCK_TIMEOUT = {"status": "deferred", "reason": "allocator_lock_timeout", "retry": True}
ERROR = {"status": "error", "reason": "ReadTimeout", "retry": True}
CONVERGED = {"status": "converged"}


class PolicyReadyRuleTests(unittest.TestCase):
    def published_after(self, reconciler, results, passes=1):
        queues = reconciler.queues
        # _run 의 finally 가 종료 시 깃발을 지우므로 게시 횟수로 판정한다.
        harness = RunPassHarness(reconciler, results)
        harness.run_passes(passes)
        # _run 이 예외로 죽어도 게시 0 이 되므로 reconcile 이 실제로 돌았는지 확인한다.
        self.assertGreaterEqual(harness.calls, passes)
        return queues.publishes

    def test_transient_reconcile_failure_keeps_flag_after_initial_publish(self):
        for result in (LOCK_TIMEOUT, ERROR):
            reconciler = make_reconciler()
            reconciler._initial_publish_pending = False
            self.assertGreaterEqual(self.published_after(reconciler, [result]), 1, result)

    def test_initial_publish_waits_for_clean_reconcile(self):
        reconciler = make_reconciler()
        self.assertEqual(self.published_after(reconciler, [LOCK_TIMEOUT]), 0)
        self.assertTrue(reconciler._initial_publish_pending)

    def test_initial_publish_after_clean_reconcile(self):
        reconciler = make_reconciler()
        self.assertGreaterEqual(self.published_after(reconciler, [CONVERGED]), 1)
        self.assertFalse(reconciler._initial_publish_pending)

    def test_unobserved_scale_down_holds_until_deletion_observed(self):
        reconciler = make_reconciler()
        reconciler._initial_publish_pending = False
        held = {
            "status": "deferred",
            "reason": "scale_down_observation_timeout",
            "retry": True,
            "hold": True,
        }
        observed = []
        reconciler._scale_down_hold_resolved = (
            lambda compute_type, target: bool(observed)
        )
        self.assertEqual(self.published_after(reconciler, [held]), 0)
        self.assertEqual(reconciler._policy_ready_holds, {"general": 0})

        # 재시도 없이 끝난 바퀴라도(쿨다운 보류 등) 삭제를 아직 못 봤으면 유지한다.
        reconciler._stop_event.clear()
        cooldown = {"status": "deferred", "reason": "scale_down_cooldown"}
        self.assertEqual(self.published_after(reconciler, [cooldown]), 0)
        self.assertEqual(reconciler._policy_ready_holds, {"general": 0})

        observed.append(True)
        reconciler._stop_event.clear()
        self.assertGreaterEqual(self.published_after(reconciler, [CONVERGED]), 1)
        self.assertEqual(reconciler._policy_ready_holds, {})

    def test_policy_sync_failure_clears_flag(self):
        reconciler = make_reconciler()
        reconciler._initial_publish_pending = False
        harness = RunPassHarness(reconciler, [CONVERGED])

        def failed_sync():
            reconciler._last_policy_sync_succeeded = False
            return set()

        reconciler.sync_policies = failed_sync
        reconciler._policy_ready = True
        reconciler.queues.ready_token = "token-1"
        # _run 의 finally 도 종료 시 깃발을 지우므로, 상태가 아니라 사유로 판정한다.
        with self.assertLogs(reconciler_module.logger, level="INFO") as logs:
            harness.run_passes(1)
        self.assertGreaterEqual(harness.calls, 1)
        self.assertEqual(reconciler.queues.publishes, 0)
        self.assertTrue(
            any(
                "[BufferPolicyReadyCleared]" in line
                and "reason=policy_sync_failed" in line
                for line in logs.output
            )
        )


class RenewerTests(unittest.TestCase):
    def published(self):
        reconciler = make_reconciler()
        refreshes = []
        reconciler.request_policy_refresh = lambda retry=False: refreshes.append(retry)
        reconciler._publish_policy_ready("test")
        return reconciler, refreshes

    def test_renew_keeps_flag(self):
        reconciler, refreshes = self.published()
        reconciler._renew_policy_ready_once()
        self.assertTrue(reconciler._policy_ready)
        self.assertEqual(refreshes, [])

    def test_expired_flag_requests_refresh(self):
        reconciler, refreshes = self.published()
        reconciler.queues.ready_token = None  # TTL 만료
        with self.assertLogs(reconciler_module.logger, level="WARNING") as logs:
            reconciler._renew_policy_ready_once()
        self.assertFalse(reconciler._policy_ready)
        self.assertEqual(refreshes, [False])
        self.assertTrue(
            any("[BufferPolicyReadyLost]" in line and "reason=expired_or_replaced" in line
                for line in logs.output)
        )

    def test_redis_error_within_ttl_keeps_flag(self):
        """키가 아직 살아 있을 수 있으므로 한 번의 오류로 갱신을 멈추지 않는다."""
        reconciler, refreshes = self.published()
        reconciler.queues.renew_error = True
        with self.assertLogs(reconciler_module.logger, level="WARNING"):
            reconciler._renew_policy_ready_once()
        self.assertTrue(reconciler._policy_ready)
        self.assertEqual(refreshes, [])

    def test_redis_error_past_ttl_requests_refresh(self):
        reconciler, refreshes = self.published()
        reconciler.queues.renew_error = True
        reconciler._policy_ready_renewed_at = time.monotonic() - 20.0
        with self.assertLogs(reconciler_module.logger, level="WARNING"):
            reconciler._renew_policy_ready_once()
        self.assertFalse(reconciler._policy_ready)
        self.assertEqual(refreshes, [False])

    def test_leadership_loss_clears_and_schedules_republish(self):
        reconciler, refreshes = self.published()
        reconciler._leadership_validator = lambda: False
        reconciler._renew_policy_ready_once()
        self.assertFalse(reconciler._policy_ready)
        self.assertIsNone(reconciler.queues.ready_token)
        # 유효성이 잠깐 끊긴 경우를 위해 권한이 돌아오면 바로 게시하도록 예약한다.
        self.assertEqual(refreshes, [False])

    def test_publish_refuses_without_authority(self):
        reconciler = make_reconciler()
        reconciler._leadership_validator = lambda: False
        self.assertFalse(reconciler._publish_policy_ready("test"))
        self.assertEqual(reconciler.queues.publishes, 0)
        self.assertFalse(reconciler._policy_ready)

    def test_not_ready_is_noop(self):
        reconciler = make_reconciler()
        reconciler.request_policy_refresh = lambda retry=False: self.fail("refresh")
        reconciler._renew_policy_ready_once()
        self.assertEqual(reconciler.queues.publishes, 0)

    def test_renewer_runs_independently_of_reconcile_loop(self):
        """reconcile 바퀴가 돌지 않아도 타이머가 TTL 을 연장한다."""
        reconciler, _ = self.published()
        renews = []
        original = reconciler.queues.renew_buffer_policy_ready

        def counting(token):
            renews.append(token)
            return original(token)

        reconciler.queues.renew_buffer_policy_ready = counting
        reconciler.policy_ready_renew_seconds = 0.02
        reconciler._renewer_generation = 1
        thread = threading.Thread(
            target=reconciler._run_policy_ready_renewer, args=(1,), daemon=True
        )
        thread.start()
        time.sleep(0.15)
        reconciler._stop_event.set()
        thread.join(timeout=1.0)
        self.assertGreaterEqual(len(renews), 3)
        self.assertTrue(reconciler._policy_ready)

    def test_old_generation_renewer_exits(self):
        reconciler, _ = self.published()
        reconciler.policy_ready_renew_seconds = 0.02
        reconciler._renewer_generation = 2
        thread = threading.Thread(
            target=reconciler._run_policy_ready_renewer, args=(1,), daemon=True
        )
        thread.start()
        thread.join(timeout=1.0)
        self.assertFalse(thread.is_alive())


if __name__ == "__main__":
    unittest.main()
