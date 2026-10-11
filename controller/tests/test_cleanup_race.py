"""정리(cleanup)가 낡은 목록으로 사용 중이거나 회복한 파드를 지우지 않는다.

정리는 몇 초 전의 목록으로 "NotReady 가 오래된 파드" 를 고른다. 그사이
  - idle 이 Ready 로 돌아와 요청에 할당됐거나(라벨이 바뀐다),
  - 라벨은 그대로(idle 또는 assigned)인 채 Ready 만 돌아왔으면
지우면 안 된다.
  - 정리는 목록에서 본 상태와 NotReady 기준(grace)을 반납에 넘긴다. 반납은 방금 읽은 파드로
    둘 다 다시 판단한다(정리와 같은 기준).
  - NotReady 사유의 삭제와 idle 삭제는 읽은 버전을 전제로 한다. 읽은 뒤 할당되거나 Ready 로
    돌아오면(버전이 바뀐다) 지워지지 않는다.
  - 무시된 반납은 released 로 세지 않는다.
"""

import contextlib
import importlib.util
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


class FakeApiException(Exception):
    def __init__(self, status=None, reason=""):
        super().__init__(reason)
        self.status = status


class FakeQueueUnavailable(Exception):
    pass


def _stub_modules():
    modules = {}
    for name in ("controller", "controller.services", "controller.services.compute"):
        package = types.ModuleType(name)
        package.__path__ = []
        modules[name] = package
    settings = types.ModuleType("config.settings")
    settings.COMPUTE_NOT_READY_GRACE_SECONDS = 30.0
    # 재사용 arm 의 반납 잠금 설정. 이 브랜치가 읽지 않으면 쓰이지 않는다(같은 테스트를
    # 모든 arm 에서 돌리기 위해 둔다).
    settings.RELEASE_LOCK_STALE_SECONDS = 60.0
    settings.request_label_scope = lambda *args, **kwargs: contextlib.nullcontext()
    settings.set_request_label = lambda *args, **kwargs: None
    config = types.ModuleType("config")
    config.settings = settings
    ticket_format = types.ModuleType("controller.services.ticket_format")
    ticket_format.set_ticket_context = lambda ticket: None
    ticket_format.log_queue_event = lambda *args, **kwargs: None
    ticket_format.datetime_to_epoch_ms = lambda value: 0
    modules["controller.services"].ticket_format = ticket_format
    queue = types.ModuleType("controller.services.queue")
    queue.QueueUnavailableError = FakeQueueUnavailable
    queue.safe_int = lambda value, default=0: default
    queue.release_lock_age_seconds = lambda token, now=None: None
    agent_client = types.ModuleType("controller.services.compute.agent_client")
    agent_client.ComputeAgent = None  # 이 테스트의 파드에는 IP 가 없어 부르지 않는다
    agent_client.ComputeAgentError = type("ComputeAgentError", (Exception,), {})
    agent_client.AgentTicketMismatch = type("AgentTicketMismatch", (agent_client.ComputeAgentError,), {})
    kubernetes = types.ModuleType("kubernetes")
    kclient = types.ModuleType("kubernetes.client")
    rest = types.ModuleType("kubernetes.client.rest")
    rest.ApiException = FakeApiException
    kubernetes.client = kclient
    kclient.rest = rest
    modules.update({
        "config": config,
        "config.settings": settings,
        ticket_format.__name__: ticket_format,
        queue.__name__: queue,
        agent_client.__name__: agent_client,
        "kubernetes": kubernetes,
        "kubernetes.client": kclient,
        "kubernetes.client.rest": rest,
    })
    return modules


def load_module(short):
    name = f"controller.services.compute.{short}"
    with patch.dict(sys.modules, _stub_modules()):
        spec = importlib.util.spec_from_file_location(name, ROOT / "services" / "compute" / f"{short}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return module


ComputeReleaser = load_module("releaser").ComputeReleaser
ComputeCleanup = load_module("cleanup").ComputeCleanup


class PodApi:
    """apiserver 의 파드 한 개. 할당(claim)은 버전을 올린다."""

    LABEL_COMPUTE_TYPE = "compute-type"
    LABEL_STATUS = "compute-status"
    STATUS_IDLE = "idle"
    STATUS_ASSIGNED = "assigned"
    ANNOTATION_ALLOCATION_TICKET = "k8s-dynamic-allocator/allocation-ticket-id"
    namespace = "kda-test"
    api_request_timeout = 5

    GRACE = 30.0

    def __init__(self, status="idle", not_ready_for: Optional[float] = None):
        """not_ready_for: Ready 였다가 떨어진 지 몇 초인가(None 이면 Ready)."""
        self.status = status
        self.not_ready_since = (
            None if not_ready_for is None else datetime.now(timezone.utc) - timedelta(seconds=not_ready_for)
        )
        self.rv = 1
        self.deleted = False
        self.calls: List[str] = []
        self.v1 = SimpleNamespace(read_namespaced_pod=self._read)

    def _read(self, name, namespace, _request_timeout=None):
        if self.deleted:
            raise FakeApiException(404, "NotFound")
        return SimpleNamespace(
            metadata=SimpleNamespace(
                labels={"compute-type": "general", "compute-status": self.status},
                annotations={},
                uid="uid-1",
                resource_version=str(self.rv),
                deletion_timestamp=None,
            ),
            status=SimpleNamespace(pod_ip="", not_ready_since=self.not_ready_since),
        )

    @staticmethod
    def _pod_not_ready_since(pod):
        # 실제(kubernetes_client._pod_not_ready_since)와 같은 뜻: Ready 면 None.
        return pod.status.not_ready_since

    def claim(self):
        self.status = "assigned"
        self.rv += 1

    def recover(self):
        """라벨은 그대로 두고 Ready 만 돌아온다(kubelet 이 상태를 써서 버전이 오른다)."""
        self.not_ready_since = None
        self.rv += 1

    def get_pod_ip(self, name):
        return ""

    def release_pod(self, name):
        self.calls.append("delete")
        if self.deleted:
            return False
        self.deleted = True
        return True

    def delete_idle_pod(self, name, uid, rv):
        return self.delete_pod_if_unchanged(name, uid, rv)

    def delete_pod_if_unchanged(self, name, uid, rv):
        self.calls.append("delete_if_unchanged")
        if self.deleted or uid != "uid-1" or rv != str(self.rv):
            return False
        self.deleted = True
        return True


class Queues:
    default_compute_type = "general"

    def normalize_compute_type(self, value):
        return (value or "general").strip().lower() or "general"


class Tickets:
    def __init__(self, before_lookup=None):
        self.before_lookup = before_lookup
        self.cleared: List[str] = []

    def get_assigned_request_context(self, compute_pod):
        if self.before_lookup:
            self.before_lookup()
        return {}

    def get_ticket_snapshot(self, ticket_id):
        return None

    def find_ticket_by_compute_pod_index_only(self, compute_pod, compute_type=None):
        return None

    def clear_assigned_request_context(self, compute_pod):
        self.cleared.append(compute_pod)

    def clear_assigned_request_context_if_owner(self, compute_pod, ticket_id):
        self.cleared.append(compute_pod)
        return []


def releaser_for(pod, tickets=None):
    return ComputeReleaser(pod, Queues(), tickets or Tickets())


def cleanup_release(pod, status, tickets=None):
    """정리가 NotReady 사유로 부르는 것과 같은 호출."""
    return releaser_for(pod, tickets).release_unreachable_compute_pod(
        "pod-a", expected_status=status, min_not_ready_seconds=PodApi.GRACE
    )


class ReleaseExpectationTests(unittest.TestCase):
    def test_idle_seen_by_cleanup_but_assigned_now_is_left_alone(self):
        pod = PodApi(status="assigned")
        result = releaser_for(pod).release_unreachable_compute_pod("pod-a", expected_status="idle")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])
        self.assertFalse(pod.deleted)

    def test_claim_between_read_and_delete_keeps_the_pod(self):
        """읽은 때는 idle 이었지만 지우기 전에 할당됐다(버전이 바뀌었다)."""
        pod = PodApi(status="idle")
        tickets = Tickets(before_lookup=pod.claim)
        result = releaser_for(pod, tickets).release_unreachable_compute_pod("pod-a", expected_status="idle")
        self.assertEqual(result["outcome"], "ignored")
        self.assertNotIn("delete", pod.calls)  # 조건 없는 삭제를 하지 않는다
        self.assertFalse(pod.deleted)
        self.assertEqual(tickets.cleared, [])  # 새 주인의 컨텍스트를 지우지 않는다

    def test_unchanged_idle_is_deleted_with_preconditions(self):
        pod = PodApi(status="idle")
        result = releaser_for(pod).release_unreachable_compute_pod("pod-a", expected_status="idle")
        self.assertNotEqual(result.get("outcome"), "ignored")
        self.assertNotIn("delete", pod.calls)  # 버전을 전제로 지웠다
        self.assertTrue(pod.deleted)

    def test_assigned_pod_is_deleted_as_before(self):
        pod = PodApi(status="assigned")
        result = releaser_for(pod).release_unreachable_compute_pod("pod-a", expected_status="assigned")
        self.assertNotEqual(result.get("outcome"), "ignored")
        self.assertTrue(pod.deleted)

    def test_user_release_without_expectation_is_unchanged(self):
        pod = PodApi(status="assigned")
        releaser_for(pod).release_compute_pod("pod-a")
        self.assertTrue(pod.deleted)


class NotReadyStillValidTests(unittest.TestCase):
    """라벨은 그대로인 채 Ready 만 돌아온 경우(다른 세션 지적)."""

    def test_assigned_pod_that_recovered_is_left_alone(self):
        pod = PodApi(status="assigned", not_ready_for=None)
        result = cleanup_release(pod, "assigned")
        self.assertEqual(result["outcome"], "ignored")
        self.assertEqual(pod.calls, [])
        self.assertFalse(pod.deleted)

    def test_idle_pod_that_recovered_is_left_alone(self):
        pod = PodApi(status="idle", not_ready_for=None)
        result = cleanup_release(pod, "idle")
        self.assertEqual(result["outcome"], "ignored")
        self.assertFalse(pod.deleted)

    def test_pod_that_flapped_back_to_not_ready_recently_is_left_alone(self):
        """회복했다가 방금 다시 NotReady: 정리의 기준(grace)을 아직 채우지 못했다."""
        pod = PodApi(status="assigned", not_ready_for=PodApi.GRACE / 3)
        result = cleanup_release(pod, "assigned")
        self.assertEqual(result["outcome"], "ignored")
        self.assertFalse(pod.deleted)

    def test_recovery_between_read_and_delete_keeps_the_pod(self):
        pod = PodApi(status="assigned", not_ready_for=PodApi.GRACE * 2)
        tickets = Tickets(before_lookup=pod.recover)
        result = cleanup_release(pod, "assigned", tickets)
        self.assertEqual(result["outcome"], "ignored")
        self.assertNotIn("delete", pod.calls)  # 조건 없는 삭제를 하지 않는다
        self.assertFalse(pod.deleted)

    def test_still_not_ready_past_grace_is_deleted(self):
        for status in ("assigned", "idle"):
            pod = PodApi(status=status, not_ready_for=PodApi.GRACE * 2)
            result = cleanup_release(pod, status)
            self.assertNotEqual(result.get("outcome"), "ignored", status)
            self.assertTrue(pod.deleted, status)


class CleanupPassesWhatItSawTests(unittest.TestCase):
    def _cleanup(self, buffer_status, outcome=None):
        calls: List[Dict] = []
        not_ready_since = datetime.now(timezone.utc) - timedelta(seconds=120)
        provider = SimpleNamespace(
            STATUS_IDLE="idle",
            STATUS_ASSIGNED="assigned",
            list_buffer_status=lambda: [{
                "name": "pod-a", "buffer_status": buffer_status, "ready": False,
                "terminating": False, "not_ready_since": not_ready_since,
                "assigned_user": "", "allocation_ticket_id": "",
            }],
        )
        response = {"status": "success", "message": ""}
        if outcome:
            response["outcome"] = outcome

        def release_unreachable(compute_pod, request_context=None, expected_status=None,
                                min_not_ready_seconds=None, **_):
            calls.append({"pod": compute_pod, "expected_status": expected_status,
                          "min_not_ready_seconds": min_not_ready_seconds})
            return response

        manager = SimpleNamespace(
            recover_stale_ticket=lambda ticket: {"status": "skipped"},
            get_assigned_request_context=lambda compute_pod: {},
            release_unreachable_compute_pod=release_unreachable,
        )
        queues = SimpleNamespace(find_stale_allocating_tickets=lambda: [])
        result = ComputeCleanup(provider, queues, manager).check_stale_allocations()
        return calls, result

    def test_idle_status_is_passed_to_the_release(self):
        calls, result = self._cleanup("idle")
        self.assertEqual(calls, [{"pod": "pod-a", "expected_status": "idle", "min_not_ready_seconds": 30.0}])
        self.assertEqual(result["released"], ["pod-a"])

    def test_ignored_release_is_not_counted(self):
        calls, result = self._cleanup("idle", outcome="ignored")
        self.assertEqual(result["released"], [])
        self.assertEqual(result["unready_released"], [])

    def test_unlabelled_pod_has_no_expectation(self):
        calls, result = self._cleanup("unknown")
        self.assertEqual(calls, [{"pod": "pod-a", "expected_status": None, "min_not_ready_seconds": 30.0}])


if __name__ == "__main__":
    unittest.main()
