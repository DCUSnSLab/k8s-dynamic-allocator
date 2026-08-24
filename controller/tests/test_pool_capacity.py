import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
CONTROLLER_ROOT = REPOSITORY_ROOT / "controller"
REST_API_ROOT = CONTROLLER_ROOT / "rest_api"
for import_root in (str(CONTROLLER_ROOT), str(REST_API_ROOT)):
    if import_root not in sys.path:
        sys.path.insert(0, import_root)


from services.compute.allocator import ComputeAllocator
from services.compute.cleanup import ComputeCleanup
from services.compute.pool_capacity_reconciler import PoolCapacityReconciler
from services.compute.queue_processor import ComputeQueueProcessor
from services.compute.warm_pod_pool import WarmPodPool
from services.compute import pool_capacity_reconciler as reconciler_module


class PoolManifestTests(unittest.TestCase):
    def test_compute_manifest_and_scale_rbac_match_r2_n5_policy(self):
        compute_manifest = yaml.safe_load(
            (CONTROLLER_ROOT / "manifests" / "compute-general.yaml").read_text()
        )
        metadata = compute_manifest["metadata"]

        self.assertEqual(metadata["labels"]["app"], "warm-pod-pool")
        self.assertEqual(metadata["labels"]["compute-type"], "general")
        self.assertEqual(
            metadata["annotations"][
                "k8s-dynamic-allocator/pool-available-min"
            ],
            "2",
        )
        self.assertEqual(
            metadata["annotations"][
                "k8s-dynamic-allocator/pool-total-max"
            ],
            "5",
        )

        role_documents = list(
            yaml.safe_load_all(
                (REPOSITORY_ROOT / "deploy" / "controller_role.yaml").read_text()
            )
        )
        role = next(
            document
            for document in role_documents
            if document.get("kind") == "Role"
        )
        scale_rules = [
            rule
            for rule in role["rules"]
            if "deployments/scale" in rule.get("resources", [])
        ]
        self.assertEqual(len(scale_rules), 1)
        self.assertTrue({"get", "patch", "update"} <= set(scale_rules[0]["verbs"]))


def _deployment(annotations):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="compute-general",
            labels={
                WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                WarmPodPool.LABEL_COMPUTE_TYPE: "general",
            },
            annotations=annotations,
            resource_version="42",
        ),
        spec=SimpleNamespace(
            selector=SimpleNamespace(
                match_labels={
                    WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                    WarmPodPool.LABEL_COMPUTE_TYPE: "general",
                    WarmPodPool.LABEL_STATUS: WarmPodPool.STATUS_AVAILABLE,
                }
            ),
            template=SimpleNamespace(
                metadata=SimpleNamespace(
                    labels={
                        WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                        WarmPodPool.LABEL_COMPUTE_TYPE: "general",
                        WarmPodPool.LABEL_STATUS: WarmPodPool.STATUS_AVAILABLE,
                        WarmPodPool.LABEL_USER: "",
                    }
                )
            ),
        ),
    )


def _pool_manifest():
    return {
        "metadata": {
            "name": "compute-general",
            "labels": {
                WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                WarmPodPool.LABEL_COMPUTE_TYPE: "general",
            },
            "annotations": {
                WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "2",
                WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
            },
        }
    }


def _legacy_deployment(*, selector_compute_type="general"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name="compute-general",
            labels={},
            annotations={},
        ),
        spec=SimpleNamespace(
            selector=SimpleNamespace(
                match_labels={
                    WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                    WarmPodPool.LABEL_COMPUTE_TYPE: selector_compute_type,
                    WarmPodPool.LABEL_STATUS: WarmPodPool.STATUS_AVAILABLE,
                }
            ),
            template=SimpleNamespace(
                metadata=SimpleNamespace(
                    labels={
                        WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                        WarmPodPool.LABEL_COMPUTE_TYPE: "general",
                        WarmPodPool.LABEL_STATUS: WarmPodPool.STATUS_AVAILABLE,
                        WarmPodPool.LABEL_USER: "",
                    }
                )
            ),
        ),
    )


def _condition(ready, transition_time=None):
    return SimpleNamespace(
        type="Ready",
        status="True" if ready else "False",
        last_transition_time=transition_time,
    )


def _pod(
    name,
    pool_status,
    *,
    phase="Running",
    ready=False,
    ip=None,
    deleting=False,
    replicaset_owned=False,
):
    owner_references = []
    if replicaset_owned:
        owner_references.append(
            SimpleNamespace(kind="ReplicaSet", controller=True)
        )
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            labels={
                WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                WarmPodPool.LABEL_COMPUTE_TYPE: "general",
                WarmPodPool.LABEL_STATUS: pool_status,
                WarmPodPool.LABEL_USER: "",
            },
            deletion_timestamp="now" if deleting else None,
            owner_references=owner_references,
            creation_timestamp=f"created-{name}",
            resource_version=f"rv-{name}",
        ),
        status=SimpleNamespace(
            phase=phase,
            pod_ip=ip,
            conditions=[_condition(ready, f"ready-{name}")],
        ),
    )


class _PodListApi:
    def __init__(self, pods):
        self.pods = pods
        self.calls = []

    def list_namespaced_pod(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(items=list(self.pods))


class WarmPodPoolPolicyTests(unittest.TestCase):
    def setUp(self):
        self.pool = object.__new__(WarmPodPool)

    def test_valid_annotations_are_parsed(self):
        policy = self.pool.parse_deployment_policy(
            _deployment(
                {
                    WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "2",
                    WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
                }
            )
        )

        self.assertEqual(
            policy,
            {
                "compute_type": "general",
                "deployment_name": "compute-general",
                "R": 2,
                "N": 5,
                "resource_version": "42",
            },
        )

    def test_missing_annotation_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "annotations are required"):
            self.pool.parse_deployment_policy(
                _deployment(
                    {
                        WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "2",
                    }
                )
            )

    def test_non_integer_annotation_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "must be integers"):
            self.pool.parse_deployment_policy(
                _deployment(
                    {
                        WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "abc",
                        WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
                    }
                )
            )

    def test_negative_annotation_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "0 <= R <= N"):
            self.pool.parse_deployment_policy(
                _deployment(
                    {
                        WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "-1",
                        WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
                    }
                )
            )

    def test_r_greater_than_n_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "0 <= R <= N"):
            self.pool.parse_deployment_policy(
                _deployment(
                    {
                        WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "6",
                        WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
                    }
                )
            )

    def test_legacy_metadata_migrates_only_when_live_identity_matches(self):
        self.pool.namespace = "test"
        self.pool.api_request_timeout = (2.0, 5.0)
        patch_deployment = MagicMock()
        self.pool.apps_v1 = SimpleNamespace(
            patch_namespaced_deployment=patch_deployment
        )

        migrated = self.pool._migrate_legacy_deployment_metadata(
            _legacy_deployment(),
            _pool_manifest(),
        )

        self.assertTrue(migrated)
        patch_deployment.assert_called_once()
        body = patch_deployment.call_args.kwargs["body"]
        self.assertEqual(
            body["metadata"]["labels"],
            {
                WarmPodPool.LABEL_APP: WarmPodPool.APP_WARM_POOL,
                WarmPodPool.LABEL_COMPUTE_TYPE: "general",
            },
        )
        self.assertEqual(
            body["metadata"]["annotations"],
            {
                WarmPodPool.ANNOTATION_POOL_AVAILABLE_MIN: "2",
                WarmPodPool.ANNOTATION_POOL_TOTAL_MAX: "5",
            },
        )

        patch_deployment.reset_mock()
        with patch(
            "services.compute.warm_pod_pool.logger.error"
        ):
            migrated = self.pool._migrate_legacy_deployment_metadata(
                _legacy_deployment(selector_compute_type="other"),
                _pool_manifest(),
            )

        self.assertFalse(migrated)
        patch_deployment.assert_not_called()


class WarmPodPoolReservationTests(unittest.TestCase):
    def test_assign_patch_contains_resource_version_and_reservation_journal(self):
        pool = object.__new__(WarmPodPool)
        pool.namespace = "test"
        pool.api_request_timeout = (2.0, 5.0)
        call_api = MagicMock()
        pool.v1 = SimpleNamespace(
            api_client=SimpleNamespace(call_api=call_api)
        )

        pool.assign_pod(
            "compute-1",
            "user-1",
            expected_resource_version="rv-1",
            ticket_id="ticket-1",
            claim_token="claim-1",
            expected_annotations={"existing": "kept"},
        )

        call_api.assert_called_once()
        body = call_api.call_args.kwargs["body"]
        self.assertIn(
            {
                "op": "test",
                "path": "/metadata/resourceVersion",
                "value": "rv-1",
            },
            body,
        )
        self.assertIn(
            {
                "op": "add",
                "path": "/metadata/annotations",
                "value": {
                    "existing": "kept",
                    WarmPodPool.ANNOTATION_ALLOCATION_TICKET: "ticket-1",
                    WarmPodPool.ANNOTATION_ALLOCATION_CLAIM: "claim-1",
                },
            },
            body,
        )
        self.assertIn(
            {
                "op": "replace",
                "path": "/metadata/labels/pool-status",
                "value": WarmPodPool.STATUS_ASSIGNED,
            },
            body,
        )


class WarmPodPoolSnapshotTests(unittest.TestCase):
    def test_snapshot_counts_pending_and_excludes_terminating(self):
        pool = object.__new__(WarmPodPool)
        pool.namespace = "test"
        pool.api_request_timeout = (2.0, 5.0)
        pool.v1 = _PodListApi(
            [
                _pod(
                    "pending",
                    WarmPodPool.STATUS_AVAILABLE,
                    phase="Pending",
                ),
                _pod(
                    "ready",
                    WarmPodPool.STATUS_AVAILABLE,
                    ready=True,
                    ip="10.0.0.2",
                ),
                _pod(
                    "assigned",
                    WarmPodPool.STATUS_ASSIGNED,
                    ready=True,
                    ip="10.0.0.3",
                    replicaset_owned=True,
                ),
                _pod(
                    "terminating",
                    WarmPodPool.STATUS_AVAILABLE,
                    ready=True,
                    ip="10.0.0.4",
                    deleting=True,
                ),
            ]
        )

        snapshot = pool.list_pool_snapshot("general")

        self.assertEqual(snapshot["physical_total"], 4)
        self.assertEqual(snapshot["pool_total"], 3)
        self.assertEqual(snapshot["pool_available"], 2)
        self.assertEqual(snapshot["pool_assigned"], 1)
        self.assertEqual(snapshot["terminating"], 1)
        self.assertEqual(snapshot["ready_available"], 1)
        self.assertEqual(snapshot["assigned_with_replicaset_owner"], 1)
        self.assertEqual(
            snapshot["available_candidates"],
            [
                {
                    "name": "ready",
                    "ip": "10.0.0.2",
                    "ready_at": "ready-ready",
                    "creation_timestamp": "created-ready",
                    "resource_version": "rv-ready",
                    "annotations": {},
                }
            ],
        )
        self.assertEqual(len(pool.v1.calls), 1)


class _AllocatorQueues:
    worker_identity = "worker-1"

    def __init__(
        self,
        *,
        gated=False,
        policy=None,
        policy_ready=True,
        tickets=None,
    ):
        self.gated = gated
        self.policy = policy
        self.policy_ready = policy_ready
        self.pending = list(tickets or [])
        self.claim_calls = 0
        self.lock_calls = 0
        self.release_calls = 0
        self.policy_ready_calls = 0

    @staticmethod
    def normalize_compute_type(value):
        return (value or "general").strip().lower()

    def is_scale_down_gated(self, compute_type):
        return self.gated

    def is_pool_policy_ready(self):
        self.policy_ready_calls += 1
        return self.policy_ready

    def acquire_allocator_lock(self, compute_type):
        self.lock_calls += 1
        return "lock-token"

    def renew_allocator_lock(self, compute_type, token):
        return True

    def release_allocator_lock(self, compute_type, token):
        self.release_calls += 1
        return True

    def find_stale_allocating_tickets(self, compute_type):
        return []

    def has_queued_tickets(self, compute_type):
        return bool(self.pending)

    def get_pool_policy(self, compute_type):
        return self.policy

    def claim_next_ticket(self, compute_type, worker_id):
        self.claim_calls += 1
        if not self.pending:
            return None
        return self.pending.pop(0)

    def pop_compute_available_at(self, compute_pod):
        return "available-at"

    def mark_compute_unavailable_started(self, compute_type):
        return None


class _AllocatorPool:
    def __init__(self, *, assigned=0, candidates=None):
        self.assigned = assigned
        self.candidates = list(candidates or [])
        self.snapshot_calls = 0
        self.assigned_pods = []
        self.released_pods = []

    def list_pool_snapshot(self, compute_type=None):
        self.snapshot_calls += 1
        return {
            "pool_assigned": self.assigned,
            "available_candidates": list(self.candidates),
        }

    def assign_pod(
        self,
        pod_name,
        user_pod,
        expected_resource_version=None,
        ticket_id="",
        claim_token="",
        expected_annotations=None,
    ):
        self.assigned_pods.append((pod_name, user_pod))

    def get_pod_ip(self, pod_name):
        raise AssertionError("allocator must reuse the snapshot IP")

    def get_pod_ready_at(self, pod_name):
        raise AssertionError("allocator must reuse the snapshot ready_at")

    def release_pod(self, pod_name):
        self.released_pods.append(pod_name)
        return True


class _AllocatorTickets:
    def __init__(self):
        self.marked = []
        self.requeued = []

    def mark_allocating(self, ticket_id, **fields):
        committed = {
            "ticket_id": ticket_id,
            "status": "allocating",
            **fields,
        }
        self.marked.append(committed)
        return committed

    def requeue_ticket(self, ticket_id, **fields):
        self.requeued.append((ticket_id, fields))
        return {"ticket_id": ticket_id, "status": "queued"}


def _ticket(number):
    return {
        "ticket_id": f"ticket-{number}",
        "claim_token": f"claim-{number}",
        "claimed_by": "worker-1",
        "user_pod": f"user-{number}",
        "compute_type": "general",
    }


def _candidate(number):
    return {
        "name": f"compute-{number}",
        "ip": f"10.0.0.{number}",
        "ready_at": f"ready-{number}",
    }


class ComputeAllocatorCapacityTests(unittest.TestCase):
    def _drain(self, pool, queues, tickets):
        allocator = ComputeAllocator(pool, queues, tickets)
        with (
            patch.object(
                allocator,
                "_compute_wait_queue_batch_plan",
                return_value=(10, 1),
            ),
            patch.object(
                allocator,
                "_safe_execute_allocated_ticket",
                return_value={"status": "assigned"},
            ),
        ):
            result = allocator.drain_wait_queue_for_type(
                "general",
                recover_stale_ticket=lambda ticket: {"status": "requeued"},
            )
        return result

    def test_scale_down_gate_blocks_before_lock_and_claim(self):
        pool = _AllocatorPool(assigned=0, candidates=[_candidate(1)])
        queues = _AllocatorQueues(
            gated=True,
            policy={"R": 2, "N": 5},
            tickets=[_ticket(1)],
        )

        result = self._drain(pool, queues, _AllocatorTickets())

        self.assertEqual(result["capacity_blocked"], "scale_down")
        self.assertEqual(queues.lock_calls, 0)
        self.assertEqual(queues.claim_calls, 0)
        self.assertEqual(pool.snapshot_calls, 0)

    def test_policy_not_ready_blocks_before_lock_claim_and_kubernetes_list(self):
        pool = _AllocatorPool(assigned=0, candidates=[_candidate(1)])
        queues = _AllocatorQueues(
            policy_ready=False,
            policy={"R": 2, "N": 5},
            tickets=[_ticket(1)],
        )

        result = self._drain(pool, queues, _AllocatorTickets())

        self.assertEqual(result["capacity_blocked"], "policy_not_ready")
        self.assertEqual(queues.lock_calls, 0)
        self.assertEqual(queues.claim_calls, 0)
        self.assertEqual(pool.snapshot_calls, 0)

    def test_policy_ready_gate_does_not_change_cold_start_claim_path(self):
        pool = SimpleNamespace(allocation_mode="cold_start")
        queues = _AllocatorQueues(
            policy_ready=False,
            tickets=[_ticket(1)],
        )
        allocator = ComputeAllocator(pool, queues, _AllocatorTickets())

        with (
            patch.object(
                allocator,
                "_compute_wait_queue_batch_plan",
                return_value=(1, 1),
            ),
            patch(
                "services.compute.allocator.threading.Thread",
                _NoopThread,
            ),
        ):
            result = allocator.drain_wait_queue_for_type(
                "general",
                recover_stale_ticket=lambda ticket: {"status": "requeued"},
            )

        self.assertEqual(result["claimed"], 1)
        self.assertEqual(queues.claim_calls, 1)
        self.assertEqual(queues.policy_ready_calls, 0)

    def test_missing_policy_leaves_ticket_unclaimed(self):
        pool = _AllocatorPool(assigned=0, candidates=[_candidate(1)])
        queues = _AllocatorQueues(policy=None, tickets=[_ticket(1)])

        result = self._drain(pool, queues, _AllocatorTickets())

        self.assertEqual(result["capacity_blocked"], "policy_unavailable")
        self.assertEqual(queues.claim_calls, 0)
        self.assertEqual(pool.snapshot_calls, 0)
        self.assertEqual(queues.release_calls, 1)

    def test_a_four_n_five_claims_only_one_and_reuses_snapshot_ip(self):
        pool = _AllocatorPool(
            assigned=4,
            candidates=[_candidate(1), _candidate(2)],
        )
        queues = _AllocatorQueues(
            policy={"R": 2, "N": 5},
            tickets=[_ticket(1), _ticket(2)],
        )
        tickets = _AllocatorTickets()

        result = self._drain(pool, queues, tickets)

        self.assertEqual(result["claimed"], 1)
        self.assertEqual(result["assigned"], 1)
        self.assertEqual(queues.claim_calls, 1)
        self.assertEqual(pool.assigned_pods, [("compute-1", "user-1")])
        self.assertEqual(tickets.marked[0]["compute_pod_ip"], "10.0.0.1")
        self.assertEqual(tickets.marked[0]["compute_ready_at"], "ready-1")
        self.assertEqual(pool.snapshot_calls, 1)

    def test_a_equal_n_claims_nothing(self):
        pool = _AllocatorPool(assigned=5, candidates=[_candidate(1)])
        queues = _AllocatorQueues(
            policy={"R": 2, "N": 5},
            tickets=[_ticket(1)],
        )

        result = self._drain(pool, queues, _AllocatorTickets())

        self.assertEqual(result["capacity_blocked"], "pool_total_max")
        self.assertEqual(result["claimed"], 0)
        self.assertEqual(queues.claim_calls, 0)
        self.assertEqual(pool.assigned_pods, [])

    def test_mark_allocating_exception_deletes_flipped_pod_and_requeues(self):
        class _FailingMarkTickets(_AllocatorTickets):
            def mark_allocating(self, ticket_id, **fields):
                raise RuntimeError("redis write failed")

        pool = _AllocatorPool(candidates=[_candidate(1)])
        queues = _AllocatorQueues(policy={"R": 2, "N": 5})
        tickets = _FailingMarkTickets()
        allocator = ComputeAllocator(pool, queues, tickets)
        result = {"queued": 0, "errors": []}

        reservation = allocator._reserve_compute_pod_for_ticket(
            ticket=_ticket(1),
            compute_type_value="general",
            candidate_pods=[_candidate(1)],
            reserved_pods=set(),
            result=result,
        )

        self.assertIsNone(reservation)
        self.assertEqual(pool.assigned_pods, [("compute-1", "user-1")])
        self.assertEqual(pool.released_pods, ["compute-1"])
        self.assertEqual(
            tickets.requeued,
            [
                (
                    "ticket-1",
                    {
                        "reason": "Compute reservation commit failed",
                        "increment_retry": False,
                        "claim_token": "claim-1",
                    },
                )
            ],
        )
        self.assertEqual(result["queued"], 1)
        self.assertEqual(result["errors"], [])

    def test_commit_failure_does_not_requeue_until_pod_delete_is_accepted(self):
        class _FailingMarkTickets(_AllocatorTickets):
            def mark_allocating(self, ticket_id, **fields):
                raise RuntimeError("redis write failed")

        class _FailingDeletePool(_AllocatorPool):
            def release_pod(self, pod_name):
                raise RuntimeError("Kubernetes delete failed")

        pool = _FailingDeletePool(candidates=[_candidate(1)])
        tickets = _FailingMarkTickets()
        allocator = ComputeAllocator(
            pool,
            _AllocatorQueues(policy={"R": 2, "N": 5}),
            tickets,
        )
        result = {"queued": 0, "errors": []}

        reservation = allocator._reserve_compute_pod_for_ticket(
            ticket=_ticket(1),
            compute_type_value="general",
            candidate_pods=[_candidate(1)],
            reserved_pods=set(),
            result=result,
        )

        self.assertIsNone(reservation)
        self.assertEqual(tickets.requeued, [])
        self.assertEqual(result["queued"], 0)
        self.assertIn("ticket remains allocating", result["errors"][0]["error"])


class ComputeQueueProcessorRecoveryTests(unittest.TestCase):
    def test_stale_ticket_without_compute_pod_uses_reservation_journal(self):
        find_calls = []
        released = []

        class _Queues:
            max_retries = 3

            @staticmethod
            def normalize_compute_type(value):
                return (value or "general").strip().lower()

        class _Pool:
            def find_reserved_pod(self, **kwargs):
                find_calls.append(kwargs)
                return "compute-journaled"

        class _Tickets:
            def get_ticket(self, ticket_id):
                return {
                    "ticket_id": ticket_id,
                    "status": "allocating",
                    "compute_type": "general",
                    "compute_pod": "",
                    "claim_token": "claim-1",
                    "retry_count": 0,
                    "max_retries": 3,
                }

            def requeue_ticket(self, ticket_id, **fields):
                return {
                    "ticket_id": ticket_id,
                    "status": "queued",
                    "retry_count": 1,
                    **fields,
                }

        processor = ComputeQueueProcessor(
            _Pool(),
            _Queues(),
            _Tickets(),
            allocator=object(),
            release_pod_best_effort=lambda compute_pod, ticket_id: released.append(
                (compute_pod, ticket_id)
            ),
        )

        result = processor.recover_stale_ticket(
            {
                "ticket_id": "ticket-1",
                "compute_type": "general",
                "compute_pod": "",
                "claim_token": "claim-1",
                "retry_count": 0,
                "max_retries": 3,
            }
        )

        self.assertEqual(result["status"], "requeued")
        self.assertEqual(
            find_calls,
            [
                {
                    "ticket_id": "ticket-1",
                    "claim_token": "claim-1",
                    "compute_type": "general",
                }
            ],
        )
        self.assertEqual(released, [("compute-journaled", "ticket-1")])


class ComputeCleanupJournalTests(unittest.TestCase):
    def test_journal_orphan_delete_is_retried_after_transient_api_failure(self):
        class _Pool:
            def __init__(self):
                self.release_attempts = 0

            def list_pool_status(self):
                return [
                    {
                        "name": "compute-old",
                        "pool_status": "assigned",
                        "terminating": False,
                        "allocation_ticket_id": "ticket-1",
                        "allocation_claim_token": "claim-old",
                    }
                ]

            def release_pod(self, pod_name):
                self.release_attempts += 1
                if self.release_attempts == 1:
                    raise RuntimeError("temporary API failure")
                return True

        tickets = SimpleNamespace(
            get_ticket=lambda ticket_id: {
                "ticket_id": ticket_id,
                "status": "queued",
                "claim_token": "",
                "compute_pod": "",
            }
        )
        pool = _Pool()
        cleanup = ComputeCleanup(
            pool,
            queues=object(),
            compute_manager=SimpleNamespace(tickets=tickets),
        )

        first = cleanup.recover_journaled_orphans()
        second = cleanup.recover_journaled_orphans()

        self.assertEqual(first["released"], [])
        self.assertEqual(first["errors"][0]["pod"], "compute-old")
        self.assertEqual(second["released"], ["compute-old"])
        self.assertEqual(pool.release_attempts, 2)

    def test_matching_inflight_journal_is_not_deleted(self):
        pool = SimpleNamespace(
            list_pool_status=lambda: [
                {
                    "name": "compute-current",
                    "pool_status": "assigned",
                    "terminating": False,
                    "allocation_ticket_id": "ticket-1",
                    "allocation_claim_token": "claim-1",
                }
            ],
            release_pod=MagicMock(),
        )
        tickets = SimpleNamespace(
            get_ticket=lambda ticket_id: {
                "ticket_id": ticket_id,
                "status": "allocating",
                "claim_token": "claim-1",
                "compute_pod": "",
            }
        )
        cleanup = ComputeCleanup(
            pool,
            queues=object(),
            compute_manager=SimpleNamespace(tickets=tickets),
        )

        result = cleanup.recover_journaled_orphans()

        self.assertEqual(result["released"], [])
        pool.release_pod.assert_not_called()


class _ReconcilerQueues:
    def __init__(self, policy, events=None):
        self.policy = policy
        self.events = events if events is not None else []
        self.lock_number = 0

    @staticmethod
    def normalize_compute_type(value):
        return (value or "general").strip().lower()

    def get_pool_policy(self, compute_type):
        return self.policy

    def acquire_scale_down_gate(self, compute_type):
        self.events.append("gate:acquire")
        return "gate-token"

    def renew_scale_down_gate(self, compute_type, token):
        self.events.append("gate:renew")
        return True

    def release_scale_down_gate(self, compute_type, token):
        self.events.append("gate:release")
        return True

    def acquire_allocator_lock(self, compute_type):
        self.lock_number += 1
        token = f"lock-{self.lock_number}"
        self.events.append(f"lock:acquire:{token}")
        return token

    def release_allocator_lock(self, compute_type, token):
        self.events.append(f"lock:release:{token}")
        return True

    def clear_pool_policy_ready(self, token=None):
        self.events.append(f"policy-ready:clear:{token or ''}")
        return True


class _ReconcilerPool:
    def __init__(
        self,
        *,
        snapshots,
        current_replicas,
        events=None,
        fail_patch=False,
    ):
        self.snapshots = list(snapshots)
        self.current_replicas = current_replicas
        self.events = events if events is not None else []
        self.fail_patch = fail_patch
        self.patch_calls = []
        self.snapshot_index = 0

    def list_pool_snapshot(self, compute_type):
        index = self.snapshot_index
        self.snapshot_index += 1
        if self.snapshots:
            snapshot = self.snapshots.pop(0)
            self._last_snapshot = snapshot
        else:
            snapshot = self._last_snapshot
        self.events.append(
            "snapshot:"
            f"{index}:owner={snapshot['assigned_with_replicaset_owner']}:"
            f"available={snapshot['pool_available']}"
        )
        return dict(snapshot)

    def read_deployment_replicas(self, deployment_name):
        self.events.append(f"scale:read:{self.current_replicas}")
        return self.current_replicas

    def patch_deployment_replicas(self, deployment_name, replicas):
        self.events.append(f"scale:patch:{replicas}")
        self.patch_calls.append((deployment_name, replicas))
        if self.fail_patch:
            raise RuntimeError("patch failed")
        self.current_replicas = replicas
        return replicas


def _snapshot(*, assigned, available, owners=0):
    return {
        "pool_total": assigned + available,
        "pool_available": available,
        "pool_assigned": assigned,
        "assigned_with_replicaset_owner": owners,
    }


class _NoSleepStopEvent:
    def is_set(self):
        return False

    def wait(self, timeout=None):
        return False


class _NoopThread:
    def __init__(self, *args, **kwargs):
        self.name = kwargs.get("name", "")

    def start(self):
        return None

    def join(self, timeout=None):
        return None


class PoolCapacityReconcilerTests(unittest.TestCase):
    POLICY = {
        "deployment_name": "compute-general",
        "R": 2,
        "N": 5,
    }

    def _reconciler(self, pool, queues, callback=None):
        reconciler = PoolCapacityReconciler(
            pool,
            queues,
            on_capacity_available=callback,
        )
        reconciler._stop_event = _NoSleepStopEvent()
        return reconciler

    def test_desired_replicas_formula(self):
        desired = PoolCapacityReconciler.desired_replicas

        self.assertEqual(desired(2, 5, 0), 2)
        self.assertEqual(desired(2, 5, 4), 1)
        self.assertEqual(desired(2, 5, 5), 0)
        self.assertEqual(desired(2, 5, 7), 0)

    def test_scale_up_then_equal_value_is_noop(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[
                _snapshot(assigned=0, available=1),
                _snapshot(assigned=0, available=2),
            ],
            current_replicas=1,
            events=events,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(pool, queues)

        scaled = reconciler.reconcile_type("general")
        converged = reconciler.reconcile_type("general")

        self.assertEqual(scaled["status"], "scaled_up")
        self.assertEqual(converged["status"], "converged")
        self.assertEqual(pool.patch_calls, [("compute-general", 2)])

    def test_invalid_leadership_blocks_before_snapshot_or_scale_patch(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[],
            current_replicas=1,
            events=events,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(pool, queues)
        reconciler.set_leadership_validator(lambda: False)

        result = reconciler.reconcile_type("general")

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "leadership_not_valid")
        self.assertEqual(pool.snapshot_index, 0)
        self.assertEqual(pool.patch_calls, [])
        self.assertEqual(events, [])

    def test_scale_down_gates_waits_for_orphan_and_patches_under_lock(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[
                _snapshot(assigned=4, available=2, owners=1),
                _snapshot(assigned=4, available=2, owners=1),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=1, owners=0),
            ],
            current_replicas=2,
            events=events,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(
            pool,
            queues,
            callback=lambda compute_type: events.append(
                f"queue:kick:{compute_type}"
            ),
        )

        with patch.object(reconciler_module.threading, "Thread", _NoopThread):
            result = reconciler.reconcile_type("general")

        self.assertEqual(result["status"], "scaled_down")
        self.assertTrue(result["deletion_observed"])
        self.assertEqual(pool.patch_calls, [("compute-general", 1)])
        self.assertLess(events.index("gate:acquire"), events.index("lock:acquire:lock-1"))
        self.assertLess(
            events.index("snapshot:2:owner=0:available=2"),
            events.index("scale:patch:1"),
        )
        self.assertLess(
            events.index("lock:acquire:lock-2"),
            events.index("scale:patch:1"),
        )
        self.assertLess(
            events.index("scale:patch:1"),
            events.index("lock:release:lock-2"),
        )
        self.assertLess(events.index("scale:patch:1"), events.index("gate:release"))
        self.assertEqual(events[-2:], ["gate:release", "queue:kick:general"])

    def test_existing_scale_down_keeps_gate_until_available_count_settles(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=1, owners=0),
            ],
            current_replicas=1,
            events=events,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(pool, queues)

        with patch.object(reconciler_module.threading, "Thread", _NoopThread):
            result = reconciler.reconcile_type("general")

        self.assertEqual(result["status"], "converged")
        self.assertTrue(result["deletion_observed"])
        self.assertEqual(pool.patch_calls, [])
        self.assertIn("snapshot:3:owner=0:available=1", events)
        self.assertLess(
            events.index("snapshot:3:owner=0:available=1"),
            events.index("gate:release"),
        )

    def test_scale_down_observation_timeout_closes_global_admission(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
            ],
            current_replicas=2,
            events=events,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(
            pool,
            queues,
            callback=lambda compute_type: events.append(
                f"queue:kick:{compute_type}"
            ),
        )
        reconciler._policy_ready = True
        reconciler._policy_ready_token = "leader-token"

        with (
            patch.object(reconciler_module.threading, "Thread", _NoopThread),
            patch.object(
                reconciler,
                "_wait_for_scale_down_observation",
                return_value=False,
            ),
        ):
            result = reconciler.reconcile_type("general")

        self.assertEqual(result["status"], "deferred")
        self.assertTrue(result["retry"])
        self.assertIn("policy-ready:clear:leader-token", events)
        self.assertIn("gate:release", events)
        self.assertNotIn("queue:kick:general", events)

    def test_scale_down_releases_gate_when_patch_fails(self):
        events = []
        pool = _ReconcilerPool(
            snapshots=[
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
                _snapshot(assigned=4, available=2, owners=0),
            ],
            current_replicas=2,
            events=events,
            fail_patch=True,
        )
        queues = _ReconcilerQueues(self.POLICY, events)
        reconciler = self._reconciler(pool, queues)

        with (
            patch.object(reconciler_module.threading, "Thread", _NoopThread),
            patch.object(reconciler_module.logger, "exception"),
        ):
            result = reconciler.reconcile_type("general")

        self.assertEqual(result["status"], "error")
        self.assertTrue(result["retry"])
        self.assertIn("scale:patch:1", events)
        self.assertIn("lock:release:lock-2", events)
        self.assertEqual(events[-1], "gate:release")


if __name__ == "__main__":
    unittest.main()
