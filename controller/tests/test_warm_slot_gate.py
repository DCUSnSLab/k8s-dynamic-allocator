"""Warm-slot admission contracts without a live Kubernetes API."""

import ast
import copy
import json
import logging
import shutil
import subprocess
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional

import yaml


class ApiException(Exception):
    def __init__(self, status):
        super().__init__(f"Kubernetes API status {status}")
        self.status = status


def load_provider_class():
    path = Path(__file__).resolve().parents[1] / "services/compute/warm_buffer_provider.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.ClassDef) and item.name == "WarmBufferProvider"
    )
    # Production Kubernetes imports require a configured cluster. Compile the
    # production class itself so its Pod decisions can be tested in isolation.
    namespace = {
        "KubernetesClient": object,
        "ApiException": ApiException,
        "Dict": Dict,
        "List": List,
        "Optional": Optional,
        "datetime": datetime,
        "timezone": timezone,
        "logger": logging.getLogger("test_warm_slot_gate"),
        "BUFFER_RESERVE_ANNOTATION": "k8s-dynamic-allocator/buffer-reserve",
        "BUFFER_CAPACITY_ANNOTATION": "k8s-dynamic-allocator/buffer-capacity",
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["WarmBufferProvider"]


WarmBufferProvider = load_provider_class()
MANIFEST = Path(__file__).resolve().parents[1] / "manifests/compute-general.yaml"
DEPLOY_SCRIPT = Path(__file__).resolve().parents[2] / "deploy/scripts/deploy.sh"


def make_pod(provider, name, *, gates=(), ready=False, assigned=False,
             owned=True, resource_version="7"):
    annotations = {
        provider.ANNOTATION_WARM_SLOT_RELEASED_AT: "",
        provider.ANNOTATION_POD_DELETION_COST: "-100",
        "example.org/keep": "unchanged",
    }
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            labels={
                provider.LABEL_APP: provider.APP_COMPUTE_POD,
                provider.LABEL_COMPUTE_TYPE: "general",
                provider.LABEL_STATUS: (
                    provider.STATUS_ASSIGNED if assigned else provider.STATUS_AVAILABLE
                ),
            },
            annotations=annotations,
            resource_version=resource_version,
            creation_timestamp=datetime(2026, 10, 10, tzinfo=timezone.utc),
            deletion_timestamp=None,
            owner_references=(
                [SimpleNamespace(kind="ReplicaSet", controller=True)] if owned else []
            ),
        ),
        spec=SimpleNamespace(
            scheduling_gates=[SimpleNamespace(name=value) for value in gates]
        ),
        status=SimpleNamespace(
            phase="Running" if ready else "Pending",
            pod_ip="10.0.0.1" if ready else None,
            conditions=[SimpleNamespace(type="Ready", status="True",
                                        last_transition_time=datetime(
                                            2026, 10, 10, tzinfo=timezone.utc
                                        ))] if ready else [],
        ),
    )


class InMemoryPodApi:
    def __init__(self, pod):
        self.pod = pod
        self.calls = []

    def call_api(self, path, method, **kwargs):
        assert method == "PATCH"
        assert kwargs["header_params"]["Content-Type"] == "application/json-patch+json"
        patch = kwargs["body"]
        self.calls.append(patch)
        updated = copy.deepcopy(self.pod)
        for operation in patch:
            try:
                segments = [
                    part.replace("~1", "/").replace("~0", "~")
                    for part in operation["path"].split("/")[1:]
                ]
                container = updated
                for segment in segments[:-1]:
                    container = container[int(segment)] if isinstance(container, list) else container[segment]
                last = segments[-1]
                key = int(last) if isinstance(container, list) else last
                actual = container[key]
            except (KeyError, IndexError, TypeError, ValueError) as exc:
                raise ApiException(422) from exc
            if operation["op"] == "test":
                if actual != operation["value"]:
                    raise ApiException(422)
            elif operation["op"] == "replace":
                container[key] = operation["value"]
            elif operation["op"] == "remove":
                if isinstance(container, list):
                    container.pop(key)
                else:
                    del container[key]
            else:
                raise AssertionError(f"Unexpected JSON Patch operation: {operation}")
        self.pod = updated


class WarmSlotGateTests(unittest.TestCase):
    def provider(self):
        instance = WarmBufferProvider.__new__(WarmBufferProvider)
        instance.namespace = "swlabpods"
        instance.api_request_timeout = (2, 5)
        instance._pod_not_ready_since = lambda pod: None
        return instance

    def test_snapshot_counts_pending_and_preserves_foreign_gate_budget(self):
        provider = self.provider()
        own = provider.SCHEDULING_GATE
        pods = [
            make_pod(provider, "ready", ready=True),
            make_pod(provider, "pending"),
            make_pod(provider, "gated", gates=[own]),
            make_pod(provider, "foreign-and-ours", gates=["example.org/hold", own]),
            make_pod(provider, "foreign-only", gates=["example.org/hold"]),
            make_pod(provider, "unowned-gated", gates=[own], owned=False),
            make_pod(provider, "assigned", assigned=True),
        ]
        provider.v1 = SimpleNamespace(list_namespaced_pod=lambda **kwargs: SimpleNamespace(items=pods))

        snapshot = provider.list_buffer_snapshot("general")
        self.assertEqual(snapshot["buffer_available"], 6)
        self.assertEqual(snapshot["buffer_assigned"], 1)
        self.assertEqual(snapshot["ungated_available"], 3)
        self.assertEqual(snapshot["ungated_not_ready"], 2)
        self.assertEqual(snapshot["schedulable_not_ready"], 1)
        self.assertEqual(
            snapshot["oldest_schedulable_not_ready_since"],
            datetime(2026, 10, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(
            snapshot["newest_schedulable_not_ready_since"],
            datetime(2026, 10, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(snapshot["unowned_available"], 1)
        self.assertEqual([pod["name"] for pod in snapshot["available_candidates"]], ["ready"])
        gates = {pod["name"]: pod for pod in snapshot["gated_available_candidates"]}
        self.assertEqual(set(gates), {"gated", "foreign-and-ours"})
        self.assertEqual(gates["foreign-and-ours"]["gate_index"], 1)
        self.assertEqual(gates["gated"]["resource_version"], "7")
        self.assertTrue(next(pod for pod in snapshot["pods"] if pod["name"] == "gated")["scheduling_gated"])

    def test_schedulable_not_ready_age_uses_gate_release_not_creation(self):
        provider = self.provider()
        old = make_pod(provider, "old")
        old.metadata.annotations[provider.ANNOTATION_WARM_SLOT_RELEASED_AT] = "2026-10-10T09:00:00+00:00"
        new = make_pod(provider, "new")
        new.metadata.annotations[provider.ANNOTATION_WARM_SLOT_RELEASED_AT] = "2026-10-10T09:00:20Z"
        foreign = make_pod(provider, "foreign", gates=["example.org/hold"])
        foreign.metadata.annotations[provider.ANNOTATION_WARM_SLOT_RELEASED_AT] = "2026-10-10T09:00:30Z"
        orphan = make_pod(provider, "orphan", owned=False)
        orphan.metadata.annotations[provider.ANNOTATION_WARM_SLOT_RELEASED_AT] = "2026-10-10T09:00:40Z"
        provider.v1 = SimpleNamespace(
            list_namespaced_pod=lambda **kwargs: SimpleNamespace(
                items=[old, new, foreign, orphan]
            )
        )

        snapshot = provider.list_buffer_snapshot("general")
        self.assertEqual(snapshot["schedulable_not_ready"], 2)
        self.assertEqual(
            snapshot["oldest_schedulable_not_ready_since"],
            datetime(2026, 10, 10, 9, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            snapshot["newest_schedulable_not_ready_since"],
            datetime(2026, 10, 10, 9, 0, 20, tzinfo=timezone.utc),
        )

    def test_gate_release_is_atomic_and_keeps_other_gates_and_annotations(self):
        provider = self.provider()
        api = InMemoryPodApi({
            "metadata": {
                "resourceVersion": "7",
                "labels": {"app": "compute-pod", "compute-status": "available"},
                "annotations": {
                    provider.ANNOTATION_WARM_SLOT_RELEASED_AT: "",
                    provider.ANNOTATION_POD_DELETION_COST: "-100",
                    "example.org/keep": "unchanged",
                },
            },
            "spec": {"schedulingGates": [
                {"name": "example.org/hold"},
                {"name": provider.SCHEDULING_GATE},
            ]},
        })
        provider.v1 = SimpleNamespace(api_client=api)

        self.assertTrue(provider.release_pod_scheduling_gate("pod-1", "7", 1))
        self.assertEqual(api.pod["spec"]["schedulingGates"], [{"name": "example.org/hold"}])
        annotations = api.pod["metadata"]["annotations"]
        self.assertEqual(annotations["example.org/keep"], "unchanged")
        self.assertEqual(annotations[provider.ANNOTATION_POD_DELETION_COST], "100")
        self.assertIsNotNone(datetime.fromisoformat(
            annotations[provider.ANNOTATION_WARM_SLOT_RELEASED_AT]
        ).tzinfo)

    def test_stale_resource_version_cannot_release_gate(self):
        provider = self.provider()
        api = InMemoryPodApi({
            "metadata": {
                "resourceVersion": "8",
                "labels": {"app": "compute-pod", "compute-status": "available"},
                "annotations": {
                    provider.ANNOTATION_WARM_SLOT_RELEASED_AT: "",
                    provider.ANNOTATION_POD_DELETION_COST: "-100",
                },
            },
            "spec": {"schedulingGates": [{"name": provider.SCHEDULING_GATE}]},
        })
        provider.v1 = SimpleNamespace(api_client=api)
        before = copy.deepcopy(api.pod)

        self.assertFalse(provider.release_pod_scheduling_gate("pod-1", "7", 0))
        self.assertEqual(api.pod, before)
        self.assertFalse(provider.release_pod_scheduling_gate("pod-1", "", 0))
        self.assertEqual(len(api.calls), 1)

    def test_manifest_rollout_and_deletion_cost_contract(self):
        provider = self.provider()
        manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(provider._validate_compute_manifest(manifest), "general")
        self.assertEqual(manifest["spec"]["strategy"]["rollingUpdate"], {
            "maxSurge": 0, "maxUnavailable": 1,
        })
        annotations = manifest["spec"]["template"]["metadata"]["annotations"]
        self.assertEqual(annotations[provider.ANNOTATION_POD_DELETION_COST], "-100")

        without_safe_strategy = copy.deepcopy(manifest)
        without_safe_strategy["spec"]["strategy"]["rollingUpdate"] = {
            "maxSurge": 1, "maxUnavailable": 0,
        }
        with self.assertRaisesRegex(ValueError, "maxSurge=0"):
            provider._validate_compute_manifest(without_safe_strategy)

        missing_gate = copy.deepcopy(manifest)
        del missing_gate["spec"]["template"]["spec"]["schedulingGates"]
        with self.assertRaisesRegex(ValueError, "warm-slot scheduling gate"):
            provider._validate_compute_manifest(missing_gate)

        missing_release_time = copy.deepcopy(manifest)
        del missing_release_time["spec"]["template"]["metadata"]["annotations"][
            provider.ANNOTATION_WARM_SLOT_RELEASED_AT
        ]
        with self.assertRaisesRegex(ValueError, "release time"):
            provider._validate_compute_manifest(missing_release_time)

        wrong_deletion_cost = copy.deepcopy(manifest)
        wrong_deletion_cost["spec"]["template"]["metadata"]["annotations"][
            provider.ANNOTATION_POD_DELETION_COST
        ] = "0"
        with self.assertRaisesRegex(ValueError, "deletion cost"):
            provider._validate_compute_manifest(wrong_deletion_cost)

    def test_missing_pod_placeholder_and_wrong_gate_index_fail_closed(self):
        provider = self.provider()
        pod = {
            "metadata": {
                "resourceVersion": "7",
                "labels": {"app": "compute-pod", "compute-status": "available"},
                "annotations": {provider.ANNOTATION_POD_DELETION_COST: "-100"},
            },
            "spec": {"schedulingGates": [
                {"name": "example.org/hold"},
                {"name": provider.SCHEDULING_GATE},
            ]},
        }
        api = InMemoryPodApi(pod)
        provider.v1 = SimpleNamespace(api_client=api)
        self.assertFalse(provider.release_pod_scheduling_gate("pod-1", "7", 1))
        self.assertEqual(api.pod, pod)

        pod["metadata"]["annotations"][provider.ANNOTATION_WARM_SLOT_RELEASED_AT] = ""
        api = InMemoryPodApi(pod)
        provider.v1 = SimpleNamespace(api_client=api)
        self.assertFalse(provider.release_pod_scheduling_gate("pod-1", "7", 0))
        self.assertEqual(api.pod, pod)

    def test_deploy_sync_patch_preserves_live_policy_and_replicas(self):
        """Parse the actual shell -p payload, including its manifest substitutions."""
        manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
        lines = DEPLOY_SCRIPT.read_text(encoding="utf-8").splitlines()
        argument = next(
            line.strip()[3:] for line in lines
            if line.strip().startswith('-p "') and "${compute_strategy}" in line
        )
        self.assertTrue(argument.startswith('"') and argument.endswith('"'))
        argument = argument[1:-1].replace('\\"', '"')
        values = {
            "compute_strategy": manifest["spec"]["strategy"],
            "compute_template_annotations": manifest["spec"]["template"]["metadata"]["annotations"],
            "compute_scheduling_gates": manifest["spec"]["template"]["spec"]["schedulingGates"],
            "compute_node_selector": manifest["spec"]["template"]["spec"]["nodeSelector"],
            "compute_resources": manifest["spec"]["template"]["spec"]["containers"][0]["resources"],
        }
        for name, value in values.items():
            argument = argument.replace(
                "${" + name + "}", json.dumps(value, separators=(",", ":"))
            )
        argument = argument.replace("${COMPUTE_POD_IMAGE}", "example.org/compute:test")
        patch = json.loads(argument)

        self.assertEqual(set(patch), {"spec"})
        self.assertNotIn("replicas", patch["spec"])
        self.assertEqual(patch["spec"]["strategy"], manifest["spec"]["strategy"])
        self.assertEqual(
            patch["spec"]["template"]["spec"]["schedulingGates"],
            [{"name": WarmBufferProvider.SCHEDULING_GATE}],
        )
        self.assertEqual(
            patch["spec"]["template"]["metadata"]["annotations"],
            manifest["spec"]["template"]["metadata"]["annotations"],
        )
        self.assertNotIn("k8s-dynamic-allocator/buffer-reserve", argument)
        self.assertNotIn("k8s-dynamic-allocator/buffer-capacity", argument)


if __name__ == "__main__":
    unittest.main()
