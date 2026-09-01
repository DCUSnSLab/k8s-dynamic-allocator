import time
from typing import Dict, Optional

from . import ticket_format


class ControllerStatus:
    def __init__(self, pool, queues, tickets, capacity_reconciler=None):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets
        self.capacity_reconciler = capacity_reconciler

    def get_pool_status(self) -> Dict:
        pool_list = self.pool.list_pool_status()

        available_count = sum(
            1
            for pod in pool_list
            if pod["pool_status"] == "available"
            and not pod.get("terminating", False)
        )
        assigned_count = sum(
            1
            for pod in pool_list
            if pod["pool_status"] == "assigned"
            and not pod.get("terminating", False)
        )
        terminating_count = sum(
            1 for pod in pool_list if pod.get("terminating", False)
        )
        logical_total = available_count + assigned_count

        response = {
            "total": logical_total,
            "available": available_count,
            "assigned": assigned_count,
            "pods": pool_list,
        }

        if not hasattr(self.pool, "list_pool_deployments"):
            return response

        response["physical_total"] = len(pool_list)
        response["terminating"] = terminating_count
        try:
            response["policy_ready"] = self.queues.is_pool_policy_ready()
        except Exception as exc:
            response["policy_ready"] = False
            response["policy_ready_error"] = str(exc)
        response["pools"] = self._pool_policy_status(pool_list)
        if self.capacity_reconciler is not None:
            response["leader_control"] = self.capacity_reconciler.get_status()
        return response

    def _pool_policy_status(self, pool_list) -> Dict:
        deployments_by_type = {}
        deployment_list_error = ""
        try:
            for deployment in self.pool.list_pool_deployments():
                metadata = getattr(deployment, "metadata", None)
                labels = getattr(metadata, "labels", None) or {}
                compute_type = self.queues.normalize_compute_type(
                    labels.get(self.pool.LABEL_COMPUTE_TYPE)
                )
                deployments_by_type.setdefault(compute_type, []).append(deployment)
        except Exception as exc:
            deployment_list_error = str(exc)

        compute_types = {
            self.queues.normalize_compute_type(pod.get("compute_type"))
            for pod in pool_list
            if pod.get("compute_type") and pod.get("compute_type") != "unknown"
        }
        compute_types.update(deployments_by_type)

        pools = {}
        for compute_type in sorted(compute_types):
            pods = [
                pod
                for pod in pool_list
                if self.queues.normalize_compute_type(pod.get("compute_type"))
                == compute_type
            ]
            pool_available = sum(
                1
                for pod in pods
                if pod.get("pool_status") == self.pool.STATUS_AVAILABLE
                and not pod.get("terminating", False)
            )
            pool_assigned = sum(
                1
                for pod in pods
                if pod.get("pool_status") == self.pool.STATUS_ASSIGNED
                and not pod.get("terminating", False)
            )

            item = {
                "compute_type": compute_type,
                "pool_total": pool_available + pool_assigned,
                "pool_available": pool_available,
                "pool_assigned": pool_assigned,
                "physical_total": len(pods),
                "terminating": sum(
                    1 for pod in pods if pod.get("terminating", False)
                ),
                "policy_valid": False,
                "policy_cached": False,
                "gate_active": False,
            }

            matching = deployments_by_type.get(compute_type, [])
            if deployment_list_error:
                item["policy_error"] = deployment_list_error
            elif len(matching) != 1:
                item["policy_error"] = (
                    "Exactly one warm-pool Deployment is required per compute-type"
                )
            else:
                try:
                    policy = self.pool.parse_deployment_policy(matching[0])
                    item.update(
                        {
                            "deployment_name": policy["deployment_name"],
                            "R": policy["R"],
                            "N": policy["N"],
                            "desired_replicas": min(
                                policy["R"],
                                max(0, policy["N"] - pool_assigned),
                            ),
                            "current_replicas": self.pool.read_deployment_replicas(
                                policy["deployment_name"]
                            ),
                            "policy_valid": True,
                            "policy_error": "",
                        }
                    )
                except Exception as exc:
                    item["policy_error"] = str(exc)

            try:
                cached = self.queues.get_pool_policy(compute_type)
                item["policy_cached"] = cached is not None
                item["gate_active"] = self.queues.is_scale_down_gated(
                    compute_type
                )
                if item["policy_valid"] and cached is None:
                    item["policy_error"] = (
                        "Policy is valid but is not active in the shared cache yet"
                    )
            except Exception as exc:
                if not item.get("policy_error"):
                    item["policy_error"] = str(exc)

            pools[compute_type] = item
        return pools

    def get_queue_status(self, compute_type: Optional[str] = None) -> Dict:
        if not compute_type:
            raise ValueError("compute_type is required")
        compute_type_value = self.queues.validate_compute_type(
            compute_type,
            self.queues.known_compute_types(),
        )
        now_ms = int(time.time() * 1000)
        snapshot = self.queues.list_waiting_users(
            compute_type_value,
            now_ms=now_ms,
        )
        return {
            "status": "success",
            **snapshot,
        }

    def get_ticket(self, ticket_id: str) -> Dict:
        ticket = self.tickets.get_ticket_snapshot(ticket_id)
        if not ticket:
            return {
                "status": "error",
                "message": f"Ticket not found: {ticket_id}",
            }
        status = str(ticket.get("status") or "queued").lower()
        default_message = {
            "queued": "Waiting for an available compute pod",
            "allocating": "Allocating compute pod",
            "assigned": "Compute pod assigned",
            "failed": ticket.get("error") or "Allocation failed",
            "cancelled": ticket.get("error") or "Ticket cancelled",
        }.get(status, "")
        return ticket_format.ticket_response(ticket, default_message)
