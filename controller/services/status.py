import time
from typing import Dict, Optional

from . import ticket_format


class ControllerStatus:
    def __init__(self, provider, queues, tickets, capacity_reconciler=None):
        self.provider = provider
        self.queues = queues
        self.tickets = tickets
        self.capacity_reconciler = capacity_reconciler

    def get_buffer_status(self) -> Dict:
        """버퍼 상태. 배포 smoke 검사와 run 기록(시뮬레이터)이 읽는다."""
        snapshot = self.provider.snapshot()
        pods = snapshot["pods"]
        capacity = self.provider.read_capacity()
        response = {
            "allocation_mode": self.provider.allocation_mode,
            "R": self.provider.read_reserve(),
            "N": capacity,
            "capacity": capacity,
            "total": len(snapshot["active_names"]),
            "available": len(snapshot["idle"]),
            "available_ready": len(snapshot["idle_ready"]),
            "assigned": int(snapshot["assigned"]),
            "terminating": sum(1 for pod in pods if pod["terminating"]),
            "physical_total": len(pods),
            "pods": [
                {key: (value.isoformat() if hasattr(value, "isoformat") else value)
                 for key, value in pod.items() if key != "annotations"}
                for pod in pods
            ],
        }
        if self.capacity_reconciler is not None:
            response["leader_control"] = self.capacity_reconciler.get_status()
        return response

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
