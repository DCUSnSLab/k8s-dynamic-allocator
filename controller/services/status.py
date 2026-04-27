import time
from typing import Dict, Optional

from . import ticket_format


class ControllerStatus:
    def __init__(self, pool, queues, tickets):
        self.pool = pool
        self.queues = queues
        self.tickets = tickets

    def get_pool_status(self) -> Dict:
        pool_list = self.pool.list_pool_status()

        available_count = sum(1 for pod in pool_list if pod["pool_status"] == "available")
        assigned_count = sum(1 for pod in pool_list if pod["pool_status"] == "assigned")

        return {
            "total": len(pool_list),
            "available": available_count,
            "assigned": assigned_count,
            "pods": pool_list,
        }

    def get_queue_status(self, backend_type: Optional[str] = None) -> Dict:
        if not backend_type:
            raise ValueError("backend_type is required")
        backend_type_value = self.queues.validate_backend_type(
            backend_type,
            self.queues.known_backend_types(),
        )
        now_ms = int(time.time() * 1000)
        snapshot = self.queues.list_waiting_frontends(
            backend_type_value,
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
            "queued": "Waiting for an available backend",
            "allocating": "Allocating backend",
            "assigned": "Backend assigned",
            "failed": ticket.get("error") or "Allocation failed",
            "cancelled": ticket.get("error") or "Ticket cancelled",
        }.get(status, "")
        return ticket_format.ticket_response(ticket, default_message)
