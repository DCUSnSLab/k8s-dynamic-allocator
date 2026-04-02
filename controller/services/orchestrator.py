import logging
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional

import httpx
from kubernetes.client.rest import ApiException

from config import settings
from config.settings import get_request_id

from .backend_agent import BackendAgent
from .backend_pool import BackendPool, PodConflictError
from .wait_queue import QueueUnavailableError, WaitQueue

logger = logging.getLogger(__name__)


class Orchestrator:
    """Backend pool and wait queue coordinator."""

    def __init__(self):
        self.pool = BackendPool()
        self.wait_queue = WaitQueue()
        self._backend_refresh_lock = threading.Lock()
        self._backend_refresh_interval = max(settings.WAIT_QUEUE_BACKEND_REFRESH_SECONDS, 1.0)
        self._last_backend_refresh = 0.0
        self._queue_kick_lock = threading.Lock()
        self._queue_kick_in_flight = False

    def health_check(self) -> str:
        return "Orchestrator healthy"

    def initialize_pool(self) -> Dict:
        result = self.pool.initialize_pool()
        self._refresh_backend_types(force=True)
        return result

    def _ticket_response(self, ticket: Optional[Dict], message: Optional[str] = None) -> Dict:
        if not ticket:
            return {
                "status": "error",
                "message": message or "Ticket not found",
            }

        status = str(ticket.get("status") or "queued").lower()
        ticket_id = ticket.get("ticket_id")
        response = {
            "status": status,
            "ticket_status": status,
            "message": message or "",
            "ticket_id": ticket_id,
            "backend_type": ticket.get("backend_type"),
            "ticket": ticket,
            "poll_url": f"/api/ticket/{ticket_id}/" if ticket_id else "",
            "cancel_url": f"/api/ticket/{ticket_id}/cancel/" if ticket_id else "",
            "release_url": "/api/pool/release/",
        }

        if ticket.get("backend_pod"):
            response["backend_pod"] = ticket.get("backend_pod")
        if ticket.get("backend_ip"):
            response["backend_ip"] = ticket.get("backend_ip")
        if status in {"queued", "allocating"}:
            response["retry_after_ms"] = int(max(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS, 0.2) * 1000)
        return response

    def _refresh_backend_types(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_backend_refresh) < self._backend_refresh_interval:
            return

        with self._backend_refresh_lock:
            now = time.monotonic()
            if not force and (now - self._last_backend_refresh) < self._backend_refresh_interval:
                return

            self._last_backend_refresh = now
            backend_types = set()
            try:
                self.pool.initialize_pool()
            except Exception as exc:
                logger.debug("Backend manifest refresh skipped: %s", exc)
            backend_types.update(self.wait_queue.known_backend_types())
            for item in self.pool.list_pool_status():
                if item.get("backend_type"):
                    backend_types.add(self.wait_queue.normalize_backend_type(item.get("backend_type")))

            if backend_types:
                self.wait_queue.register_backend_types(sorted(backend_types))

    def _discover_backend_types(self) -> List[str]:
        self._refresh_backend_types()
        return sorted(self.wait_queue.known_backend_types())

    def _select_backend_pod(self, backend_type: str) -> Dict:
        backend_type_value = self.wait_queue.normalize_backend_type(backend_type)
        backend_pod = self.pool.get_available_pod(backend_type_value)
        if not backend_pod:
            return {}
        return {
            "name": backend_pod,
            "ip": self.pool.get_pod_ip(backend_pod) or "",
        }

    def _recover_stale_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        backend_type = self.wait_queue.normalize_backend_type(ticket.get("backend_type"))
        backend_pod = ticket.get("backend_pod", "")
        claim_token = ticket.get("claim_token") or None
        retry_count = int(ticket.get("retry_count") or 0)
        max_retries = int(ticket.get("max_retries") or self.wait_queue.max_retries)

        try:
            current = self.wait_queue.get_ticket(ticket_id)
            if not current or str(current.get("status") or "").lower() != "allocating":
                return {
                    "ticket_id": ticket_id,
                    "backend_type": backend_type,
                    "status": "skipped",
                }
            if claim_token and (current.get("claim_token") or "") != claim_token:
                return {
                    "ticket_id": ticket_id,
                    "backend_type": backend_type,
                    "status": "skipped",
                }

            if retry_count < max_retries:
                recovered = self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason="stale allocation recovered",
                    increment_retry=True,
                    claim_token=claim_token,
                )
                if backend_pod and recovered and recovered.get("status") == "queued":
                    try:
                        self.pool.release_pod(backend_pod)
                    except Exception as exc:
                        logger.warning(
                            "Failed to release stale backend %s for ticket %s: %s",
                            backend_pod,
                            ticket_id,
                            exc,
                        )
                if recovered:
                    return {
                        "ticket_id": ticket_id,
                        "backend_type": backend_type,
                        "status": "requeued",
                    }
                return {
                    "ticket_id": ticket_id,
                    "backend_type": backend_type,
                    "status": "skipped",
                }

            failed = self.wait_queue.mark_failed(
                ticket_id,
                "stale allocation exceeded retry budget",
                claim_token=claim_token,
            )
            if backend_pod and failed and failed.get("status") == "failed":
                try:
                    self.pool.release_pod(backend_pod)
                except Exception as exc:
                    logger.warning(
                        "Failed to release stale backend %s for ticket %s: %s",
                        backend_pod,
                        ticket_id,
                        exc,
                    )
            if failed:
                return {
                    "ticket_id": ticket_id,
                    "backend_type": backend_type,
                    "status": "failed",
                }
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "skipped",
            }
        except QueueUnavailableError as exc:
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "error",
                "error": str(exc),
            }

    def _kick_wait_queue_worker(self) -> None:
        with self._queue_kick_lock:
            if self._queue_kick_in_flight:
                return
            self._queue_kick_in_flight = True

        def _runner():
            try:
                self.process_wait_queues()
            finally:
                with self._queue_kick_lock:
                    self._queue_kick_in_flight = False

        threading.Thread(
            target=_runner,
            name="queue-kick",
            daemon=True,
        ).start()

    def _execute_allocated_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        backend_type = self.wait_queue.normalize_backend_type(ticket.get("backend_type"))
        backend_pod = ticket.get("backend_pod", "")
        backend_ip = ticket.get("backend_ip", "")
        frontend_pod = ticket.get("frontend_pod") or ""
        frontend_ip = ticket.get("frontend_ip") or ""
        command = ticket.get("command") or ""
        claim_token = ticket.get("claim_token") or ""

        if not backend_pod or not frontend_ip:
            self.wait_queue.mark_failed(ticket_id, "Missing backend or frontend context", claim_token=claim_token)
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "failed",
                "message": "Missing backend or frontend context",
            }

        current = self.wait_queue.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "queued",
                "message": "Ticket no longer owns the allocation",
            }

        try:
            agent = BackendAgent(backend_ip)
            try:
                agent.mount(frontend_ip, command, frontend_pod)
            finally:
                agent.close()
        except httpx.HTTPError as exc:
            logger.warning("Mount failed for ticket %s on %s: %s", ticket_id, backend_pod, exc)
            try:
                self.pool.release_pod(backend_pod)
            except Exception as release_exc:
                logger.warning(
                    "Failed to release backend pod %s after mount error: %s",
                    backend_pod,
                    release_exc,
                )

            current = self.wait_queue.get_ticket(ticket_id)
            if current and int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.wait_queue.max_retries):
                requeued = self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason=f"Mount failed: {exc}",
                    increment_retry=True,
                    claim_token=claim_token,
                )
                requeued_ticket = requeued if requeued and requeued.get("status") == "queued" else self.wait_queue.get_ticket(ticket_id)
                return self._ticket_response(requeued_ticket, str(exc))

            failed = self.wait_queue.mark_failed(ticket_id, f"Mount failed: {exc}", claim_token=claim_token)
            failed_ticket = failed if failed and failed.get("status") == "failed" else self.wait_queue.get_ticket(ticket_id)
            return self._ticket_response(failed_ticket, str(exc))
        except Exception as exc:
            logger.exception("Unexpected mount failure for ticket %s on %s", ticket_id, backend_pod)
            try:
                self.pool.release_pod(backend_pod)
            except Exception as release_exc:
                logger.warning(
                    "Failed to release backend pod %s after unexpected mount error: %s",
                    backend_pod,
                    release_exc,
                )

            current = self.wait_queue.get_ticket(ticket_id)
            if current and int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.wait_queue.max_retries):
                requeued = self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason=f"Unexpected mount failure: {exc}",
                    increment_retry=True,
                    claim_token=claim_token,
                )
                requeued_ticket = requeued if requeued and requeued.get("status") == "queued" else self.wait_queue.get_ticket(ticket_id)
                return self._ticket_response(requeued_ticket, str(exc))

            failed = self.wait_queue.mark_failed(ticket_id, f"Unexpected mount failure: {exc}", claim_token=claim_token)
            failed_ticket = failed if failed and failed.get("status") == "failed" else self.wait_queue.get_ticket(ticket_id)
            return self._ticket_response(failed_ticket, str(exc))

        current = self.wait_queue.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.pool.release_pod(backend_pod)
            except Exception:
                pass
            return {
                "ticket_id": ticket_id,
                "backend_type": backend_type,
                "status": "queued",
                "message": "Ticket was cancelled before commit",
            }

        committed = self.wait_queue.mark_assigned(ticket_id, backend_pod, backend_ip, claim_token=claim_token)
        if committed and committed.get("status") == "assigned":
            response = self._ticket_response(committed, "Command assigned to backend")
            response.update(
                {
                    "frontend_pod": frontend_pod,
                    "frontend_ip": frontend_ip,
                    "command": command,
                    "submitted_at": datetime.now().isoformat(),
                }
            )
            return response

        try:
            self.pool.release_pod(backend_pod)
        except Exception:
            pass
        current = self.wait_queue.get_ticket(ticket_id)
        return self._ticket_response(current or ticket, "Ticket no longer owns the allocation")

    def _drain_wait_queue_for_type(self, backend_type: str) -> Dict:
        backend_type_value = self.wait_queue.normalize_backend_type(backend_type)
        result = {
            "backend_type": backend_type_value,
            "stale_recovered": 0,
            "claimed": 0,
            "assigned": 0,
            "queued": 0,
            "failed": 0,
            "errors": [],
        }

        lock_token = None
        ticket = None
        try:
            lock_token = self.wait_queue.acquire_allocator_lock(backend_type_value)
            if not lock_token:
                result["queued"] += 1
                return result

            for stale_ticket in self.wait_queue.find_stale_allocating_tickets(backend_type_value):
                recovered = self._recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    result["stale_recovered"] += 1
                elif recovered["status"] == "failed":
                    result["failed"] += 1
                elif recovered["status"] == "error":
                    result["errors"].append(recovered)

            ticket = self.wait_queue.claim_next_ticket(
                backend_type_value,
                worker_id=self.wait_queue.worker_identity,
            )
            if not ticket:
                return result
            ticket_id = ticket["ticket_id"]

            backend = self._select_backend_pod(backend_type_value)
            if not backend:
                self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason="No available backend pods",
                    increment_retry=False,
                    claim_token=ticket.get("claim_token"),
                )
                result["queued"] += 1
                return result

            backend_pod = backend.get("name", "")
            frontend_identity = ticket.get("frontend_pod") or "unknown"

            try:
                self.pool.assign_pod(backend_pod, frontend_identity)
            except PodConflictError as exc:
                self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason=f"Backend contention: {exc}",
                    increment_retry=False,
                    claim_token=ticket.get("claim_token"),
                )
                result["errors"].append(
                    {
                        "ticket_id": ticket_id,
                        "backend_type": backend_type_value,
                        "error": str(exc),
                    }
                )
                return result

            backend_ip = self.pool.get_pod_ip(backend_pod) or ""
            if not backend_ip:
                try:
                    self.pool.release_pod(backend_pod)
                except Exception:
                    pass
                self.wait_queue.requeue_ticket(
                    ticket_id,
                    reason="Backend IP unavailable",
                    increment_retry=False,
                    claim_token=ticket.get("claim_token"),
                )
                result["queued"] += 1
                return result

            ticket = self.wait_queue.mark_allocating(
                ticket_id,
                backend_pod=backend_pod,
                backend_ip=backend_ip,
                claimed_by=ticket.get("claimed_by"),
                claim_token=ticket.get("claim_token"),
            )
            if not ticket or ticket.get("status") != "allocating":
                try:
                    self.pool.release_pod(backend_pod)
                except Exception:
                    pass
                result["errors"].append(
                    {
                        "ticket_id": ticket_id,
                        "backend_type": backend_type_value,
                        "error": "Ticket lost ownership before backend commit",
                    }
                )
                return result
            result["claimed"] += 1
        finally:
            if lock_token:
                self.wait_queue.release_allocator_lock(backend_type_value, lock_token)

        if not ticket:
            return result

        execution = self._execute_allocated_ticket(ticket)
        if execution["status"] == "assigned":
            result["assigned"] += 1
        elif execution["status"] == "queued":
            result["queued"] += 1
        elif execution["status"] == "failed":
            result["failed"] += 1
        else:
            result["errors"].append(execution)
        return result

    def execute_command(
        self,
        username: str,
        command: str,
        frontend_ip: str,
        frontend_pod: str = "",
        backend_type: Optional[str] = None,
    ) -> Dict:
        try:
            if not frontend_ip:
                return {
                    "status": "error",
                    "message": "frontend_ip is required",
                }

            self._refresh_backend_types()
            backend_type_value = self.wait_queue.validate_backend_type(
                backend_type,
                self._discover_backend_types(),
            )
            frontend_pod_value = frontend_pod or ""
            ticket = self.wait_queue.create_ticket(
                username=username,
                command=command,
                frontend_pod=frontend_pod_value,
                frontend_ip=frontend_ip,
                backend_type=backend_type_value,
                request_id=get_request_id(),
            )

            self._kick_wait_queue_worker()
            current_ticket = self.wait_queue.get_ticket(ticket["ticket_id"])
            response = self._ticket_response(current_ticket or ticket)
            if response["status"] == "assigned":
                response["message"] = "Command assigned to backend"
            elif response["status"] == "failed":
                response["message"] = "Command failed during allocation"
            elif response["status"] == "cancelled":
                response["message"] = "Ticket was cancelled during allocation"
            else:
                response["message"] = "Command queued for allocation"
            return response
        except ValueError as exc:
            return {
                "status": "error",
                "message": str(exc),
                "error_type": "invalid_backend_type",
            }
        except QueueUnavailableError as exc:
            logger.error("Queue unavailable during execute: %s", exc)
            return {
                "status": "error",
                "message": str(exc),
            }
        except ApiException as exc:
            logger.error("[FAILED] Execute: K8s API error (%s %s)", exc.status, exc.reason)
            return {
                "status": "error",
                "message": f"K8s API error: {exc.reason}",
            }
        except Exception as exc:
            logger.error("[FAILED] Execute: Unexpected error (%s)", exc)
            return {
                "status": "error",
                "message": f"Unexpected error: {exc}",
            }

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

    def get_ticket(self, ticket_id: str) -> Dict:
        ticket = self.wait_queue.get_ticket(ticket_id)
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
        return self._ticket_response(ticket, default_message)

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Dict:
        try:
            ticket = self.wait_queue.get_ticket(ticket_id)
            if not ticket:
                return {
                    "status": "error",
                    "message": f"Ticket not found: {ticket_id}",
                }

            current = self.wait_queue.get_ticket(ticket_id)
            if not current:
                return {
                    "status": "error",
                    "message": f"Ticket not found: {ticket_id}",
                }
            if current.get("status") == "assigned":
                return {
                    "status": "error",
                    "message": "Ticket is already assigned",
                    "ticket": current,
                }

            cancelled = self.wait_queue.cancel_ticket(ticket_id, reason=reason)
            final_ticket = cancelled or self.wait_queue.get_ticket(ticket_id) or current
            backend_pod = final_ticket.get("backend_pod") or ""
            if backend_pod and final_ticket.get("status") in {"cancelled", "failed"}:
                try:
                    self.pool.release_pod(backend_pod)
                except Exception as exc:
                    logger.warning("Failed to release backend pod %s after cancel: %s", backend_pod, exc)

            if final_ticket.get("status") == "assigned":
                return {
                    "status": "error",
                    "message": "Ticket is already assigned",
                    "ticket": final_ticket,
                }
            message = "Ticket cancelled" if final_ticket.get("status") == "cancelled" else "Cancellation did not change ticket state"
            return self._ticket_response(final_ticket, message)
        except QueueUnavailableError as exc:
            return {
                "status": "error",
                "message": str(exc),
            }

    def release_backend(self, backend_pod: str) -> Dict:
        try:
            frontend = "unknown"
            try:
                pod = self.pool.v1.read_namespaced_pod(backend_pod, self.pool.namespace)
                frontend = (pod.metadata.labels or {}).get(self.pool.LABEL_FRONTEND, "unknown")
            except ApiException as exc:
                if exc.status != 404:
                    raise
                logger.info("Backend pod already released: %s", backend_pod)
                return {
                    "status": "success",
                    "message": f"Backend already released: {backend_pod}",
                }

            backend_ip = self.pool.get_pod_ip(backend_pod)
            if backend_ip:
                agent = BackendAgent(backend_ip)
                try:
                    agent.unmount()
                finally:
                    agent.close()

            self.pool.release_pod(backend_pod)
            logger.info("[SUCCESS] Released: %s <-> %s (%s)", frontend, backend_pod, backend_ip)
            return {
                "status": "success",
                "message": f"Released: {backend_pod}",
            }
        except Exception as exc:
            logger.error("[FAILED] Release: %s (%s)", backend_pod, exc)
            return {
                "status": "error",
                "message": str(exc),
            }

    def process_wait_queues(self) -> Dict:
        try:
            self._refresh_backend_types()
            backend_types = set(self._discover_backend_types())
            processed = []
            for backend_type in sorted(backend_types):
                try:
                    processed.append(self._drain_wait_queue_for_type(backend_type))
                except Exception as exc:
                    logger.exception("Failed to drain wait queue for %s", backend_type)
                    processed.append(
                        {
                            "backend_type": backend_type,
                            "status": "error",
                            "error": str(exc),
                        }
                    )

            return {
                "status": "success",
                "processed": processed,
            }
        except QueueUnavailableError as exc:
            return {
                "status": "error",
                "message": str(exc),
            }

    def check_stale_allocations(self) -> Dict:
        queue_recovered = []
        queue_failed = []

        try:
            for stale_ticket in self.wait_queue.find_stale_allocating_tickets():
                recovered = self._recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    queue_recovered.append(recovered["ticket_id"])
                elif recovered["status"] == "failed":
                    queue_failed.append(recovered["ticket_id"])
        except QueueUnavailableError as exc:
            logger.warning("Queue stale recovery skipped: %s", exc)

        pool_list = self.pool.list_pool_status()
        assigned = [pod for pod in pool_list if pod["pool_status"] == "assigned"]

        released = []
        errors = []

        for pod_info in assigned:
            frontend_pod = pod_info.get("assigned_frontend", "")
            backend_pod = pod_info["name"]

            if not frontend_pod or frontend_pod == "unknown":
                continue

            frontend_status = self.pool.get_pod_status(frontend_pod)

            if frontend_status is None or frontend_status != "Running":
                logger.warning(
                    "Frontend '%s' is %s, releasing '%s'",
                    frontend_pod,
                    frontend_status,
                    backend_pod,
                )
                result = self.release_backend(backend_pod)
                if result["status"] == "success":
                    released.append(backend_pod)
                else:
                    errors.append({"pod": backend_pod, "error": result["message"]})

        return {
            "checked": len(assigned),
            "released": released,
            "queue_recovered": queue_recovered,
            "queue_failed": queue_failed,
            "errors": errors,
        }
