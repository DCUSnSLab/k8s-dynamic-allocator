import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from kubernetes.client.rest import ApiException

from config.settings import request_label_scope, set_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError, safe_int
from .agent_client import ComputeAgent

logger = logging.getLogger(__name__)


class ComputeReleaser:
    _RELEASE_MAX_ATTEMPTS = 2

    def __init__(
        self,
        provider,
        queues,
        tickets,
        on_released: Optional[Callable[[Optional[str]], None]] = None,
    ):
        self.provider = provider
        self.queues = queues
        self.tickets = tickets
        self._on_released = on_released or (lambda compute_type: None)

    def set_release_callback(self, on_released: Callable[[Optional[str]], None]) -> None:
        self._on_released = on_released

    def release_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
        expected_status: Optional[str] = None,
    ) -> Dict:
        """Release a Compute Pod back to the buffer.

        expected_status 는 호출자(정리)가 마지막으로 본 파드 상태다. 그 뒤 바뀐 파드는
        건드리지 않는다(아래 _release_once).
        """
        return self._release(compute_pod, request_context, unmount=True, expected_status=expected_status)

    def release_unreachable_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
        expected_status: Optional[str] = None,
        min_not_ready_seconds: Optional[float] = None,
    ) -> Dict:
        """Release a Compute Pod whose agent is already known to be silent.

        Skips the unmount call, which would wait out its full timeout before
        failing anyway. Everything else matches an ordinary release.

        min_not_ready_seconds 는 호출자(정리)가 "NotReady 가 오래됐다" 고 본 기준이다. 방금
        읽은 파드가 그새 Ready 로 돌아왔거나 그 기준을 채우지 못하면 손대지 않는다.
        """
        return self._release(
            compute_pod,
            request_context,
            unmount=False,
            expected_status=expected_status,
            min_not_ready_seconds=min_not_ready_seconds,
        )

    def _release(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]],
        unmount: bool,
        expected_status: Optional[str] = None,
        min_not_ready_seconds: Optional[float] = None,
    ) -> Dict:
        # Tags this thread with the ticket's label so its own log lines attribute
        # correctly; the caller's label comes back once the release is done.
        with request_label_scope():
            return self._release_with_retries(
                compute_pod, request_context, unmount, expected_status, min_not_ready_seconds
            )

    def _release_with_retries(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]],
        unmount: bool,
        expected_status: Optional[str] = None,
        min_not_ready_seconds: Optional[float] = None,
    ) -> Dict:
        release_started_ms = int(time.time() * 1000)
        request_context_value = dict(request_context or {})

        cleanup_context = False
        compute_unmounted = not unmount
        released_compute_type: Optional[str] = None

        def _release_once() -> Dict:
            nonlocal cleanup_context, compute_unmounted, released_compute_type
            release_started = time.perf_counter()
            compute_type = self.queues.default_compute_type
            try:
                pod = self.provider.v1.read_namespaced_pod(compute_pod, self.provider.namespace)
                labels = pod.metadata.labels or {}
                compute_type = self.queues.normalize_compute_type(labels.get(self.provider.LABEL_COMPUTE_TYPE))
            except ApiException as exc:
                if exc.status != 404:
                    raise
                logger.debug("[Released] compute_pod=%s status=already_released", compute_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Compute already released: {compute_pod}",
                }

            # 정리는 몇 초 전의 목록으로 정한다. 그사이 NotReady 였던 idle 이 Ready 로 돌아와
            # 할당됐으면 그 파드는 이제 다른 요청의 것이다. 방금 읽은 상태로 확인한다(추가
            # 왕복 없음). 요청 컨텍스트는 지우지 않는다 - 새 주인의 것일 수 있다.
            pod_status = labels.get(self.provider.LABEL_STATUS)
            if expected_status and pod_status != expected_status:
                return self._ignore_release(compute_pod, expected_status, pod_status, "stale_snapshot")
            # 라벨은 그대로인 채 Ready 만 돌아왔을 수 있다. 정리 사유(NotReady 가 오래됐다)가
            # 지금도 맞는지 정리와 같은 기준으로 다시 본다.
            if min_not_ready_seconds is not None and not self._still_not_ready(pod, min_not_ready_seconds):
                return self._ignore_release(compute_pod, expected_status or "", pod_status, "recovered")

            assigned_context = dict(request_context_value)
            # An empty dict means the caller already looked and found nothing.
            if request_context is None:
                try:
                    assigned_context = self.tickets.get_assigned_request_context(compute_pod) or {}
                except QueueUnavailableError as exc:
                    logger.debug(
                        "[AssignedContextUnavailable] compute_pod=%s reason=%r",
                        compute_pod,
                        str(exc),
                    )

            if assigned_context.get("request_label"):
                set_request_label(assigned_context.get("request_label"))

            ticket = self._ticket_for_compute_pod_with_context(compute_pod, assigned_context)
            if ticket:
                ticket_format.set_ticket_context(ticket)
                compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
            elif assigned_context:
                compute_type = self.queues.normalize_compute_type(
                    assigned_context.get("compute_type") or compute_type
                )
            released_compute_type = compute_type

            compute_pod_ip = self.provider.get_pod_ip(compute_pod)
            if compute_pod_ip and not compute_unmounted:
                # Best effort: the Pod is deleted right after and takes the mount
                # namespace with it, so a silent agent must not block the release.
                try:
                    with ComputeAgent(compute_pod_ip) as agent:
                        agent.unmount()
                        compute_unmounted = True
                except Exception as exc:
                    logger.warning(
                        "[Warning] operation=compute_unmount compute_pod=%s reason=%r",
                        compute_pod,
                        str(exc),
                    )

            if pod_status == self.provider.STATUS_IDLE or min_not_ready_seconds is not None:
                # 읽은 버전을 전제로 지운다. 읽은 뒤 할당(claim PATCH)이 끼어들거나 Ready 로
                # 돌아오면(kubelet 이 상태를 쓴다) 버전이 바뀌어 지워지지 않는다(409). 그 밖의
                # 이유로 버전만 바뀐 경우는 다음 정리가 다시 본다.
                released_now = self.provider.delete_idle_pod(
                    compute_pod,
                    getattr(pod.metadata, "uid", "") or "",
                    getattr(pod.metadata, "resource_version", "") or "",
                )
                if not released_now:
                    return self._ignore_release(compute_pod, expected_status or "", pod_status, "changed_before_delete")
            else:
                released_now = self.provider.release_pod(compute_pod)
            if not released_now:
                logger.debug("[Released] compute_pod=%s status=already_released_or_terminating", compute_pod)
                cleanup_context = True
                return {
                    "status": "success",
                    "message": f"Compute already released: {compute_pod}",
                }

            release_ms = int((time.perf_counter() - release_started) * 1000)
            assigned_at_ms = 0
            if ticket:
                assigned_at_ms = ticket_format.datetime_to_epoch_ms(ticket.get("assigned_at"))
            if not assigned_at_ms:
                assigned_at_ms = safe_int(assigned_context.get("assigned_at_ms"), 0)
            session_ms = max(0, release_started_ms - assigned_at_ms) if assigned_at_ms else None
            ticket_format.log_queue_event(
                "info",
                "Released",
                ticket,
                component="SUCCESS",
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip or "",
                session_ms=session_ms,
                release_ms=release_ms,
            )
            cleanup_context = True
            return {
                "status": "success",
                "message": f"Released: {compute_pod}",
            }

        try:
            for attempt in range(1, self._RELEASE_MAX_ATTEMPTS + 1):
                try:
                    response = _release_once()
                    if response.get("status") == "success" and response.get("outcome") != "ignored":
                        self._on_released(released_compute_type)
                    return response
                except Exception as exc:
                    if attempt < self._RELEASE_MAX_ATTEMPTS:
                        logger.debug(
                            "[ReleaseRetry] compute_pod=%s attempt=%s max_attempts=%s reason=%r",
                            compute_pod,
                            attempt,
                            self._RELEASE_MAX_ATTEMPTS,
                            str(exc),
                        )
                        continue
                    logger.error("[Failed] operation=release compute_pod=%s reason=%r", compute_pod, str(exc))
                    return {
                        "status": "error",
                        "message": str(exc),
                    }
        finally:
            if cleanup_context:
                self.clear_assigned_request_context_best_effort(compute_pod)

    def _still_not_ready(self, pod, min_not_ready_seconds: float) -> bool:
        """정리의 기준(cleanup._not_ready_seconds)과 같다: Ready 였다가 떨어진 뒤 그만큼 지났나."""
        not_ready_since = self.provider._pod_not_ready_since(pod)
        if not isinstance(not_ready_since, datetime):
            return False
        return (datetime.now(timezone.utc) - not_ready_since).total_seconds() >= min_not_ready_seconds

    @staticmethod
    def _ignore_release(compute_pod: str, expected_status: str, pod_status: Optional[str], reason: str) -> Dict:
        logger.info(
            "[ReleaseIgnored] compute_pod=%s expected_status=%s pod_status=%s reason=%s",
            compute_pod,
            expected_status or "-",
            pod_status or "-",
            reason,
        )
        return {
            "status": "success",
            "outcome": "ignored",
            "message": f"Release ignored ({reason}): {compute_pod}",
        }

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        return self.tickets.get_assigned_request_context(compute_pod)

    def _ticket_for_compute_pod(self, compute_pod: str, compute_type: Optional[str] = None) -> Optional[Dict]:
        try:
            ticket = self.tickets.find_ticket_by_compute_pod_index_only(
                compute_pod,
                compute_type=compute_type,
            )
        except QueueUnavailableError:
            return None
        return ticket

    def _ticket_for_compute_pod_with_context(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Optional[Dict]:
        ticket_id = ""
        compute_type = None
        if request_context:
            ticket_id = (request_context.get("ticket_id") or "").strip()
            compute_type = request_context.get("compute_type") or None

        if ticket_id:
            try:
                ticket = self.tickets.get_ticket_snapshot(ticket_id)
            except QueueUnavailableError as exc:
                logger.debug(
                    "[TicketLookupSkipped] ticket_id=%s compute_pod=%s reason=%r",
                    ticket_id,
                    compute_pod,
                    str(exc),
                )
            else:
                if ticket:
                    assigned_compute = (ticket.get("compute_pod") or "").strip()
                    if assigned_compute == compute_pod and str(ticket.get("status") or "").lower() == "assigned":
                        return ticket

        return self._ticket_for_compute_pod(compute_pod, compute_type=compute_type)

    def release_pod_best_effort(self, compute_pod: str, ticket_id: str) -> bool:
        if not compute_pod:
            return True
        try:
            self.provider.release_pod(compute_pod)
            return True
        except Exception as exc:
            logger.warning(
                "[Warning] operation=release_stale_compute compute_pod=%s ticket_id=%s reason=%r",
                compute_pod,
                ticket_id,
                str(exc),
            )
            return False

    def clear_assigned_request_context_best_effort(self, compute_pod: str) -> None:
        try:
            self.tickets.clear_assigned_request_context(compute_pod)
        except QueueUnavailableError as exc:
            logger.warning(
                "[Warning] operation=assigned_context_cleanup compute_pod=%s status=skipped reason=%r",
                compute_pod,
                str(exc),
            )
        except Exception as exc:
            logger.warning(
                "[Warning] operation=assigned_context_cleanup compute_pod=%s status=failed reason=%r",
                compute_pod,
                str(exc),
            )
