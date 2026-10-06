import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from kubernetes.client.rest import ApiException

from config import settings
from config.settings import request_label_scope, set_request_label

from .. import ticket_format
from ..queue import QueueUnavailableError, safe_int
from .agent_client import AgentTicketMismatch, ComputeAgent, ComputeAgentError

logger = logging.getLogger(__name__)


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


class ComputeReleaser:
    _RELEASE_MAX_ATTEMPTS = 2
    _RELABEL_MARGIN_SECONDS = 20.0

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
        self.release_lock_stale_seconds = settings.RELEASE_LOCK_STALE_SECONDS

    def set_release_callback(self, on_released: Callable[[Optional[str]], None]) -> None:
        self._on_released = on_released

    def release_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
        expected_status: Optional[str] = None,
        expected_ticket_id: str = "",
    ) -> Dict:
        """Release a Compute Pod by deleting it; the Deployment backfills a fresh one.

        expected_status (and, for an assigned Pod, expected_ticket_id) is the Pod
        as the caller last saw it. A Pod that has changed since is left alone:
        with reuse it may have been returned and handed to the next user.
        """
        return self._release(
            compute_pod,
            request_context,
            unmount=True,
            expected_status=expected_status,
            expected_ticket_id=expected_ticket_id,
        )

    def release_unreachable_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
        expected_status: Optional[str] = None,
        expected_ticket_id: str = "",
    ) -> Dict:
        """Release a Compute Pod whose agent is already known to be silent.

        Skips the unmount call, which would wait out its full timeout before
        failing anyway. Everything else matches an ordinary release.
        """
        return self._release(
            compute_pod,
            request_context,
            unmount=False,
            expected_status=expected_status,
            expected_ticket_id=expected_ticket_id,
        )

    def release_stale_locked_compute_pod(
        self,
        compute_pod: str,
        ticket_id: str,
        stale_lock_token: str,
        request_context: Optional[Dict[str, object]] = None,
        unmount: bool = True,
    ) -> Dict:
        """Delete a Pod still assigned to a ticket whose release died holding its lock.

        Cleanup's backstop, since nothing else would ever release such a Pod.
        The lock is taken over from stale_lock_token rather than acquired, so
        only that one lock is bypassed; a release that has meanwhile moved it
        on still wins.
        """
        return self._release(
            compute_pod,
            request_context,
            unmount=unmount,
            expected_status=self.provider.STATUS_ASSIGNED,
            expected_ticket_id=ticket_id,
            stale_lock_token=stale_lock_token,
        )

    def return_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
        ticket_id: str = "",
    ) -> Dict:
        """Scrub a Compute Pod its user released and hand it back to the buffer.

        Only for the user's own release and the agent's fallback. Every other
        path deletes: a Pod whose state nobody vouches for must never reach
        another user. ticket_id is the caller's and required; a release naming
        a ticket the Pod no longer carries is late and changes nothing.
        """
        with request_label_scope():
            return self._return(compute_pod, request_context, (ticket_id or "").strip())

    def _release(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]],
        unmount: bool,
        expected_status: Optional[str] = None,
        expected_ticket_id: str = "",
        stale_lock_token: str = "",
    ) -> Dict:
        # Tags this thread with the ticket's label so its own log lines attribute
        # correctly; the caller's label comes back once the release is done.
        with request_label_scope():
            return self._release_with_retries(
                compute_pod,
                request_context,
                unmount,
                expected_status,
                (expected_ticket_id or "").strip(),
                stale_lock_token or "",
            )

    def _release_with_retries(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]],
        unmount: bool,
        expected_status: Optional[str],
        expected_ticket: str,
        stale_lock_token: str,
    ) -> Dict:
        release_started_ms = int(time.time() * 1000)
        request_context_value = dict(request_context or {})
        outcome = "deleted_stale_lock" if stale_lock_token else "deleted"

        cleanup_context = False
        # Whose assignment keys the final clear may drop; empty when the Pod
        # named no ticket, and the clear is then unconditional as before.
        context_owner = ""
        compute_unmounted = not unmount
        released_compute_type: Optional[str] = None
        # Held across attempts, so a retry does not find its own lock taken.
        lock_ticket = ""
        lock_token: Optional[str] = None
        # Once the delete went out (or the Pod was already gone) the lock stays:
        # nothing is left for a later release to do.
        pod_settled = False

        def _release_once() -> Dict:
            nonlocal cleanup_context, context_owner, compute_unmounted, released_compute_type
            nonlocal lock_ticket, lock_token, pod_settled
            release_started = time.perf_counter()
            try:
                pod = self.provider.v1.read_namespaced_pod(compute_pod, self.provider.namespace)
            except ApiException as exc:
                if exc.status != 404:
                    raise
                pod_settled = True
                cleanup_context = True
                context_owner = lock_ticket or expected_ticket
                return self._ignore_release(compute_pod, context_owner, "", "already_released")

            metadata = pod.metadata
            labels = metadata.labels or {}
            annotations = metadata.annotations or {}
            compute_type = self.queues.normalize_compute_type(labels.get(self.provider.LABEL_COMPUTE_TYPE))
            pod_status = labels.get(self.provider.LABEL_STATUS)
            pod_ticket = (annotations.get(self.provider.ANNOTATION_ALLOCATION_TICKET) or "").strip()

            if expected_status and (
                pod_status != expected_status
                or (expected_status == self.provider.STATUS_ASSIGNED and pod_ticket != expected_ticket)
            ):
                # The caller decided on an older view of the Pod, which with
                # reuse may since have been returned and handed to someone else.
                return self._ignore_release(compute_pod, expected_ticket, pod_ticket, "stale_snapshot")

            ticket_id = pod_ticket if pod_status == self.provider.STATUS_ASSIGNED else ""
            if ticket_id != lock_ticket:
                self._drop_release_lock_best_effort(lock_ticket, lock_token)
                lock_ticket, lock_token = "", None
            if ticket_id and not lock_token:
                # The return path takes the same lock, so this delete and the
                # Pod's return never act on one ticket at once.
                if stale_lock_token:
                    lock_token = self.tickets.take_over_release_lock(ticket_id, stale_lock_token)
                else:
                    lock_token = self.tickets.acquire_release_lock(ticket_id)
                if not lock_token:
                    return self._ignore_release(compute_pod, ticket_id, pod_ticket, "duplicate")
                lock_ticket = ticket_id

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
            context_ticket = (assigned_context.get("ticket_id") or "").strip()
            if ticket_id and context_ticket and context_ticket != ticket_id:
                # Keyed by Pod name, so with reuse it may describe another ticket.
                assigned_context = {}

            if assigned_context.get("request_label"):
                set_request_label(assigned_context.get("request_label"))

            if ticket_id:
                ticket = self._ticket_by_id(compute_pod, ticket_id)
            else:
                ticket = self._ticket_for_compute_pod_with_context(compute_pod, assigned_context)
            if ticket:
                ticket_format.set_ticket_context(ticket)
                compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
            elif assigned_context:
                compute_type = self.queues.normalize_compute_type(
                    assigned_context.get("compute_type") or compute_type
                )
            released_compute_type = compute_type

            compute_pod_ip = getattr(pod.status, "pod_ip", None) or ""
            if compute_pod_ip and not compute_unmounted:
                # Best effort: the Pod is deleted right after and takes the mount
                # namespace with it, so a silent agent must not block the release.
                try:
                    with ComputeAgent(compute_pod_ip) as agent:
                        agent.unmount(ticket_id=ticket_id or None)
                        compute_unmounted = True
                except AgentTicketMismatch as exc:
                    # The agent serves someone else, so whatever the labels say
                    # this Pod is not the ticket's to delete.
                    self._drop_release_lock_best_effort(lock_ticket, lock_token)
                    lock_ticket, lock_token = "", None
                    return self._ignore_release(
                        compute_pod,
                        ticket_id,
                        pod_ticket,
                        "agent_ticket_mismatch",
                        agent_ticket_id=exc.current_ticket_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "[Warning] operation=compute_unmount compute_pod=%s reason=%r",
                        compute_pod,
                        str(exc),
                    )

            released_now = self.provider.release_pod(compute_pod)
            pod_settled = True
            cleanup_context = True
            context_owner = ticket_id
            if not released_now:
                return self._ignore_release(compute_pod, ticket_id, pod_ticket, "already_released")

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
                compute_pod_ip=compute_pod_ip,
                # Written on this branch so every [Released] has an outcome
                # and the shares across outcomes add up.
                outcome=outcome,
                session_ms=session_ms,
                release_ms=release_ms,
            )
            # 자리가 비는 속도를 남긴다. 지금 버퍼 크기는 큐 길이로 정하므로 이 값을
            # 읽는 쪽은 없지만, 제안 시스템과 코드를 같게 두려고 유지한다 - 두 비교군이
            # 재사용 기능 말고는 달라지지 않아야 한다.
            try:
                self.queues.record_release(compute_type, compute_pod)
            except Exception as exc:  # noqa: BLE001 - 신호는 반납을 막지 않는다
                logger.debug("[ReleaseSignalSkipped] reason=%r", str(exc))
            return {
                "status": "success",
                "outcome": outcome,
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
            # The Pod is still assigned to the ticket and was not deleted, so the
            # lock is freed for cleanup's next sweep to try again. The stale-lock
            # backstop keeps its own instead: with no lock left, that sweep would
            # not come back to a Pod whose User Pod is still running.
            if lock_token and not pod_settled and not stale_lock_token:
                self._drop_release_lock_best_effort(lock_ticket, lock_token)
            if cleanup_context:
                if context_owner:
                    self._clear_owned_context_best_effort(compute_pod, context_owner)
                else:
                    self.clear_assigned_request_context_best_effort(compute_pod)

    def _return(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]],
        caller_ticket_id: str,
    ) -> Dict:
        if not caller_ticket_id:
            # Only the ticket tells this user's release from a late one for the
            # previous user, whose Pod may already be serving someone else.
            return self._ignore_release(compute_pod, "", "", "no_ticket")

        release_started_ms = int(time.time() * 1000)
        release_started = time.perf_counter()
        provider = self.provider

        try:
            pod = provider.v1.read_namespaced_pod(
                compute_pod,
                provider.namespace,
                _request_timeout=provider.api_request_timeout,
            )
        except Exception as exc:
            if isinstance(exc, ApiException) and exc.status == 404:
                return self._ignore_release(compute_pod, caller_ticket_id, "", "already_released")
            # Without the Pod's state there is no telling whose it is now, so
            # nothing is touched.
            logger.error(
                "[Failed] operation=return compute_pod=%s ticket_id=%s reason=%r",
                compute_pod,
                caller_ticket_id,
                str(exc),
            )
            return {"status": "error", "message": str(exc)}

        metadata = pod.metadata
        labels = metadata.labels or {}
        annotations = metadata.annotations or {}
        pod_ticket = (annotations.get(provider.ANNOTATION_ALLOCATION_TICKET) or "").strip()
        target_ticket = caller_ticket_id

        context = dict(request_context or {})
        # An empty dict means the caller already looked and found nothing.
        if request_context is None:
            try:
                context = self.tickets.get_assigned_request_context(compute_pod) or {}
            except QueueUnavailableError as exc:
                logger.debug(
                    "[AssignedContextUnavailable] compute_pod=%s reason=%r",
                    compute_pod,
                    str(exc),
                )
        # The context is keyed by Pod name, so it may already describe the next user.
        context_ticket = (context.get("ticket_id") or "").strip()
        if context_ticket and context_ticket != target_ticket:
            context = {}
        ticket = self._ticket_by_id(compute_pod, target_ticket)
        if ticket:
            ticket_format.set_ticket_context(ticket)
        elif context.get("request_label"):
            set_request_label(context.get("request_label"))

        if (
            getattr(metadata, "deletion_timestamp", None)
            or labels.get(provider.LABEL_STATUS) != provider.STATUS_ASSIGNED
        ):
            return self._ignore_release(compute_pod, target_ticket, pod_ticket, "not_assigned")
        if caller_ticket_id != pod_ticket:
            # A late release for the previous user must not touch the current one.
            return self._ignore_release(compute_pod, caller_ticket_id, pod_ticket, "stale_ticket")

        compute_type = self.queues.normalize_compute_type(
            (ticket or {}).get("compute_type")
            or context.get("compute_type")
            or labels.get(provider.LABEL_COMPUTE_TYPE)
        )
        compute_pod_ip = getattr(pod.status, "pod_ip", None) or ""
        reuse_count = safe_int(annotations.get(provider.ANNOTATION_REUSE_COUNT), 0)
        assigned_at_ms = 0
        if ticket:
            assigned_at_ms = ticket_format.datetime_to_epoch_ms(ticket.get("assigned_at"))
        if not assigned_at_ms:
            assigned_at_ms = safe_int(context.get("assigned_at_ms"), 0)
        session_ms = max(0, release_started_ms - assigned_at_ms) if assigned_at_ms else None

        def _finish(outcome: str, **fields) -> Dict:
            ticket_format.log_queue_event(
                "info",
                "Released",
                ticket,
                component="SUCCESS",
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                outcome=outcome,
                session_ms=session_ms,
                release_ms=_elapsed_ms(release_started),
                **fields,
            )
            self._on_released(compute_type)
            return {
                "status": "success",
                "outcome": outcome,
                "message": f"Released ({outcome}): {compute_pod}",
            }

        def _failed(operation: str, exc: Exception) -> Dict:
            logger.error(
                "[Failed] operation=%s compute_pod=%s ticket_id=%s reason=%r",
                operation,
                compute_pod,
                target_ticket,
                str(exc),
            )
            return {"status": "error", "message": str(exc)}

        lock_started = time.monotonic()
        try:
            lock_token = self.tickets.acquire_release_lock(target_ticket)
        except QueueUnavailableError as exc:
            # Without the lock a duplicate could scrub the Pod after it has
            # gone to the next user, so nothing is touched.
            return _failed("return_lock", exc)
        if not lock_token:
            return self._ignore_release(compute_pod, target_ticket, pod_ticket, "duplicate")

        def _unlock() -> None:
            # Only while the Pod is still assigned to this ticket, neither
            # relabelled nor deleted: the client already has its 202, so a later
            # release (the agent's fallback, cleanup) is the only retry left and
            # must not be turned away as a duplicate.
            self._drop_release_lock_best_effort(target_ticket, lock_token)

        def _delete(outcome: str, clear_context: bool = True, **fields) -> Dict:
            # Still assigned to this ticket under its lock, so deleting cannot
            # reach anyone else.
            try:
                deleted = self._delete_pod(compute_pod)
            except Exception as delete_exc:
                _unlock()
                return _failed("return_delete", delete_exc)
            if clear_context:
                self._clear_owned_context_best_effort(compute_pod, target_ticket)
            if not deleted:
                return self._ignore_release(compute_pod, target_ticket, pod_ticket, "already_released")
            return _finish(outcome, **fields)

        scrub_started = time.perf_counter()
        try:
            if not compute_pod_ip:
                raise ComputeAgentError("Compute Pod has no IP")
            with ComputeAgent(compute_pod_ip) as agent:
                scrub_result = agent.scrub(ticket_id=target_ticket)
        except AgentTicketMismatch as exc:
            # The agent already serves another ticket, so the labels read above
            # are behind and the session it holds is not this user's to end.
            _unlock()
            return self._ignore_release(
                compute_pod,
                target_ticket,
                pod_ticket,
                "agent_ticket_mismatch",
                agent_ticket_id=exc.current_ticket_id,
            )
        except Exception as exc:
            scrub_ms = _elapsed_ms(scrub_started)
            logger.warning(
                "[Warning] operation=compute_scrub compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )
            return _delete("deleted_scrub_failed", scrub_ms=scrub_ms, reuse_count=reuse_count)
        # scrub_ms is always the controller's round trip, so it compares across
        # outcomes; the agent's own figure leaves out the HTTP around it.
        scrub_fields = {
            "scrub_ms": _elapsed_ms(scrub_started),
            "agent_scrub_ms": safe_int(scrub_result.get("scrub_ms"), None),
        }

        # Cleared before the relabel: once the Pod is available it can be
        # reassigned at once, and the next user's keys sit under the same name.
        try:
            self.tickets.clear_assigned_request_context_if_owner(compute_pod, target_ticket)
        except QueueUnavailableError as exc:
            logger.warning(
                "[Warning] operation=assigned_context_cleanup compute_pod=%s status=failed reason=%r",
                compute_pod,
                str(exc),
            )
            return _delete("deleted_return_failed", clear_context=False, reuse_count=reuse_count, **scrub_fields)

        if self._lock_too_old_to_relabel(lock_started):
            # Deleting stays safe even if the sweep deletes too; relabelling
            # could hand the Pod to the next user just before that delete lands.
            logger.warning(
                "[Warning] operation=compute_return compute_pod=%s reason=%r",
                compute_pod,
                "release lock near its stale age",
            )
            return _delete("deleted_return_failed", reuse_count=reuse_count, **scrub_fields)

        available_since = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        relabel_started = time.perf_counter()
        try:
            provider.return_pod(compute_pod, target_ticket, reuse_count + 1, available_since)
        except Exception as exc:
            relabel_ms = _elapsed_ms(relabel_started)
            logger.warning(
                "[Warning] operation=compute_return compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )
            # The patch may have landed or the Pod may have moved on, so it is
            # deleted only if it still carries this ticket. No retry: a second
            # relabel could land after the Pod went to the next user.
            try:
                state = self._failed_return_state(compute_pod, target_ticket, available_since)
            except Exception as settle_exc:
                # Whether the relabel landed is unknown, so the lock stays and
                # cleanup's stale-lock sweep deletes the Pod if it is still ours.
                return _failed("return_settle", settle_exc)
            # Decided by what the Pod is now, not by how the patch failed: a
            # conflict can still leave the Pod ours to delete.
            fields = dict(scrub_fields, relabel_ms=relabel_ms)
            if state == "ours":
                return _delete("deleted_return_failed", reuse_count=reuse_count, **fields)
            if state == "returned":
                self._set_compute_available_best_effort(compute_pod, available_since)
                self._claim_replica_slot_for_return(compute_type)
                return _finish("returned", reuse_count=reuse_count + 1, **fields)
            if state == "gone":
                return self._ignore_release(compute_pod, target_ticket, pod_ticket, "already_released")
            return _finish("lost_external", reuse_count=reuse_count, **fields)

        relabel_ms = _elapsed_ms(relabel_started)
        self._set_compute_available_best_effort(compute_pod, available_since)
        self._claim_replica_slot_for_return(compute_type)
        return _finish("returned", relabel_ms=relabel_ms, reuse_count=reuse_count + 1, **scrub_fields)

    def _claim_replica_slot_for_return(self, compute_type: str) -> None:
        """되돌린 파드 한 개를 replicas 에 반영한다. 실패해도 반납은 성공으로 끝낸다.

        되돌린 파드는 compute-status 가 available 로 바뀌면서 Deployment selector 안으로
        다시 들어온다. replicas 를 그대로 두면 ReplicaSet 이 "지시보다 하나 많다" 고 보고
        방금 되돌린(정리까지 끝낸) 파드를 지운다. 실측에서 요청 631건에 파드 667개가
        만들어지고 666개가 지워졌다 - 재사용의 이득이 전부 거기서 사라졌다.

        할당기가 할당할 때 그만큼 내려 두므로(allocator._lower_replicas_for_claims), 여기서
        하나 올리면 숫자가 제자리로 온다. 파드를 새로 만드는 것이 아니라 이미 있는 파드를
        숫자에 반영하는 것이라 점유량은 늘지 않는다.

        리컨실러의 목표(수요에 따른 available 수)는 그대로 둔다. 수요가 꺼지면 리컨실러가
        정책대로 줄이고, 그때는 되돌린 파드도 지워진다 - 그것은 버퍼 정책이지 이 경로의
        몫이 아니다.
        """
        try:
            policy = self.queues.get_buffer_policy(compute_type)
            if not policy:
                return
            deployment_name = policy.get("deployment_name")
            if not deployment_name:
                return
            ceiling = int(policy.get("N") or 0)
            for _ in range(3):
                current = int(self.provider.read_deployment_replicas(deployment_name))
                target = current + 1
                if ceiling > 0 and target > ceiling:
                    return
                if self.provider.raise_deployment_replicas(
                    deployment_name,
                    current,
                    target,
                ):
                    logger.info(
                        "[BufferSlotRestored] compute_type=%s replicas=%s->%s N=%s",
                        compute_type,
                        current,
                        target,
                        ceiling,
                    )
                    return
        except Exception as exc:  # noqa: BLE001 - 숫자 보정이 반납을 막지 않는다
            logger.warning(
                "[Warning] operation=buffer_slot_restore compute_type=%s reason=%r",
                compute_type,
                str(exc),
            )

    def _lock_too_old_to_relabel(self, lock_started: float) -> bool:
        """True once cleanup's stale-lock sweep could be taking the lock over.

        The margin covers the relabel's own API timeout and clock skew between
        controllers, since the sweep judges age by the token's wall-clock time.
        """
        stale_seconds = self.release_lock_stale_seconds
        return (
            stale_seconds > 0
            and time.monotonic() - lock_started > stale_seconds - self._RELABEL_MARGIN_SECONDS
        )

    def _ticket_by_id(self, compute_pod: str, ticket_id: str) -> Optional[Dict]:
        """The ticket a release names, looked up by id only.

        The compute-ticket index is keyed by Pod name, and with reuse it may
        already point at the next user's ticket; only a delete of a Pod that
        names no ticket still falls back to it.
        """
        if not ticket_id:
            return None
        try:
            ticket = self.tickets.get_ticket_snapshot(ticket_id)
        except QueueUnavailableError as exc:
            logger.debug(
                "[TicketLookupSkipped] ticket_id=%s compute_pod=%s reason=%r",
                ticket_id,
                compute_pod,
                str(exc),
            )
            return None
        if (
            ticket
            and (ticket.get("compute_pod") or "").strip() == compute_pod
            and str(ticket.get("status") or "").lower() == "assigned"
        ):
            return ticket
        return None

    @staticmethod
    def _ignore_release(
        compute_pod: str,
        ticket_id: str,
        pod_ticket: str,
        reason: str,
        agent_ticket_id: str = "",
    ) -> Dict:
        logger.info(
            "[ReleaseIgnored] compute_pod=%s ticket_id=%s pod_ticket_id=%s reason=%s%s",
            compute_pod,
            ticket_id or "-",
            pod_ticket or "-",
            reason,
            f" agent_ticket_id={agent_ticket_id}" if agent_ticket_id else "",
        )
        return {
            "status": "success",
            "outcome": "ignored",
            "message": f"Release ignored ({reason}): {compute_pod}",
        }

    def _delete_pod(self, compute_pod: str) -> bool:
        """Delete with one retry; False when the Pod was already gone or terminating."""
        for attempt in range(1, self._RELEASE_MAX_ATTEMPTS + 1):
            try:
                return bool(self.provider.release_pod(compute_pod))
            except Exception as exc:
                if attempt >= self._RELEASE_MAX_ATTEMPTS:
                    raise
                logger.debug(
                    "[ReleaseRetry] compute_pod=%s attempt=%s max_attempts=%s reason=%r",
                    compute_pod,
                    attempt,
                    self._RELEASE_MAX_ATTEMPTS,
                    str(exc),
                )

    def _failed_return_state(self, compute_pod: str, ticket_id: str, available_since: str) -> str:
        """Re-read a Pod whose relabel failed.

        Returns "ours" when it is still assigned to ticket_id (the caller
        deletes it), "returned" when the patch landed after all, "gone" when
        the Pod no longer exists, or "left" when it belongs to someone else now.
        """
        try:
            pod = self.provider.v1.read_namespaced_pod(
                compute_pod,
                self.provider.namespace,
                _request_timeout=self.provider.api_request_timeout,
            )
        except ApiException as exc:
            if exc.status == 404:
                return "gone"
            raise
        metadata = pod.metadata
        labels = metadata.labels or {}
        annotations = metadata.annotations or {}
        if (
            not getattr(metadata, "deletion_timestamp", None)
            and labels.get(self.provider.LABEL_STATUS) == self.provider.STATUS_ASSIGNED
            and annotations.get(self.provider.ANNOTATION_ALLOCATION_TICKET) == ticket_id
        ):
            return "ours"
        if annotations.get(self.provider.ANNOTATION_AVAILABLE_SINCE) == available_since:
            return "returned"
        return "left"

    def _set_compute_available_best_effort(self, compute_pod: str, available_since: str) -> None:
        try:
            self.queues.set_compute_available(compute_pod, available_since)
        except QueueUnavailableError as exc:
            logger.debug(
                "[ComputeAvailableSkipped] compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )

    def _clear_owned_context_best_effort(self, compute_pod: str, ticket_id: str) -> None:
        try:
            self.tickets.clear_assigned_request_context_if_owner(compute_pod, ticket_id)
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

    def _drop_release_lock_best_effort(self, ticket_id: str, token: Optional[str]) -> None:
        if not ticket_id or not token:
            return
        try:
            self.tickets.drop_release_lock(ticket_id, token)
        except Exception as exc:
            # Left for cleanup's stale-lock sweep to take over.
            logger.warning(
                "[Warning] operation=release_lock_drop ticket_id=%s status=failed reason=%r",
                ticket_id,
                str(exc),
            )

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
