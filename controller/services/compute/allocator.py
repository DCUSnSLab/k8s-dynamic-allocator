import logging
import threading
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from config import settings

from .. import ticket_format
from .agent_client import ComputeAgent, ComputeAgentError
from .direct_buffer_provider import AllocationClaimLost


try:
    from .. import timing
except ImportError:  # 단위 테스트는 이 파일 하나만 따로 읽어 패키지가 없다
    from contextlib import nullcontext as _timing_nullcontext

    class timing:  # noqa: N801 - 모듈 대역. 계측이 꺼진 것과 같게 동작한다.
        ENABLED = False

        @staticmethod
        def timed(kind):
            return lambda fn: fn

        @staticmethod
        def emit(*args, **kwargs):
            pass

        @staticmethod
        def phase(*args, **kwargs):
            pass

        @staticmethod
        def note(*args, **kwargs):
            pass

        @staticmethod
        def current_purpose():
            return "other"

        @staticmethod
        def current_trigger():
            return "direct"

        @staticmethod
        def lock_acquired(*args, **kwargs):
            pass

        @staticmethod
        def lock_missed(*args, **kwargs):
            pass

        @staticmethod
        def lock_released(*args, **kwargs):
            pass

        @staticmethod
        def set_leader_probe(*args, **kwargs):
            pass

        @staticmethod
        def start(*args, **kwargs):
            pass

        @staticmethod
        def scope(*args, **kwargs):
            return _timing_nullcontext()

        @staticmethod
        def phase_timer():
            return _timing_nullcontext()


logger = logging.getLogger(__name__)


class ComputeAllocator:
    def __init__(self, provider, queues, tickets):
        self.provider = provider
        self.queues = queues
        self.tickets = tickets

    def drain_wait_queue_for_type(
        self,
        compute_type: str,
        recover_stale_ticket: Callable[[Dict], Dict],
    ) -> Dict:
        """계측 포장. KDA_TIMING_LOG 가 꺼져 있으면 원래 경로를 그대로 탄다.

        drain 한 번의 결과(claim, 자리 없음, 락 놓침 등)와 소요를 한 줄로 남긴다.
        """
        if not timing.ENABLED:
            return self._drain_wait_queue_for_type_impl(
                compute_type, recover_stale_ticket
            )
        with timing.scope(purpose="drain"), timing.phase_timer() as timer:
            result = None
            try:
                result = self._drain_wait_queue_for_type_impl(
                    compute_type, recover_stale_ticket
                )
                return result
            finally:
                r = result or {}
                if result is None:
                    outcome = "exception"
                elif r.get("claimed"):
                    outcome = "claimed"
                elif r.get("capacity_blocked"):
                    outcome = r["capacity_blocked"]
                elif r.get("lock_missed"):
                    outcome = "lock_missed"
                elif r.get("lock_lost"):
                    outcome = "lock_lost"
                else:
                    outcome = r.get("empty_reason", "empty")
                timing.emit(
                    "DrainResult",
                    trigger=timing.current_trigger(),
                    compute_type=r.get("compute_type", compute_type),
                    outcome=outcome,
                    claimed=r.get("claimed", 0),
                    assigned=r.get("assigned", 0),
                    stale=r.get("stale_recovered", 0),
                    total_ms=timer.total_ms(),
                    **timer.fields(),
                )

    # ------------------------------------------------------------------
    # 할당. 버퍼 파드는 Deployment 없이 컨트롤러가 직접 만들고 지운다.
    #
    # cold start 의 할당 경로에 "놀고 있는(idle) 파드를 먼저 쓴다" 만 더했다.
    #   - 할당 락 아래에서: 목록 1회, N 여유 계산, 티켓 claim, idle 파드 할당(조건부 PATCH)
    #   - 락 밖에서(티켓마다 스레드): idle 이 없던 티켓은 파드를 만들고, Ready 를 기다려
    #     마운트한다. cold start 와 같은 함수 순서다.
    # 포화에서는 idle 이 없으므로 cold start 와 같은 경로가 된다.

    def _drain_wait_queue_for_type_impl(
        self,
        compute_type: str,
        recover_stale_ticket: Callable[[Dict], Dict],
    ) -> Dict:
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        result = {
            "compute_type": compute_type_value,
            "stale_recovered": 0,
            "claimed": 0,
            "assigned": 0,
            "queued": 0,
            "failed": 0,
            "errors": [],
        }
        # (티켓, idle 파드 이름 또는 None=새로 만듦, 마운트만 남은 확정 티켓 또는 None)
        work: List[Tuple[Dict, Optional[str], Optional[Dict]]] = []
        effective_batch, _mount_concurrency = self._compute_wait_queue_batch_plan()
        lock_token = None
        lock_renewed_at = 0.0
        try:
            lock_token = self.queues.acquire_allocator_lock(compute_type_value)
            if not lock_token:
                result["queued"] += 1
                result["lock_missed"] = True
                return result
            lock_renewed_at = time.monotonic()

            for stale_ticket in self.queues.find_stale_allocating_tickets(compute_type_value):
                recovered = recover_stale_ticket(stale_ticket)
                if recovered["status"] == "requeued":
                    result["stale_recovered"] += 1
                elif recovered["status"] == "failed":
                    result["failed"] += 1
                elif recovered["status"] == "error":
                    result["errors"].append(recovered)

            if not self.queues.has_queued_tickets(compute_type_value):
                return result

            try:
                snapshot = self.provider.snapshot(compute_type_value)
                capacity = self.provider.read_capacity(compute_type_value)
                active = snapshot["active_names"]
                # claim 을 먼저, 생성 중 목록을 나중에 읽는다. 요청 파드는 (이름을 티켓에
                # 적음 → 생성 중 목록에 올림 → 소유 재확인 → 생성) 순서로 만들어지므로,
                # 이 순서로 읽으면 그사이 취소가 끼어도 둘 중 하나에는 반드시 잡힌다.
                missing_claims = self.queues.allocating_missing_pods(active, compute_type_value)
                # 목록에 보인 이름(지우는 중 포함)은 목록 쪽에서 이미 셌거나 자리를
                # 돌려주는 중이다. 리더 조정의 계산과 같게 둔다.
                listed = {pod["name"] for pod in snapshot["pods"]}
                pending_names = {
                    name
                    for name in self.queues.pending_creates(
                        compute_type_value,
                        float(getattr(settings, "WARM_BUFFER_PENDING_CREATE_SECONDS", 60.0)),
                    )
                    if name not in listed
                }
                # 같은 요청 파드가 claim 과 생성 중 목록 양쪽에 있으면 한 번만 센다.
                pending_tickets = sum(
                    1 for name in missing_claims if not name or name not in pending_names
                )
            except Exception as exc:
                # 상한을 확인할 수 없으면 이번에는 하나도 claim 하지 않는다(cold start 와
                # 같다). 1초 뒤 작업자가 다시 온다. 확인 못 한 채 한 묶음을 내주면 상한이
                # 꺼진다.
                logger.warning(
                    "[Warning] operation=direct_capacity_check compute_type=%s reason=%r",
                    compute_type_value,
                    str(exc),
                )
                result["queued"] += 1
                result["capacity_blocked"] = "capacity_unknown"
                return result

            # Ready 인 idle 먼저, 그다음 오래된(먼저 부팅을 시작한) 것부터. Ready 였다가
            # NotReady 가 된 파드(에이전트가 죽음)는 내주지 않는다 - 정리가 곧 지운다.
            # 옛 템플릿 세대(배포 전 이미지)도 내주지 않는다 - 리더가 교체한다.
            current_hash = self._current_template_hash(compute_type_value)
            idle = sorted(
                (
                    pod for pod in snapshot["idle"]
                    if (pod["ready"] or pod.get("not_ready_since") is None)
                    and (not current_hash or pod.get("template_hash") == current_hash)
                ),
                key=lambda pod: (
                    not pod["ready"],
                    pod["created"].timestamp() if pod.get("created") else float("inf"),
                ),
            )
            # N 이 없으면 하나도 만들지 않는다(설정 누락으로 상한 없는 run 이 되지 않게).
            new_slots = max(
                0,
                int(capacity or 0) - len(active) - pending_tickets - len(pending_names),
            )
            limit = min(effective_batch, len(idle) + new_slots)
            if limit <= 0:
                result["queued"] += 1
                result["capacity_blocked"] = "capacity"
                return result

            idle_iter = iter(idle)
            renew_interval = max(
                1.0, float(getattr(settings, "WAIT_QUEUE_LOCK_RENEW_SECONDS", 20))
            )
            for _ in range(limit):
                if time.monotonic() - lock_renewed_at >= renew_interval:
                    if not self.queues.renew_allocator_lock(compute_type_value, lock_token):
                        result["lock_lost"] = True
                        break
                    lock_renewed_at = time.monotonic()

                ticket = None
                try:
                    ticket = self.queues.claim_next_ticket(
                        compute_type_value,
                        worker_id=self.queues.worker_identity,
                    )
                    if not ticket:
                        break

                    claimed_pod, committed, ticket_alive = self._claim_idle_for_ticket(
                        ticket, idle_iter
                    )
                    if not ticket_alive:
                        # 묶는 사이 취소됐다. 이 티켓 몫으로 새 파드를 만들지 않는다.
                        ticket = None
                        continue
                    if claimed_pod:
                        work.append((ticket, claimed_pod, committed))
                    elif new_slots > 0:
                        # 자리를 락 안에서 생성 중 목록으로 옮겨 둔다. 스레드가 늦게 떠서
                        # 그사이 claim 이 만료돼도(만료된 claim 은 세지 않는다) 자리가
                        # 비지 않는다. 스레드가 소유를 확인하지 못하면 이 이름을 지운다.
                        self.queues.add_pending_create(
                            compute_type_value, self.provider.request_pod_name(ticket), "request"
                        )
                        new_slots -= 1
                        work.append((ticket, None, None))
                    else:
                        # idle 이 다 남의 것이 됐고 새로 만들 자리도 없다. 순서를 지킨 채
                        # 돌려놓는다.
                        self.tickets.requeue_ticket(
                            ticket["ticket_id"],
                            reason="No idle compute pod and no capacity",
                            increment_retry=False,
                            claim_token=ticket.get("claim_token"),
                        )
                        result["queued"] += 1
                        break
                    result["claimed"] += 1
                except Exception as exc:
                    # 이미 claim 한 앞의 일감은 아래에서 그대로 시작한다. 이 티켓만
                    # 돌려놓는다(idle PATCH 결과를 모르는 경우 그 파드는 claim 토큰이
                    # 어긋나 정리가 회수한다).
                    logger.warning(
                        "[Warning] operation=direct_claim compute_type=%s ticket_id=%s reason=%r",
                        compute_type_value,
                        (ticket or {}).get("ticket_id"),
                        str(exc),
                    )
                    result["errors"].append({"status": "error", "message": str(exc)})
                    if ticket:
                        try:
                            self.tickets.requeue_ticket(
                                ticket["ticket_id"],
                                reason=f"Claim failed: {exc}",
                                increment_retry=False,
                                claim_token=ticket.get("claim_token"),
                            )
                        except Exception:
                            pass
                    break
        finally:
            if lock_token:
                try:
                    self.queues.release_allocator_lock(compute_type_value, lock_token)
                except Exception as exc:
                    if not work:
                        raise
                    logger.warning(
                        "[Warning] operation=allocator_lock_release compute_type=%s "
                        "claims=%s reason=%r",
                        compute_type_value,
                        len(work),
                        str(exc),
                    )

        for ticket, idle_pod, committed in work:
            threading.Thread(
                target=self._safe_execute_direct_ticket,
                args=(ticket, compute_type_value, idle_pod, committed),
                name=f"direct-{str(ticket.get('ticket_short') or ticket.get('ticket_id') or '')[:10]}",
                daemon=True,
            ).start()
        if not work:
            result["queued"] += 1
        return result

    def _current_template_hash(self, compute_type_value: str) -> str:
        reader = getattr(self.provider, "template_hash", None)
        if reader is None:
            return ""
        try:
            return reader(compute_type_value) or ""
        except Exception:
            # 세대를 모르면 거르지 않는다(템플릿을 못 읽으면 어차피 새로 만들 수도 없다).
            return ""

    def _claim_idle_for_ticket(
        self, ticket: Dict, idle_iter
    ) -> Tuple[Optional[str], Optional[Dict], bool]:
        """남은 idle 파드를 차례로 이 티켓에 묶어 본다.

        (묶은 파드 이름 또는 None, 마운트만 남은 확정 티켓 또는 None, 티켓이 살아 있나).
        PATCH 가 resourceVersion 을 test 하므로, 그사이 다른 쪽이 가져갔거나 리더가
        지우기 시작한 파드는 실패하고 다음 것으로 넘어간다.

        Ready 인 파드는 방금 읽은 목록에 IP 와 Ready 시각이 있으므로 여기서 바로 할당을
        확정한다(Deployment 버퍼와 같다). 락 밖에서 파드를 다시 읽는 왕복이 없다.
        """
        ticket_id = ticket["ticket_id"]
        claim_token = ticket.get("claim_token") or ""
        user_pod = ticket.get("user_pod") or "unknown"
        for pod in idle_iter:
            if not self.provider.claim_idle_pod(
                pod["name"],
                pod.get("resource_version") or "",
                user_pod,
                ticket_id,
                claim_token,
                expected_annotations=pod.get("annotations"),
            ):
                continue
            if pod["ready"] and pod.get("ip"):
                ready_at = pod.get("ready_at")
                recorded = self.tickets.mark_allocating(
                    ticket_id,
                    compute_pod=pod["name"],
                    compute_pod_ip=pod["ip"],
                    claimed_by=ticket.get("claimed_by"),
                    claim_token=claim_token,
                    compute_ready_at=ready_at,
                    # 되돌린 파드는 Ready 시각이 첫 부팅이다. 대기하던 요청이 본 것은 되돌린 시각이다.
                    compute_available_at=pod.get("available_at") or ready_at,
                )
                committed = recorded
            else:
                recorded = self.tickets.record_allocating_pod(
                    ticket_id,
                    compute_pod=pod["name"],
                    claim_token=claim_token,
                )
                committed = None
            if not self._owns_claim(recorded, claim_token):
                # 티켓이 그사이 취소됐다. 묶은 파드는 버린다(요청 하나에 하나만 쓴다).
                try:
                    self.provider.release_pod(pod["name"])
                except Exception:
                    pass
                return None, None, False
            return pod["name"], committed, True
        return None, None, True

    def _safe_execute_direct_ticket(
        self,
        ticket: Dict,
        compute_type_value: str,
        idle_pod: Optional[str],
        committed: Optional[Dict] = None,
    ) -> Dict:
        if committed is None:
            try:
                committed, response = self._provision_direct_ticket(ticket, idle_pod)
            except Exception as exc:
                logger.exception(
                    "[Failed] operation=provision_direct_ticket ticket_id=%s compute_type=%s reason=%r",
                    ticket.get("ticket_id"),
                    compute_type_value,
                    str(exc),
                )
                return self._handle_provision_failure(ticket=ticket, compute_pod=idle_pod or "", exc=exc)
            if committed is None:
                return response  # 이미 정리를 마친 응답(소유를 잃음, 재시도 등)
        # 마운트 단계. 여기서 난 예외로 파드를 지우지 않는다 - 할당이 확정된 뒤일 수 있다
        # (mark_assigned 뒤 문맥 기록 실패 등). 남은 정리는 stale 회수와 고아 정리가 한다.
        try:
            return self._execute_allocated_ticket(committed)
        except Exception as exc:
            logger.exception(
                "[Failed] operation=execute_allocated_ticket ticket_id=%s compute_type=%s reason=%r",
                ticket.get("ticket_id"),
                compute_type_value,
                str(exc),
            )
            return {
                "ticket_id": ticket.get("ticket_id", ""),
                "compute_type": compute_type_value,
                "status": "error",
                "message": str(exc),
            }

    def _provision_direct_ticket(
        self, ticket: Dict, idle_pod: Optional[str]
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """파드를 마련해(새로 만들거나 부팅 중인 idle 을 기다려) 할당을 확정한다.

        (확정된 티켓, None) 이면 호출자가 마운트한다. (None, 응답) 이면 이미 정리를
        마쳤다(소유를 잃음, 재시도로 돌려보냄 등).
        """
        ticket_id = ticket.get("ticket_id", "")
        claim_token = ticket.get("claim_token") or ""
        compute_pod = idle_pod or ""

        try:
            if not compute_pod:
                compute_pod = self._reserve_request_pod(ticket)
                try:
                    self.provider.create_pod_for_ticket(ticket, compute_pod)
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    if isinstance(status, int) and 400 <= status < 500:
                        # 확정된 거절이라 만들어지지 않았다. 생성 중 목록의 자리를 돌려준다.
                        # 응답 없음·5xx 는 서버에 반영됐을 수 있어 그대로 둔다(상한이 지나면
                        # 빠진다). 409(이미 있음)는 공급자가 상태 없는 오류로 바꿔 올린다.
                        try:
                            self.queues.remove_pending_creates(
                                ticket.get("compute_type"), [compute_pod]
                            )
                        except Exception:
                            pass
                    raise
            ready_pod = self.provider.wait_pod_ready(
                compute_pod,
                keep_claim=lambda: self.tickets.extend_allocation_deadline(ticket_id, claim_token),
                # 미리 만든 파드는 이미 있다. 사라졌으면 기다리지 않는다.
                must_exist=bool(idle_pod),
            )
            compute_pod_ip = getattr(ready_pod.status, "pod_ip", "") or self.provider.get_pod_ip(compute_pod) or ""
            if not compute_pod_ip:
                raise RuntimeError("Compute IP unavailable after pod Ready")

            compute_ready_at = self.provider._pod_ready_at(ready_pod)
            committed = self.tickets.mark_allocating(
                ticket_id,
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                claimed_by=ticket.get("claimed_by"),
                claim_token=claim_token,
                compute_ready_at=compute_ready_at,
                compute_available_at=compute_ready_at if idle_pod else None,
            )
            if not self._owns_claim(committed, claim_token):
                try:
                    self.provider.release_pod(compute_pod)
                except Exception:
                    pass
                current = self.tickets.get_ticket(ticket_id)
                return None, ticket_format.ticket_response(
                    current or ticket,
                    "Ticket no longer owns the allocation",
                )
            return committed, None
        except AllocationClaimLost:
            # 지금 티켓을 쥔 쪽(취소, 또는 claim 만료 뒤 다른 작업자)이 뒤를 맡는다.
            # 우리 파드만 버린다.
            try:
                self.provider.release_pod(compute_pod)
            except Exception:
                pass
            current = self.tickets.get_ticket(ticket_id)
            return None, ticket_format.ticket_response(
                current or ticket,
                "Ticket no longer owns the allocation",
            )
        except Exception as exc:
            return None, self._handle_provision_failure(ticket=ticket, compute_pod=compute_pod, exc=exc)

    def _reserve_request_pod(self, ticket: Dict) -> str:
        """요청 파드의 자리를 생성 중 목록에 옮겨 놓고 이름을 돌려준다.

        claim 은 취소되거나 만료되는 순간 N 계산에서 빠진다. 그 뒤에 이 스레드가 파드를
        만들면 그 자리를 다른 claim 이 이미 받아 N 을 넘는다(가짜 클러스터 동시 실행
        테스트에서 재현했다. cold start 도 같은 틈이 있다). 그래서 이름을 티켓과 생성 중
        목록에 먼저 올리고, 소유를 다시 확인한 뒤에 만든다. 확인 뒤의 취소는 생성 중
        목록이 자리를 쥐고 있고, 확인 전의 취소는 여기서 멈춘다.
        """
        ticket_id = ticket.get("ticket_id", "")
        claim_token = ticket.get("claim_token") or ""
        compute_type = ticket.get("compute_type")
        pod_name = self.provider.request_pod_name(ticket)
        recorded = self.tickets.record_allocating_pod(
            ticket_id,
            compute_pod=pod_name,
            claim_token=claim_token,
        )
        current = None
        if self._owns_claim(recorded, claim_token):
            # drain 이 락 안에서 올린 예약을 갱신해 상한(생성 응답을 기다릴 수 있는 시간)을
            # 지금부터 센다. 사라진 예약은 되살리지 않는다(XX).
            alive = pod_name in self.queues.refresh_pending_creates(compute_type, [pod_name], "request")
            current = self.tickets.get_ticket(ticket_id)
            if self._owns_claim(current, claim_token):
                if alive:
                    return pod_name
                # 예약이 사라져도 claim 이 아직 유효하면 claim 이 자리를 쥐고 있다(파드 없는
                # claim 으로 센다). 그때는 예약을 다시 올려 확인 뒤의 취소에 대비한다.
                if not self.queues.is_allocation_stale(current):
                    self.queues.add_pending_create(compute_type, pod_name, "request")
                    return pod_name
        # 만들지 않는다. drain 이 잡아 둔 자리도 돌려준다.
        self.queues.remove_pending_creates(compute_type, [pod_name])
        if self._owns_claim(current, claim_token):
            # 예약도 claim 도 자리를 보장하지 못한다. 순서를 지킨 채 다시 줄 세운다.
            self.tickets.requeue_ticket(
                ticket_id,
                reason="Creation reservation expired before the pod was created",
                increment_retry=False,
                claim_token=claim_token,
            )
        raise AllocationClaimLost(f"Ticket lost its claim before creating {pod_name}")

    @staticmethod
    def _owns_claim(ticket: Optional[Dict], claim_token: str) -> bool:
        return (
            bool(ticket)
            and ticket.get("status") == "allocating"
            and (not claim_token or ticket.get("claim_token") == claim_token)
        )

    def _handle_provision_failure(
        self,
        *,
        ticket: Dict,
        compute_pod: str,
        exc: Exception,
    ) -> Dict:
        """파드를 만들거나 Ready 를 기다리다 실패했다(cold start 와 같다).

        파드는 지우고 티켓은 재시도 예산 안에서 큐로 돌려보낸다. 미리 만든(idle) 파드가
        부팅 중에 죽은 경우도 여기로 온다 - 요청은 실패시키지 않고 다시 줄 세운다.
        """
        ticket_id = ticket.get("ticket_id", "")
        claim_token = ticket.get("claim_token") or ""
        reason = f"Compute provisioning failed: {exc}"

        current = self.tickets.get_ticket(ticket_id)
        in_use = (
            bool(current)
            and str(current.get("status") or "").lower() == "assigned"
            and (current.get("compute_pod") or "") == compute_pod
        )
        if compute_pod and not in_use:
            try:
                self.provider.release_pod(compute_pod)
            except Exception as release_exc:
                logger.warning(
                    "[Warning] operation=release_after_provision_error ticket_id=%s compute_pod=%s reason=%r",
                    ticket_id,
                    compute_pod,
                    str(release_exc),
                )

        if not current:
            return {
                "ticket_id": ticket_id,
                "compute_type": self.queues.normalize_compute_type(ticket.get("compute_type")),
                "status": "failed",
                "message": reason,
            }

        current_status = str(current.get("status") or "").lower()
        if current_status in {"assigned", "failed", "cancelled"}:
            return ticket_format.ticket_response(current, reason)

        if int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.queues.max_retries):
            requeued = self.tickets.requeue_ticket(
                ticket_id,
                reason=reason,
                increment_retry=True,
                claim_token=claim_token,
            )
            requeued_ticket = requeued if requeued and requeued.get("status") == "queued" else self.tickets.get_ticket(ticket_id)
            if requeued_ticket:
                ticket_format.log_queue_event(
                    "debug",
                    "Requeued",
                    requeued_ticket,
                    include_ticket_fields=(),
                    compute_pod=compute_pod,
                    compute_pod_ip=requeued_ticket.get("compute_pod_ip"),
                    retry_count=requeued_ticket.get("retry_count"),
                    reason=reason,
                )
            return ticket_format.ticket_response(requeued_ticket, reason)

        failed = self.tickets.mark_failed(ticket_id, reason, claim_token=claim_token)
        failed_ticket = failed if failed and failed.get("status") == "failed" else self.tickets.get_ticket(ticket_id)
        if failed_ticket:
            ticket_format.log_queue_event(
                "info",
                "Failed",
                failed_ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=failed_ticket.get("compute_pod_ip"),
                retry_count=failed_ticket.get("retry_count"),
                reason=reason,
            )
        return ticket_format.ticket_response(failed_ticket, reason)

    def _compute_wait_queue_batch_plan(self) -> Tuple[int, int]:
        """Return (effective_batch, mount_concurrency) clamped against TTL."""
        configured_batch = max(1, int(getattr(settings, "WAIT_QUEUE_BATCH_LIMIT", 10)))
        configured_concurrency = max(
            1,
            int(getattr(settings, "WAIT_QUEUE_MOUNT_CONCURRENCY", configured_batch)),
        )

        mount_timeout = max(1.0, float(settings.COMPUTE_AGENT_MOUNT_TIMEOUT_SECONDS))
        ttl = max(1.0, float(settings.WAIT_QUEUE_ALLOCATING_TTL_SECONDS))
        worker_interval = max(0.0, float(settings.WAIT_QUEUE_WORKER_INTERVAL_SECONDS))

        if ttl <= mount_timeout:
            logger.warning(
                "[Warning] operation=queue_batch_plan allocating_ttl_seconds=%s "
                "mount_timeout_seconds=%s reason=%r",
                ttl,
                mount_timeout,
                "allocating TTL is not greater than compute mount timeout",
            )

        usable_ttl = max(1.0, ttl - max(2.0, worker_interval))
        waves = max(1, int(usable_ttl // mount_timeout))

        safe_ceiling = max(1, waves * configured_concurrency)
        effective_batch = min(configured_batch, safe_ceiling)
        effective_concurrency = min(configured_concurrency, effective_batch)
        return effective_batch, effective_concurrency

    def _execute_allocated_ticket(self, ticket: Dict) -> Dict:
        ticket_id = ticket.get("ticket_id", "")
        compute_type = self.queues.normalize_compute_type(ticket.get("compute_type"))
        compute_pod = ticket.get("compute_pod", "")
        compute_pod_ip = ticket.get("compute_pod_ip", "")
        user_pod = ticket.get("user_pod") or ""
        user_pod_ip = ticket.get("user_pod_ip") or ""
        command = ticket.get("command") or ""
        claim_token = ticket.get("claim_token") or ""

        if not compute_pod or not user_pod_ip:
            if compute_pod and not self._release_before_ticket_transition(
                compute_pod,
                ticket_id,
                "release_missing_allocation_context",
            ):
                return {
                    "ticket_id": ticket_id,
                    "compute_type": compute_type,
                    "status": "error",
                    "message": (
                        "Missing allocation context and compute Pod cleanup "
                        "is pending"
                    ),
                }
            failed = self.tickets.mark_failed(ticket_id, "Missing compute pod or user pod context", claim_token=claim_token)
            ticket_format.log_queue_event(
                "info",
                "Failed",
                failed or ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                retry_count=(failed or ticket).get("retry_count"),
                reason="Missing compute pod or user pod context",
            )
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "failed",
                "message": "Missing compute pod or user pod context",
            }

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.provider.release_pod(compute_pod)
            except Exception:
                pass
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Ticket no longer owns the allocation",
            )
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "queued",
                "message": "Ticket no longer owns the allocation",
            }

        try:
            with ComputeAgent(compute_pod_ip) as agent:
                agent.mount(user_pod_ip, command, user_pod, ticket_id=ticket_id)
        except ComputeAgentError as exc:
            ticket_format.log_queue_event(
                "warning",
                "Warning",
                ticket,
                include_ticket_fields=(),
                operation="mount",
                compute_pod=compute_pod,
                reason=str(exc),
            )
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                compute_pod=compute_pod,
                claim_token=claim_token,
                exc=exc,
            )
        except Exception as exc:
            ticket_format.set_ticket_context(ticket)
            logger.exception(
                "[Failed] operation=mount error_type=unexpected compute_pod=%s reason=%r",
                compute_pod,
                str(exc),
            )
            return self._handle_mount_failure(
                ticket_id=ticket_id,
                compute_pod=compute_pod,
                claim_token=claim_token,
                exc=exc,
            )

        current = self.tickets.get_ticket(ticket_id)
        if not current or current.get("status") != "allocating" or current.get("claim_token") != claim_token:
            try:
                self.provider.release_pod(compute_pod)
            except Exception:
                pass
            return {
                "ticket_id": ticket_id,
                "compute_type": compute_type,
                "status": "queued",
                "message": "Ticket was cancelled before commit",
            }

        committed = self.tickets.mark_assigned(ticket_id, compute_pod, compute_pod_ip, claim_token=claim_token)
        if committed and committed.get("status") == "assigned":
            assigned_context = ticket_format.assigned_request_context(committed)
            self.tickets.set_assigned_request_context(compute_pod, assigned_context)
            ticket_format.log_queue_event(
                "info",
                "Assigned",
                committed,
                include_ticket_fields=(),
                claimed_by=committed.get("claimed_by"),
                compute_pod=compute_pod,
                compute_pod_ip=compute_pod_ip,
                retry_count=committed.get("retry_count"),
                **ticket_format.assignment_timing_fields(committed),
            )
            response = ticket_format.ticket_response(committed, "Command assigned to compute pod")
            response.update(
                {
                    "user_pod": user_pod,
                    "user_pod_ip": user_pod_ip,
                    "command": command,
                    "submitted_at": datetime.now().isoformat(),
                }
            )
            return response

        try:
            self.provider.release_pod(compute_pod)
        except Exception:
            pass
        current = self.tickets.get_ticket(ticket_id)
        if current:
            ticket_format.log_queue_event(
                "debug",
                "Requeued",
                current,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                reason="Ticket no longer owns the allocation",
            )
        return ticket_format.ticket_response(current or ticket, "Ticket no longer owns the allocation")

    def _handle_mount_failure(
        self,
        ticket_id: str,
        compute_pod: str,
        claim_token: str,
        exc: Exception,
    ) -> Dict:
        reason = f"Mount failed: {exc}"
        if not self._release_before_ticket_transition(
            compute_pod,
            ticket_id,
            "release_after_mount_error",
        ):
            return {
                "ticket_id": ticket_id,
                "status": "error",
                "message": (
                    f"{reason}; compute Pod cleanup is pending before retry"
                ),
            }

        current = self.tickets.get_ticket(ticket_id)
        if current and int(current.get("retry_count") or 0) < int(current.get("max_retries") or self.queues.max_retries):
            requeued = self.tickets.requeue_ticket(
                ticket_id,
                reason=reason,
                increment_retry=True,
                claim_token=claim_token,
            )
            requeued_ticket = requeued if requeued and requeued.get("status") == "queued" else self.tickets.get_ticket(ticket_id)
            if requeued_ticket:
                ticket_format.log_queue_event(
                    "debug",
                    "Requeued",
                    requeued_ticket,
                    include_ticket_fields=(),
                    compute_pod=compute_pod,
                    compute_pod_ip=requeued_ticket.get("compute_pod_ip"),
                    retry_count=requeued_ticket.get("retry_count"),
                    reason=reason,
                )
            return ticket_format.ticket_response(requeued_ticket, str(exc))

        failed = self.tickets.mark_failed(ticket_id, reason, claim_token=claim_token)
        failed_ticket = failed if failed and failed.get("status") == "failed" else self.tickets.get_ticket(ticket_id)
        if failed_ticket:
            ticket_format.log_queue_event(
                "info",
                "Failed",
                failed_ticket,
                include_ticket_fields=(),
                compute_pod=compute_pod,
                compute_pod_ip=failed_ticket.get("compute_pod_ip"),
                retry_count=failed_ticket.get("retry_count"),
                reason=reason,
            )
        return ticket_format.ticket_response(failed_ticket, str(exc))

    def _release_before_ticket_transition(
        self,
        compute_pod: str,
        ticket_id: str,
        operation: str,
    ) -> bool:
        if not compute_pod:
            return True
        try:
            self.provider.release_pod(compute_pod)
            return True
        except Exception as exc:
            logger.warning(
                "[Warning] operation=%s ticket_id=%s compute_pod=%s reason=%r",
                operation,
                ticket_id,
                compute_pod,
                str(exc),
            )
            return False
