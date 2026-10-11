"""리더 전용: 놀고 있는(idle) 파드 수를 버퍼 목표에 맞춘다. Deployment 는 쓰지 않는다.

목표는 BufferPolicy(동적 R, 축소 쿨다운)가 정한다.

  desired = min(max(R, 수요), N - 받아 간 요청)   수요 = 대기 + 아직 Ready 파드를 못 받은 요청
  목표    = 쿨다운 창 안의 desired 최댓값, 단 지금 남은 자리 이하
  쿨다운  = 관측값(파드 생성 → Ready p90, 목표가 가라앉아 있던 시간 p90)의 큰 쪽

N 계산과 생성 결정, idle 삭제 결정은 할당(drain)과 같은 할당 락 아래에서 한다.
실제 생성·삭제 호출은 락 밖에서 한다:
  - 생성: 이름을 먼저 "생성 중" 목록(Redis)에 올리므로 N 계산에 이미 들어가 있다.
  - 삭제: uid + resourceVersion 전제 조건이라 그사이 할당된 파드는 지워지지 않는다.
포화에서 호출 하나가 0.2~0.5초라 락 안에서 하면 할당이 그만큼 밀린다.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from kubernetes.client.rest import ApiException

from config import settings

from ..queue import QueueUnavailableError
from .buffer_policy import BufferPolicy

logger = logging.getLogger(__name__)


class DirectBufferReconciler:
    def __init__(
        self,
        provider,
        queues,
        on_capacity_available: Optional[Callable[[Optional[str]], None]] = None,
        on_periodic_cleanup: Optional[Callable[[], Dict]] = None,
    ):
        self.provider = provider
        self.queues = queues
        self.on_capacity_available = on_capacity_available
        self.on_periodic_cleanup = on_periodic_cleanup
        self.policy = BufferPolicy(queues)

        self.resync_seconds = max(0.2, float(settings.WARM_BUFFER_RESYNC_SECONDS))
        self.cleanup_seconds = max(1.0, float(settings.BUFFER_RECONCILE_RESYNC_SECONDS))
        self.pending_ttl_seconds = max(1.0, float(settings.WARM_BUFFER_PENDING_CREATE_SECONDS))
        self.lock_ttl_seconds = max(1.0, float(getattr(queues, "lock_ttl_seconds", 60) or 60))
        self.ready_timeout_seconds = max(1.0, float(settings.COLD_START_POD_READY_TIMEOUT_SECONDS))
        # 같은 순간 몰려오는 파드 이벤트를 한 바퀴로 합친다.
        self.debounce_seconds = 0.05
        # 할당 락을 못 잡았을 때 다시 볼 때까지.
        self.lock_retry_seconds = 0.2

        self._condition = threading.Condition()
        self._wake = False
        self._stop_event = threading.Event()
        self._generation = 0
        self._thread: Optional[threading.Thread] = None
        self._maintenance_thread: Optional[threading.Thread] = None
        self._leadership_validator: Callable[[], bool] = lambda: True
        self._status_lock = threading.Lock()
        self._status: Dict[str, Dict] = {}

    # ------------------------------------------------------------------ 수명

    def set_leadership_validator(self, validator: Callable[[], bool]) -> None:
        self._leadership_validator = validator
        self.policy.set_leadership_validator(validator)

    def _still_in_charge(self, generation: Optional[int]) -> bool:
        """이 바퀴의 결정을 계속 집행해도 되나. 메모리 안의 값만 본다(원격 조회 없음).

        리더를 잃었거나 멈추라는 신호가 왔거나 다른 세대가 시작됐으면, 이전 목표를 계속
        집행하지 않는다. 이미 보낸 요청의 결과는 그대로 둔다(예약이 자리를 쥐고 있다).
        """
        if self._stop_event.is_set():
            return False
        if generation is not None and generation != self._generation:
            return False
        return self._is_leader()

    def _is_leader(self) -> bool:
        try:
            return bool(self._leadership_validator())
        except Exception:
            logger.exception("[Failed] operation=leadership_validation")
            return False

    def start(self) -> None:
        with self._condition:
            self._generation += 1
            generation = self._generation
            self._stop_event.clear()
            self._wake = True
            self._thread = threading.Thread(
                target=self._run,
                args=(generation,),
                name="direct-buffer-reconciler",
                daemon=True,
            )
            self._thread.start()
            # 주기 정리(고아 파드 회수 등)는 할당된 파드마다 API 를 부르므로 길다. 조정
            # 스레드에서 돌리면 그동안 idle 보충이 멈춘다.
            self._maintenance_thread = threading.Thread(
                target=self._maintenance_loop,
                args=(generation,),
                name="direct-buffer-maintenance",
                daemon=True,
            )
            self._maintenance_thread.start()
            self._condition.notify_all()
        logger.info("[DirectBufferReconcilerStarted]")

    def stop(self) -> None:
        with self._condition:
            self._generation += 1
            self._stop_event.set()
            self._condition.notify_all()
            threads = [self._thread, self._maintenance_thread]
        for thread in threads:
            if thread and thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=5)
        logger.info("[DirectBufferReconcilerStopped]")

    def request(self, compute_type: Optional[str] = None) -> None:
        del compute_type
        with self._condition:
            self._wake = True
            self._condition.notify_all()

    def on_buffer_event(self, event_type: str, pod, source: str = "watch") -> None:
        """파드 이벤트: 생성 → Ready 표본을 남기고 조정을 깨운다."""
        del event_type, source
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = labels.get(self.provider.LABEL_COMPUTE_TYPE)
        if compute_type:
            if self._is_leader() and self.provider._pod_is_ready(pod):
                # 표본은 리더만 공유 저장소에 쓴다(정책 쪽 규칙과 같다).
                self.policy.note_create_time(
                    self.queues.normalize_compute_type(compute_type),
                    getattr(metadata, "uid", None),
                    getattr(metadata, "creation_timestamp", None),
                    self.provider._pod_ready_at(pod),
                )
            self.request(compute_type)

    def get_status(self) -> Dict[str, Dict]:
        with self._status_lock:
            return {key: dict(value) for key, value in self._status.items()}

    # ------------------------------------------------------------------ 루프

    def _run(self, generation: int) -> None:
        while not self._stop_event.is_set():
            with self._condition:
                if self._generation != generation:
                    return
                if not self._wake:
                    self._condition.wait(timeout=self.resync_seconds)
                if self._generation != generation or self._stop_event.is_set():
                    return
                self._wake = False
            time.sleep(self.debounce_seconds)
            if not self._is_leader():
                continue

            retry_soon = False
            for compute_type in self._compute_types():
                if self._nothing_to_do(compute_type):
                    continue
                try:
                    outcome = self.reconcile_type(compute_type, generation)
                    retry_soon = retry_soon or outcome.get("retry", False)
                except Exception as exc:
                    retry_soon = True
                    logger.exception(
                        "[Failed] operation=direct_buffer_reconcile compute_type=%s reason=%r",
                        compute_type,
                        str(exc),
                    )
            if retry_soon:
                self._wake_after(self.lock_retry_seconds, generation)

    def _maintenance_loop(self, generation: int) -> None:
        # 리더가 되자마자 한 번(남은 Deployment 버퍼를 먼저 내린다), 그다음 주기마다.
        while not self._stop_event.is_set() and self._generation == generation:
            if self._still_in_charge(generation):
                self._periodic_maintenance()
            if self._stop_event.wait(self.cleanup_seconds):
                return

    def _wake_after(self, delay: float, generation: int) -> None:
        def _timer():
            time.sleep(delay)
            with self._condition:
                if self._generation == generation:
                    self._wake = True
                    self._condition.notify_all()

        threading.Thread(target=_timer, name="direct-buffer-retry", daemon=True).start()

    def _periodic_maintenance(self) -> None:
        # 모드 전환 때 이전 리더가 Deployment 버퍼를 다시 올릴 수 있어 주기적으로 확인한다.
        try:
            self.provider.drain_buffer_deployments()
        except Exception as exc:
            logger.warning("[Warning] operation=drain_buffer_deployments reason=%r", str(exc))
        if self.on_periodic_cleanup is not None:
            try:
                self.on_periodic_cleanup()
            except Exception as exc:
                logger.warning("[Warning] operation=direct_buffer_cleanup reason=%r", str(exc))

    def _nothing_to_do(self, compute_type: str) -> bool:
        """포화 구간에서 파드 이벤트마다 할당 락을 잡지 않기 위한 거름망.

        대기 티켓이 있으면 미리 만들지 않고, 직전 바퀴에 idle(생성 중 포함)이 하나도
        없었다면 지울 것도 없다. 이때 이벤트로 깨어난 바퀴는 목록·락·Redis 없이
        건너뛴다(직전 바퀴의 기록만 본다). 직전 바퀴가 resync 주기보다 오래됐으면 거르지
        않는다 - 그사이 큐가 비었을 수 있고, 그래서 보충이 늦어지는 것은 그 주기까지다.
        Deployment 버퍼에서 공급 패스가 포화에 초당 1.3번 락을 0.66초씩 쥐어 락 점유가
        94% 였던 일(10/11)을 되풀이하지 않으려는 것이다.
        """
        ct = self.queues.normalize_compute_type(compute_type)
        with self._status_lock:
            last = self._status.get(ct)
        if not last:
            return False
        if time.time() - float(last.get("updated_at") or 0.0) >= self.resync_seconds:
            return False
        if int(last.get("idle") or 0) + int(last.get("pending_idle") or 0) - int(last.get("deleted") or 0) > 0:
            return False
        return int(last.get("queued") or 0) > 0

    def _compute_types(self) -> List[str]:
        types = set(getattr(self.provider, "_templates_by_type", {}) or {})
        try:
            types.update(self.queues.known_compute_types())
        except QueueUnavailableError:
            pass
        if not types:
            types.add(self.queues.normalize_compute_type(None))
        return sorted(self.queues.normalize_compute_type(t) for t in types)

    # ------------------------------------------------------------------ 한 바퀴

    def reconcile_type(self, compute_type: str, generation: Optional[int] = None) -> Dict:
        ct = self.queues.normalize_compute_type(compute_type)
        lock_token = self.queues.acquire_allocator_lock(ct)
        if not lock_token:
            return {"status": "lock_missed", "retry": True}

        to_create: List[str] = []
        to_delete: List[Dict] = []
        record: Dict = {}
        started = time.monotonic()
        try:
            plan = self._plan(ct)
            record = plan["record"]
            to_delete = plan["delete"]
            if plan["create"]:
                # 락 TTL 의 절반을 넘긴 계획은 버린다(보조 장치). 보장은 아래 등록이 한다 -
                # 락을 아직 쥐고 있을 때만 원자적으로 예약한다.
                if time.monotonic() - started > self.lock_ttl_seconds / 2.0:
                    record["create_aborted"] = "slow_plan"
                elif self.queues.add_pending_creates_if_locked(ct, plan["create"], lock_token):
                    to_create = list(plan["create"])
                else:
                    record["create_aborted"] = "lock_lost"
        finally:
            try:
                self.queues.release_allocator_lock(ct, lock_token)
            except Exception as exc:
                logger.warning(
                    "[Warning] operation=allocator_lock_release compute_type=%s reason=%r",
                    ct,
                    str(exc),
                )

        created = self._create_idle(ct, to_create, generation)
        deleted = self._delete_idle(to_delete, generation)
        record.update({"created": created, "deleted": deleted})
        self._record_status(ct, record)
        if created or deleted or record.get("queued"):
            logger.info(
                "[DirectBufferTarget] compute_type=%s queued=%s in_flight=%s demand=%s desired=%s "
                "target=%s bound_by=%s idle=%s idle_ready=%s pending_idle=%s assigned=%s "
                "pending_tickets=%s active=%s R=%s N=%s cooldown_s=%.1f hold_s=%s created=%s deleted=%s",
                ct,
                record.get("queued"),
                record.get("in_flight"),
                record.get("demand"),
                record.get("desired"),
                record.get("target"),
                record.get("bound_by"),
                record.get("idle"),
                record.get("idle_ready"),
                record.get("pending_idle"),
                record.get("assigned"),
                record.get("pending_tickets"),
                record.get("active"),
                record.get("R"),
                record.get("N"),
                float(record.get("cooldown_s") or 0.0),
                record.get("hold_s"),
                created,
                deleted,
            )
        return {"status": "ok", "record": record, "retry": False}

    def _plan(self, ct: str) -> Dict:
        """할당 락 아래에서 한다. 만들 이름과 지울 파드를 정한다(호출은 하지 않는다).

        Deployment 버퍼와 같은 셈이 되도록 "가는 중인 버퍼" 를 버퍼로 센다. 거기서는
        대기 티켓이 Ready 가 될 버퍼 파드를 기다렸으므로, 그 티켓 몫으로 부팅 중인
        파드가 곧 버퍼였고 assigned 에도 들지 않았다. 여기서는 티켓이 자리를 잡자마자
        자기 파드를 띄운다(allocating). 그 파드(부팅 중인 assigned)와 아직 파드가 안
        보이는 claim 을 버퍼로 세지 않으면, 몰린 요청이 각자 파드를 띄우는 동안 같은
        수의 idle 을 또 만든다 - 같은 정책인데 파드를 두 배 만들고, 몰림 한가운데서
        부팅을 다툰다. 그렇게 세면 버퍼 증원은 Deployment 버퍼와 같은 때(몰린 요청이
        파드를 받아 간 뒤, 쿨다운 창이 목표를 붙잡고 있는 동안)에 일어난다.
        """
        snapshot = self.provider.snapshot(ct)
        capacity = self.provider.read_capacity(ct)
        reserve = self.provider.read_reserve(ct)
        all_names = {pod["name"] for pod in snapshot["pods"]}
        active = snapshot["active_names"]

        # 할당과 같은 순서로 읽는다(claim 먼저, 생성 중 목록 나중). 이유는 할당 쪽 주석.
        missing_claims = self.queues.allocating_missing_pods(active, ct)
        pending = self.queues.pending_creates(ct, self.pending_ttl_seconds)
        seen = [name for name in pending if name in all_names]
        if seen:
            self.queues.remove_pending_creates(ct, seen)
        pending_names = [name for name in pending if name not in all_names]
        pending_set = set(pending_names)
        pending_tickets = sum(1 for name in missing_claims if not name or name not in pending_set)
        pending_idle = [name for name in pending_names if self.provider.is_idle_name(name)]
        # 생성 요청을 보냈거나 보내는 중인 요청 파드.
        pending_requests = len(pending_names) - len(pending_idle)

        assigned = int(snapshot["assigned"])
        booting = sum(
            1
            for pod in snapshot["pods"]
            if not pod["terminating"]
            and pod["buffer_status"] == self.provider.STATUS_ASSIGNED
            and not pod["ready"]
        )
        # 아직 Ready 파드를 받지 못한 요청. Deployment 버퍼에서는 대기(queued)로 남아
        # 있던 요청이다.
        in_flight = booting + pending_tickets + pending_requests
        # Ready 파드를 받아 간 요청. Deployment 버퍼의 assigned 에 해당한다.
        settled = max(0, assigned - booting)
        # claim 이 건너뛰는 티켓(클라이언트가 폴링을 멈춤)은 세지 않는다. 그런 티켓
        # 하나가 큐에 남아 있다고 대기 제한(최대 30분) 내내 미리 만들기를 막으면 안 된다.
        queued = int(self.queues.claimable_queued_count(ct))
        demand = queued + in_flight
        # N 이 없으면 하나도 만들지 않는다(설정 누락으로 상한 없는 run 이 되지 않게).
        cap_value = int(capacity) if capacity is not None else 0
        detail = self.policy.desired_detail(reserve, cap_value, settled, demand)
        desired = int(detail["desired"])

        cooldown = self.policy.cooldown_seconds(ct)
        self.policy.note_target(ct, desired, cooldown)
        room = max(0, cap_value - settled)
        target = min(max(self.policy.window_max(ct) or 0, desired), room)

        idle = snapshot["idle"]
        idle_total = len(idle) + len(pending_idle) + in_flight
        free = max(0, cap_value - len(active) - pending_tickets - len(pending_names))

        create: List[str] = []
        delete: List[Dict] = []
        hold = None
        now = datetime.now(timezone.utc)
        # 끝내 Ready 가 안 되는 idle 은 자리만 쥔다. Ready 대기 상한을 넘기면 지운다
        # (Pending 으로 3분 넘게 남으면 클러스터 경보도 울린다).
        stuck = [
            pod for pod in idle
            if not pod["ready"]
            and pod.get("created") is not None
            and (now - pod["created"]).total_seconds() > self.ready_timeout_seconds
        ]
        delete.extend(stuck)

        if idle_total < target and not queued:
            # 대기 티켓이 있으면 미리 만들지 않는다 - 그 자리는 drain 이 티켓에 묶어 만든다.
            count = min(target - idle_total, free)
            create = [self.provider.new_idle_name(ct) for _ in range(max(0, count))]
        elif idle_total > target:
            # 창 안의 최댓값보다 많으면 줄인다. 단 이력이 창을 덮기 전(막 리더가 됐거나
            # 막 시작한 직후)에는 그 사이 더 높은 목표가 있었는지 모르므로 기다린다.
            hold = self.policy.cooldown_blocks_scale_down(ct, target, cooldown)
            if hold is None:
                excess = min(idle_total - target, len(idle))
                # 아직 Ready 가 아닌 것(쓸 수 없는 것)부터, 그다음 가장 최근에 만든 것.
                order = sorted(
                    (pod for pod in idle if pod not in stuck),
                    key=lambda pod: (
                        pod["ready"],
                        -(pod["created"].timestamp() if pod.get("created") else 0.0),
                    ),
                )
                delete.extend(order[: max(0, excess - len(stuck))])

        record = {
            "demand": demand,
            "queued": queued,
            "in_flight": in_flight,
            "settled": settled,
            "desired": desired,
            "bound_by": detail.get("bound_by"),
            "target": target,
            "idle": len(idle),
            "idle_ready": len(snapshot["idle_ready"]),
            "pending_idle": len(pending_idle),
            "assigned": assigned,
            "pending_tickets": pending_tickets,
            "pending_requests": pending_requests,
            "active": len(active),
            "R": reserve,
            "N": capacity,
            "cooldown_s": cooldown,
            "hold_s": None if hold is None else round(float(hold), 1),
        }
        return {"create": create, "delete": delete, "record": record}

    def _create_idle(self, ct: str, names: List[str], generation: Optional[int] = None) -> int:
        """예약한 이름을 차례로 만든다. 예약이 남아 있는 이름만 만든다.

        호출마다 남은 예약 전부의 시각을 갱신해, 생성이 느려도 뒤쪽 예약이 차례를 기다리는
        동안 만료되지 않게 한다. 이미 사라진 예약(다른 쪽이 그 자리를 빈자리로 셌다)은
        되살리지도 만들지도 않는다 - 되살리면 같은 자리를 둘이 쓴다(가짜 시계로 재현:
        N=16, 호출당 4.9초에서 19개).
        """
        created = 0
        remaining = list(names)
        while remaining:
            if not self._still_in_charge(generation):
                # 만들지 않을 이름의 자리는 바로 돌려준다(상한까지 쥐고 있지 않게).
                self._forget_pending(ct, *remaining)
                logger.info(
                    "[DirectBufferCreateStopped] compute_type=%s remaining=%s reason=%r",
                    ct,
                    len(remaining),
                    "no longer the leader or stopping",
                )
                break
            try:
                alive = self.queues.refresh_pending_creates(ct, remaining)
            except Exception as exc:
                # 예약 상태를 모르면 만들지 않는다. 남은 예약은 상한이 지나면 빠진다.
                logger.warning(
                    "[Warning] operation=direct_buffer_create_refresh compute_type=%s remaining=%s reason=%r",
                    ct,
                    len(remaining),
                    str(exc),
                )
                break
            name = remaining.pop(0)
            lost = [n for n in [name, *remaining] if n not in alive]
            remaining = [n for n in remaining if n in alive]
            if lost:
                logger.warning(
                    "[Warning] operation=direct_buffer_create compute_type=%s skipped=%s reason=%r",
                    ct,
                    lost,
                    "reservation expired before create",
                )
            if name not in alive:
                continue
            try:
                self.provider.create_idle_pod(ct, name)
                created += 1
            except ApiException as exc:
                # 확정된 거절(4xx)은 만들어지지 않았다 - 생성 중 목록에서 뺀다. 5xx 와
                # 응답 없음은 서버에 반영됐을 수 있어 목록에 둔다(상한이 지나면 빠진다).
                if 400 <= int(exc.status or 0) < 500:
                    self._forget_pending(ct, name)
                logger.warning(
                    "[Warning] operation=direct_buffer_create compute_pod=%s status=%s reason=%r",
                    name,
                    exc.status,
                    str(exc.reason),
                )
            except Exception as exc:
                logger.warning(
                    "[Warning] operation=direct_buffer_create compute_pod=%s reason=%r",
                    name,
                    str(exc),
                )
        return created

    def _forget_pending(self, ct: str, *names: str) -> None:
        try:
            self.queues.remove_pending_creates(ct, list(names))
        except QueueUnavailableError:
            pass

    def _delete_idle(self, pods: List[Dict], generation: Optional[int] = None) -> int:
        deleted = 0
        for pod in pods:
            if not self._still_in_charge(generation):
                break
            try:
                if self.provider.delete_idle_pod(pod["name"], pod.get("uid") or "", pod.get("resource_version") or ""):
                    deleted += 1
            except Exception as exc:
                logger.warning(
                    "[Warning] operation=direct_buffer_delete compute_pod=%s reason=%r",
                    pod.get("name"),
                    str(exc),
                )
        return deleted

    def _record_status(self, ct: str, record: Dict) -> None:
        with self._status_lock:
            self._status[ct] = {**record, "updated_at": time.time()}
