import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Set

from config import settings

from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class BufferCapacityReconciler:
    """Leader-only R/N policy cache and Deployment scale reconciler."""

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

        self.debounce_seconds = max(
            0.0,
            float(settings.BUFFER_RECONCILE_DEBOUNCE_SECONDS),
        )
        self.resync_seconds = max(
            1.0,
            float(settings.BUFFER_RECONCILE_RESYNC_SECONDS),
        )
        self.wait_timeout_seconds = max(
            0.1,
            float(settings.BUFFER_SCALE_DOWN_WAIT_TIMEOUT_SECONDS),
        )
        self.gate_renew_seconds = max(
            0.1,
            min(
                float(settings.BUFFER_SCALE_DOWN_GATE_RENEW_SECONDS),
                max(0.1, float(settings.BUFFER_SCALE_DOWN_GATE_TTL_SECONDS) / 2.0),
            ),
        )
        self.policy_ready_renew_seconds = max(
            0.1,
            min(
                float(settings.BUFFER_POLICY_READY_RENEW_SECONDS),
                max(0.1, float(settings.BUFFER_POLICY_READY_TTL_SECONDS) / 2.0),
            ),
        )

        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._restart_thread: Optional[threading.Thread] = None
        self._pending_types: Set[str] = set()
        self._policy_refresh_pending = True
        self._next_run_at: Optional[float] = None
        self._known_policy_types: Set[str] = set()
        self._retry_counts: Dict[str, int] = {}
        self._policy_retry_count = 0
        self._policy_ready_token = ""
        self._policy_ready = False
        self._last_policy_sync_succeeded = False
        self._leadership_validator: Callable[[], bool] = lambda: True
        self._status_lock = threading.Lock()
        self._status: Dict[str, Dict] = {}

        self.dynamic_reserve = bool(settings.BUFFER_DYNAMIC_RESERVE_ENABLED)
        self.cooldown_samples = max(
            1,
            int(settings.BUFFER_SCALE_DOWN_COOLDOWN_SAMPLES),
        )
        self.cooldown_min_samples = max(
            1,
            int(settings.BUFFER_SCALE_DOWN_COOLDOWN_MIN_SAMPLES),
        )
        # 비교군별로: 목표가 가라앉아 있던 시간들(쿨다운 추정에 씀), 진행 중인
        # 꺼짐의 시작 시각, 쿨다운이 막아낸 축소 횟수. 다른 스레드가 상태를 읽으므로
        # _churn_lock 으로 보호한다.
        self._churn_lock = threading.Lock()
        self._churn: Dict[str, Dict] = {}

    @staticmethod
    def desired_replicas(
        buffer_reserve: int,
        buffer_capacity: int,
        assigned: int,
        queue_busy: Optional[bool] = None,
    ) -> int:
        """여유 파드를 몇 개 들고 있을지.

        queue_busy=None 이 고정 정책이고 기본값이다. 동적 경로를 끈 실행은 예전과
        한 글자도 다르지 않다.

        동적 경로는 비율로 크기를 정하지 않는다. 그 방식은 공급이 병목인 동안
        리틀의 법칙 항등식이 되어 어떤 신호를 넣어도 하한으로 수렴한다 -
        여유 파드 V = 소비율 x 슬롯 회전시간 이므로, desired = 소비율 x 창 은
        V x (창 / 회전시간) 이 되고, 실측에서 회전시간 5.6~7.7초 대 창 0~1초라
        V 의 1/6~1/8 이 나온다. 큐 길이, 도착률, 반납률을 차례로 넣어 봤지만
        셋 다 같은 이유로 목표가 R 밖으로 나가지 못했다.

        그래서 재는 것을 "얼마나 빠른가" 에서 "못 쓴 수요가 있는가" 로 바꾼다.
        대기 중인 티켓이 있으면 자리가 허락하는 만큼 든다. 그 동안 준비된 파드는
        즉시 소비되므로 낭비가 원리적으로 생기지 않는다.

        이 규칙의 평형은 N x T_slot / (T_slot + T_work) 이다 - 여유가 그만큼일 때
        소비율(여유/T_slot)과 완료율(작업중/T_work)이 같아진다. N=80, 회전 6초,
        작업 60초면 여유 7.3 / 작업중 72.7 로 N 의 91% 를 쓴다. 고정 R=2 는 같은
        조건에서 작업중 20 에 묶였고 실측도 17.2 였다.

        큐가 비면 하한으로 돌아간다. 다만 그 판정은 호출자가 쿨다운을 거쳐 내린다 -
        큐가 깜빡일 때마다 목표를 내리면 만들던 파드를 버리게 된다.
        """
        room = max(0, int(buffer_capacity) - int(assigned))
        if queue_busy is None:
            return min(int(buffer_reserve), room)
        if queue_busy:
            return room
        return min(int(buffer_reserve), room)

    def _queue_busy(self, compute_type: str) -> Optional[bool]:
        """대기 중인 티켓이 있는가. 고정 정책을 유지할 때는 None.

        크기가 아니라 있고 없음만 본다. 큐 길이를 목표로 쓰면 길이를 따라 목표가
        출렁이고, 그 사이 만들던 파드가 버려진다 - 그 방식으로 돌린 실행에서 844개를
        만들어 쓰지 않고 지웠다. 자리는 N - assigned 가 알아서 막으므로 길이는
        필요하지 않다.

        Redis 가 흔들리면 이번 패스만 고정 정책으로 물러난다. R 개를 드는 것은 언제나
        안전한 답이고 다음 패스가 다시 읽는다.
        """
        if not self.dynamic_reserve:
            return None
        try:
            return self.queues.queued_count(compute_type) > 0
        except Exception as exc:
            logger.warning(
                "[Warning] operation=buffer_dynamic_reserve_queue compute_type=%s reason=%r",
                compute_type,
                str(exc),
            )
            return None

    # 쿨다운에 쓰는 분위. 높은 쪽이 맞다 - "줄였다가 되돌려지지 않으려면 얼마나
    # 기다려야 하나"의 답이고, 중앙값으로 맞추면 절반은 되돌려진다.
    #
    # 목표 크기에는 더 이상 생성 시간을 쓰지 않는다. 비율 기반 사이징을 버렸기
    # 때문이고, 덕분에 이 값이 틀려도 목표가 틀어지지 않는다.
    COOLDOWN_QUANTILE = 0.9

    def create_time_seconds(self, compute_type: str, quantile: float = COOLDOWN_QUANTILE) -> float:
        """파드를 세우는 데 걸린 시간의 분위수. 표본이 모자라면 0.

        쿨다운이 이 값을 쓴다 - 줄였다가 바로 필요해지면 사용자가 파드를 세우는
        시간만큼 기다린다. 그보다 짧게 기다리고 줄일 이유가 없다.
        """
        with self._churn_lock:
            creates = sorted(self._churn_state(compute_type)["creates"])
        if len(creates) < self.cooldown_min_samples:
            return 0.0
        return float(creates[min(len(creates) - 1, int(len(creates) * quantile))])

    def _churn_state(self, compute_type: str) -> Dict:
        return self._churn.setdefault(
            compute_type,
            {
                # 목표가 어떤 수준에서 떨어졌다가 그 수준으로 돌아오기까지 걸린
                # 시간들. 쿨다운 길이를 여기서 추정한다.
                "dips": [],
                # 파드 생성 → Ready 에 걸린 시간들. 쿨다운의 바닥을 정한다.
                "creates": [],
                # Ready 이벤트는 한 파드에 여러 번 오므로 센 것을 기억한다.
                "created_seen": [],
                # 진행 중인 꺼짐: 언제 떨어졌고, 어느 값에서 떨어졌나.
                "dip_started_at": None,
                "last_target": None,
                "held_back": 0,
                "cooldown": None,
                # (관측 시각, 그때의 목표). 쿨다운 창 밖으로 나간 것은 버린다.
                "targets": [],
            },
        )

    def cooldown_seconds(self, compute_type: str) -> float:
        """관측값 두 개의 큰 쪽. 설정에 적힌 숫자가 아니다.

            생성 시간   파드를 다시 세우는 데 걸리는 시간. 줄였다가 바로 필요해지면
                        사용자가 그만큼 기다린다. 그보다 짧게 기다릴 이유가 없다.
            꺼짐 길이   목표가 가라앉아 있던 시간. 그 안에 줄이면 되돌려진다.

        둘 다 표본이 모자란 때는 0 을 돌려준다. 그 시점은 기동 직후뿐이고 버퍼를
        채우는 중이라 줄일 것이 없으므로 위험하지 않다.

        목표가 꺼졌다 돌아오는 시간을 쓰는 이유:

        쿨다운이 답해야 할 질문이 그것이다 - 지금 줄이면 얼마 뒤에 되돌려지는가.
        목표값은 큐를 그대로 따라가고 쿨다운이 손대지 않으므로, 이 신호는 쿨다운
        자신의 동작에 오염되지 않는다. 축소에서 재확대까지의 간격을 쓰면 쿨다운이
        밀어낸 시간이 그 안에 섞여 추정치가 자기 자신을 키운다.

        중앙값이 아니라 높은 분위를 쓴다. 중앙값으로 맞추면 절반의 꺼짐이 쿨다운보다
        길어 그만큼은 되돌려진다.

        표본이 모이기 전에는 하한을 쓴다. 기동 직후가 그 상태다.
        """
        with self._churn_lock:
            dips = sorted(self._churn_state(compute_type)["dips"])

        def high(values):
            if len(values) < self.cooldown_min_samples:
                return None
            return float(values[min(len(values) - 1, int(len(values) * 0.9))])

        create = self.create_time_seconds(compute_type) or None
        candidates = [v for v in (create, high(dips)) if v is not None]
        return max(candidates) if candidates else 0.0

    def _note_target(self, compute_type: str, desired: int, cooldown: float) -> None:
        """이번 패스의 목표를 적고, 꺼짐이 끝났으면 그 길이를 표본에 넣는다.

        꺼짐은 목표가 내려간 순간 시작되고 올라간 순간 끝난다. 기준값을 고정해
        "그 수준으로 돌아올 때까지"로 재면, 목표가 계단식으로 내려가 다시 올라오지
        않는 구간에서 꺼짐이 영구히 열린 채로 남아 그 뒤의 표본을 하나도 못 모은다.
        재려는 것은 "수요가 가라앉아 있는 시간"이고, 조금이라도 되돌아오면 그 구간은
        끝난 것으로 보는 편이 보수적이다 - 쿨다운을 짧게 잡는 방향이 아니라 표본을
        확보하는 방향이다.
        """
        now = time.monotonic()
        target = int(desired)
        cutoff = now - max(0.0, cooldown)
        with self._churn_lock:
            state = self._churn_state(compute_type)

            previous = state["last_target"]
            if previous is not None:
                if state["dip_started_at"] is None:
                    if target < previous:
                        state["dip_started_at"] = now
                elif target > previous:
                    state["dips"].append(now - state["dip_started_at"])
                    if len(state["dips"]) > self.cooldown_samples:
                        del state["dips"][: len(state["dips"]) - self.cooldown_samples]
                    state["dip_started_at"] = None
            state["last_target"] = target

            targets = state["targets"]
            targets.append((now, target))
            while targets and targets[0][0] < cutoff:
                targets.pop(0)

    def _cooldown_blocks_scale_down(
        self,
        compute_type: str,
        desired: int,
        cooldown: float,
    ) -> Optional[float]:
        """아직 기다려야 하는 초, 또는 줄여도 될 때 None.

        창 안에서 목표가 지금보다 높았던 적이 있으면 아직 출렁이는 중이므로 기다린다.
        내내 지금 이하였다면 수요가 가라앉은 것이고, 그때의 축소는 되돌려지지 않는다.

        여기에는 "여유가 목표보다 많다"는 경우만 들어온다. 용량 상한이 요구하는 축소는
        이 함수를 거치지 않으므로 N 이 열린 채로 유지되는 일은 없다.
        """
        if cooldown <= 0:
            return None
        now = time.monotonic()
        with self._churn_lock:
            state = self._churn_state(compute_type)
            state["cooldown"] = cooldown
            higher = [(at, value) for at, value in state["targets"] if value > int(desired)]
            if not higher:
                return None
            newest_at = max(at for at, _ in higher)
            state["held_back"] += 1
        remaining = cooldown - (now - newest_at)
        return remaining if remaining > 0 else None

    def set_leadership_validator(self, validator: Callable[[], bool]) -> None:
        self._leadership_validator = validator

    def _has_write_authority(self) -> bool:
        try:
            return bool(self._leadership_validator())
        except Exception:
            logger.exception("[Failed] operation=leadership_validation")
            return False

    def start(self) -> None:
        with self._condition:
            if self._thread and self._thread.is_alive():
                if (
                    self._stop_event.is_set()
                    and (
                        self._restart_thread is None
                        or not self._restart_thread.is_alive()
                    )
                ):
                    old_thread = self._thread
                    self._restart_thread = threading.Thread(
                        target=self._restart_after_thread_exit,
                        args=(old_thread,),
                        name="buffer-capacity-restart",
                        daemon=True,
                    )
                    self._restart_thread.start()
                return
            self._policy_ready_token = uuid.uuid4().hex
            self._policy_ready = False
            self._last_policy_sync_succeeded = False
            try:
                self.queues.clear_buffer_policy_ready()
            except QueueUnavailableError as exc:
                logger.warning(
                    "[Warning] operation=buffer_policy_ready_invalidate reason=%r",
                    str(exc),
                )
            self._stop_event.clear()
            self._policy_refresh_pending = True
            self._next_run_at = time.monotonic()
            self._thread = threading.Thread(
                target=self._run,
                name="buffer-capacity-reconciler",
                daemon=True,
            )
            self._thread.start()
            self._condition.notify_all()
        logger.info("[BufferCapacityReconcilerStarted]")

    def _restart_after_thread_exit(self, old_thread: threading.Thread) -> None:
        old_thread.join()
        with self._condition:
            if self._thread is old_thread:
                self._thread = None
            if self._restart_thread is threading.current_thread():
                self._restart_thread = None
        if self._has_write_authority():
            self.start()

    def stop(self) -> None:
        with self._condition:
            thread = self._thread
            self._stop_event.set()
            self._condition.notify_all()
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(2.0, self.wait_timeout_seconds + 1.0))
        with self._condition:
            if self._thread is thread and (not thread or not thread.is_alive()):
                self._thread = None
        token = self._policy_ready_token
        if token:
            try:
                self.queues.clear_buffer_policy_ready(token)
            except QueueUnavailableError as exc:
                logger.warning(
                    "[Warning] operation=buffer_policy_ready_clear reason=%r",
                    str(exc),
                )
        self._policy_ready = False
        logger.info("[BufferCapacityReconcilerStopped]")

    def on_buffer_event(self, event_type: str, pod, source: str = "watch") -> None:
        del event_type, source
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = labels.get(self.provider.LABEL_COMPUTE_TYPE)
        if compute_type:
            self._note_create_time(compute_type, pod)
            self.request_reconcile(compute_type)

    def _note_create_time(self, compute_type: str, pod) -> None:
        """파드가 Ready 가 된 순간, 세우는 데 걸린 시간을 표본에 넣는다.

        쿨다운의 바닥이 되는 값이다. Ready 이벤트는 같은 파드에 여러 번 오므로
        uid 로 한 번만 센다. 시각이 둘 다 있을 때만 쓰고, 없으면 조용히 넘어간다 -
        추정기는 표본이 모자라면 스스로 비켜선다.
        """
        try:
            if not self.provider._pod_is_ready(pod):
                return
            uid = getattr(getattr(pod, "metadata", None), "uid", None)
            created = getattr(getattr(pod, "metadata", None), "creation_timestamp", None)
            ready = self.provider._pod_ready_at(pod)
            if not uid or created is None or ready is None:
                return
            seconds = (ready - created).total_seconds()
            if seconds < 0:
                return
        except Exception:
            return
        with self._churn_lock:
            state = self._churn_state(compute_type)
            if uid in state["created_seen"]:
                return
            state["created_seen"].append(uid)
            if len(state["created_seen"]) > self.cooldown_samples * 2:
                del state["created_seen"][: len(state["created_seen"]) - self.cooldown_samples * 2]
            state["creates"].append(seconds)
            if len(state["creates"]) > self.cooldown_samples:
                del state["creates"][: len(state["creates"]) - self.cooldown_samples]

    def on_deployment_event(
        self,
        event_type: str,
        deployment,
        source: str = "watch",
    ) -> None:
        del event_type, source
        if not self._has_write_authority() or self._stop_event.is_set():
            return
        metadata = getattr(deployment, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = self.queues.normalize_compute_type(
            labels.get(self.provider.LABEL_COMPUTE_TYPE)
        )
        self.request_reconcile(compute_type)
        self.request_policy_refresh()

    def request_reconcile(self, compute_type: str, *, retry: bool = False) -> None:
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        with self._condition:
            self._pending_types.add(compute_type_value)
            if retry:
                retry_count = self._retry_counts.get(compute_type_value, 0) + 1
                self._retry_counts[compute_type_value] = retry_count
                delay = min(
                    self.resync_seconds,
                    max(1.0, 2.0 ** min(retry_count - 1, 6)),
                )
            else:
                self._retry_counts.pop(compute_type_value, None)
                delay = self.debounce_seconds
            self._schedule_locked(delay)

    def request_policy_refresh(self, *, retry: bool = False) -> None:
        with self._condition:
            self._policy_refresh_pending = True
            if retry:
                self._policy_retry_count += 1
                delay = min(
                    self.resync_seconds,
                    max(1.0, 2.0 ** min(self._policy_retry_count - 1, 6)),
                )
            else:
                self._policy_retry_count = 0
                delay = self.debounce_seconds
            self._schedule_locked(delay)

    def get_status(self) -> Dict[str, Dict]:
        with self._status_lock:
            return {key: dict(value) for key, value in self._status.items()}

    def _schedule_after(self, delay: float) -> None:
        """Wake up when the cooldown expires instead of waiting for an event.

        Without this a buffer that went quiet would hold its extra spare until the
        next resync, which is a minute away.
        """
        with self._condition:
            self._schedule_locked(max(0.0, float(delay)))

    def _schedule_locked(self, delay: float) -> None:
        run_at = time.monotonic() + max(0.0, delay)
        if self._next_run_at is None or run_at < self._next_run_at:
            self._next_run_at = run_at
        self._condition.notify_all()

    def _run(self) -> None:
        next_resync_at = time.monotonic()
        next_ready_renew_at = float("inf")
        try:
            while not self._stop_event.is_set():
                if not self._has_write_authority():
                    self._clear_policy_ready_best_effort()
                    self._stop_event.wait(1.0)
                    continue
                refresh_policies = False
                renew_policy_ready = False
                compute_types: Set[str] = set()

                with self._condition:
                    while not self._stop_event.is_set():
                        now = time.monotonic()
                        due_at = next_resync_at
                        if self._next_run_at is not None:
                            due_at = min(due_at, self._next_run_at)
                        due_at = min(due_at, next_ready_renew_at)
                        if now >= due_at:
                            break
                        self._condition.wait(timeout=max(0.0, due_at - now))

                    if self._stop_event.is_set():
                        break

                    now = time.monotonic()
                    periodic_resync = now >= next_resync_at
                    scheduled_run = (
                        self._next_run_at is not None and now >= self._next_run_at
                    )
                    if periodic_resync:
                        refresh_policies = True
                        next_resync_at = now + self.resync_seconds
                    if scheduled_run:
                        refresh_policies = (
                            refresh_policies or self._policy_refresh_pending
                        )
                        compute_types.update(self._pending_types)
                        self._pending_types.clear()
                        self._policy_refresh_pending = False
                        self._next_run_at = None
                    renew_policy_ready = (
                        self._policy_ready and now >= next_ready_renew_at
                    )

                ready_candidate_types: Set[str] = set()
                publish_policy_ready = False
                if periodic_resync and self.on_periodic_cleanup is not None:
                    try:
                        self.on_periodic_cleanup()
                    except Exception as exc:
                        logger.warning(
                            "[Warning] operation=periodic_cleanup reason=%r",
                            str(exc),
                        )
                if refresh_policies:
                    ready_candidate_types = self.sync_policies()
                    compute_types.update(ready_candidate_types)
                    renew_policy_ready = False
                    publish_policy_ready = (
                        self._last_policy_sync_succeeded
                        and not self._stop_event.is_set()
                        and self._has_write_authority()
                    )
                    if not publish_policy_ready:
                        self._clear_policy_ready_best_effort()
                        next_ready_renew_at = float("inf")
                        self.request_policy_refresh(retry=True)

                if renew_policy_ready and self._policy_ready:
                    if (
                        self._stop_event.is_set()
                        or not self._has_write_authority()
                    ):
                        self._clear_policy_ready_best_effort()
                        next_ready_renew_at = float("inf")
                        continue
                    try:
                        renewed = self.queues.renew_buffer_policy_ready(
                            self._policy_ready_token
                        )
                    except QueueUnavailableError as exc:
                        renewed = False
                        logger.warning(
                            "[Warning] operation=buffer_policy_ready_renew reason=%r",
                            str(exc),
                        )
                    if renewed:
                        next_ready_renew_at = (
                            time.monotonic() + self.policy_ready_renew_seconds
                        )
                    else:
                        self._policy_ready = False
                        next_ready_renew_at = float("inf")
                        self.request_policy_refresh()

                ready_reconcile_failed = False
                for compute_type in sorted(compute_types):
                    if (
                        self._stop_event.is_set()
                        or not self._has_write_authority()
                    ):
                        break
                    result = self.reconcile_type(compute_type)
                    if result.get("retry"):
                        self.request_reconcile(compute_type, retry=True)
                        if compute_type in ready_candidate_types:
                            ready_reconcile_failed = True
                    else:
                        with self._condition:
                            self._retry_counts.pop(compute_type, None)

                if publish_policy_ready:
                    if (
                        ready_reconcile_failed
                        or self._stop_event.is_set()
                        or not self._has_write_authority()
                    ):
                        self._clear_policy_ready_best_effort()
                        next_ready_renew_at = float("inf")
                        self.request_policy_refresh(retry=True)
                    else:
                        try:
                            self.queues.publish_buffer_policy_ready(
                                self._policy_ready_token
                            )
                            self._policy_ready = True
                            next_ready_renew_at = (
                                time.monotonic()
                                + self.policy_ready_renew_seconds
                            )
                            self._policy_retry_count = 0
                            if self.on_capacity_available is not None:
                                for compute_type in sorted(
                                    ready_candidate_types
                                ):
                                    try:
                                        self.on_capacity_available(
                                            compute_type
                                        )
                                    except Exception as exc:
                                        logger.debug(
                                            "[BufferQueueKickSkipped] "
                                            "compute_type=%s reason=%r",
                                            compute_type,
                                            str(exc),
                                        )
                        except QueueUnavailableError as exc:
                            self._policy_ready = False
                            next_ready_renew_at = float("inf")
                            logger.warning(
                                "[Warning] operation=buffer_policy_ready_publish "
                                "reason=%r",
                                str(exc),
                            )
                            self.request_policy_refresh(retry=True)
        except Exception as exc:
            logger.exception(
                "[Failed] operation=buffer_capacity_reconciler reason=%r",
                str(exc),
            )
        finally:
            self._clear_policy_ready_best_effort()
            with self._condition:
                if self._thread is threading.current_thread():
                    self._thread = None

    def sync_policies(self) -> Set[str]:
        """Refresh the leader-owned Redis cache from Deployment annotations."""
        self._last_policy_sync_succeeded = False
        if self._stop_event.is_set() or not self._has_write_authority():
            return set()
        try:
            deployments = self.provider.list_buffer_deployments()
        except Exception as exc:
            logger.warning(
                "[Warning] operation=buffer_policy_sync reason=%r",
                str(exc),
            )
            self._record_status("_global", policy_error=str(exc))
            return set()

        deployments_by_type: Dict[str, list] = {}
        invalid_by_type: Dict[str, str] = {}
        for deployment in deployments:
            metadata = getattr(deployment, "metadata", None)
            labels = getattr(metadata, "labels", None) or {}
            compute_type = self.queues.normalize_compute_type(
                labels.get(self.provider.LABEL_COMPUTE_TYPE)
            )
            deployments_by_type.setdefault(compute_type, []).append(deployment)

        valid_policies: Dict[str, Dict] = {}
        for compute_type, matching in deployments_by_type.items():
            if len(matching) != 1:
                invalid_by_type[compute_type] = (
                    "Exactly one warm-buffer Deployment is required per compute-type"
                )
                continue
            try:
                valid_policies[compute_type] = self.provider.parse_deployment_policy(
                    matching[0]
                )
            except ValueError as exc:
                invalid_by_type[compute_type] = str(exc)

        try:
            registered_types = set(self.queues.known_compute_types())
        except QueueUnavailableError:
            registered_types = set()
        all_known_types = (
            registered_types
            | self._known_policy_types
            | set(deployments_by_type)
        )
        sync_succeeded = True

        for compute_type in sorted(all_known_types - set(valid_policies)):
            try:
                if not self._publish_policy_serialized(compute_type, None):
                    sync_succeeded = False
            except QueueUnavailableError as exc:
                sync_succeeded = False
                logger.warning(
                    "[Warning] operation=buffer_policy_clear compute_type=%s reason=%r",
                    compute_type,
                    str(exc),
                )
            error = invalid_by_type.get(
                compute_type,
                "No warm-buffer Deployment exists for this compute-type",
            )
            self._record_status(
                compute_type,
                policy_valid=False,
                policy_error=error,
            )
            logger.error(
                "[BufferPolicyInvalid] compute_type=%s reason=%r",
                compute_type,
                error,
            )

        stored_types: Set[str] = set()
        for compute_type, policy in sorted(valid_policies.items()):
            try:
                if self._publish_policy_serialized(compute_type, policy):
                    stored_types.add(compute_type)
                    self._record_status(
                        compute_type,
                        policy_valid=True,
                        policy_error="",
                        deployment_name=policy["deployment_name"],
                        R=policy["R"],
                        N=policy["N"],
                    )
                else:
                    sync_succeeded = False
            except QueueUnavailableError as exc:
                sync_succeeded = False
                self._record_status(
                    compute_type,
                    policy_valid=False,
                    policy_error=str(exc),
                )
                logger.warning(
                    "[Warning] operation=buffer_policy_store compute_type=%s reason=%r",
                    compute_type,
                    str(exc),
                )

        if stored_types:
            try:
                self.queues.register_compute_types(sorted(stored_types))
            except QueueUnavailableError as exc:
                sync_succeeded = False
                logger.warning(
                    "[Warning] operation=buffer_policy_register reason=%r",
                    str(exc),
                )

        self._known_policy_types = set(deployments_by_type)
        self._last_policy_sync_succeeded = sync_succeeded
        return stored_types

    @staticmethod
    def _same_policy(current: Optional[Dict], desired: Optional[Dict]) -> bool:
        if current is None or desired is None:
            return current is None and desired is None
        return (
            current.get("deployment_name") == desired.get("deployment_name")
            and int(current.get("R")) == int(desired.get("R"))
            and int(current.get("N")) == int(desired.get("N"))
        )

    def _publish_policy_serialized(
        self,
        compute_type: str,
        policy: Optional[Dict],
    ) -> bool:
        """
        Publish or clear one policy under the same gate and lock as allocation.

        This prevents an allocator from reading an old N concurrently with an
        annotation update. Unchanged Deployment status events remain O(1) and
        do not acquire either control primitive.
        """
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        if self._stop_event.is_set() or not self._has_write_authority():
            return False
        current = self.queues.get_buffer_policy(compute_type_value)
        if self._same_policy(current, policy):
            return True

        gate_token = self.queues.acquire_scale_down_gate(compute_type_value)
        if not gate_token:
            return False

        lock_token = None
        changed = False
        try:
            deadline = time.monotonic() + min(
                1.0,
                self.wait_timeout_seconds,
            )
            lock_token = self._acquire_allocator_lock_until(
                compute_type_value,
                deadline,
                threading.Event(),
            )
            if not lock_token:
                return False

            if self._stop_event.is_set() or not self._has_write_authority():
                return False
            current = self.queues.get_buffer_policy(compute_type_value)
            if self._same_policy(current, policy):
                return True

            if policy is None:
                self.queues.clear_buffer_policy(compute_type_value)
            else:
                self.queues.set_buffer_policy(
                    compute_type=compute_type_value,
                    deployment_name=policy["deployment_name"],
                    R=policy["R"],
                    N=policy["N"],
                    resource_version=policy.get("resource_version", ""),
                )
            changed = True
            return True
        finally:
            if lock_token:
                self.queues.release_allocator_lock(
                    compute_type_value,
                    lock_token,
                )
            self.queues.release_scale_down_gate(
                compute_type_value,
                gate_token,
            )
            if changed and self.on_capacity_available is not None:
                try:
                    self.on_capacity_available(compute_type_value)
                except Exception as exc:
                    logger.debug(
                        "[BufferQueueKickSkipped] compute_type=%s reason=%r",
                        compute_type_value,
                        str(exc),
                    )

    def _clear_policy_ready_best_effort(self) -> None:
        token = self._policy_ready_token
        self._policy_ready = False
        if not token:
            return
        try:
            self.queues.clear_buffer_policy_ready(token)
        except QueueUnavailableError as exc:
            logger.warning(
                "[Warning] operation=buffer_policy_ready_clear reason=%r",
                str(exc),
            )

    def reconcile_type(self, compute_type: str) -> Dict:
        """Synchronously reconcile one compute type; safe to unit test directly."""
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        if self._stop_event.is_set() or not self._has_write_authority():
            return {
                "compute_type": compute_type_value,
                "status": "blocked",
                "reason": "leadership_not_valid",
            }
        try:
            policy = self.queues.get_buffer_policy(compute_type_value)
            if not policy:
                result = {
                    "compute_type": compute_type_value,
                    "status": "blocked",
                    "reason": "policy_unavailable",
                }
                self._record_status(compute_type_value, **result)
                return result

            snapshot = self.provider.list_buffer_snapshot(compute_type_value)
            queue_busy = self._queue_busy(compute_type_value)
            desired = self.desired_replicas(
                policy["R"],
                policy["N"],
                snapshot["buffer_assigned"],
                queue_busy,
            )
            current = self.provider.read_deployment_replicas(
                policy["deployment_name"]
            )

            base_result = {
                "compute_type": compute_type_value,
                "deployment_name": policy["deployment_name"],
                "R": policy["R"],
                "N": policy["N"],
                "buffer_total": snapshot["buffer_total"],
                "buffer_available": snapshot["buffer_available"],
                "buffer_assigned": snapshot["buffer_assigned"],
                "current_replicas": current,
                "desired_replicas": desired,
            }

            if current < desired:
                result = self._scale_up(
                    compute_type_value,
                    base_result,
                )
                self._record_status(compute_type_value, **result)
                return result

            # Over capacity is an N violation and shrinks at once. Anything else
            # is just an extra spare and waits out the cooldown, so a buffer that
            # is merely cautious can never hold N open.
            #
            # 판정은 replicas 가 아니라 실제로 떠 있는 파드로 한다. replicas 를 낮춰도
            # ReplicaSet 이 따라잡기 전까지는 대기 파드가 그보다 많이 남아 있고, 그
            # 구간에서 replicas 만 보면 "한도 안" 으로 읽혀 쿨다운을 타게 된다. 실측에서
            # 축소가 1186번 미뤄지는 동안 할당중 + 대기중 이 14.7% 의 시간 동안 N 을
            # 넘었다. buffer_total 은 삭제 중인 파드를 뺀 할당중 + 대기중 이므로,
            # 이 값이 N 을 넘으면 그 자체로 위반이고 기다릴 이유가 없다.
            # 이번 패스가 무엇을 보고 무엇을 정했는지 남긴다. 이 줄이 없어서
            # "동적 경로가 켜져 있었는가" 를 사후에 확인할 수 없었고, 기능이 꺼진
            # 실행을 켜진 것으로 읽어 여러 번 잘못 판단했다.
            logger.info(
                "[BufferTarget] compute_type=%s queue_busy=%s desired=%s current=%s "
                "assigned=%s available=%s total=%s R=%s N=%s dynamic=%s",
                compute_type_value,
                queue_busy,
                desired,
                current,
                snapshot["buffer_assigned"],
                snapshot["buffer_available"],
                snapshot["buffer_total"],
                policy["R"],
                policy["N"],
                self.dynamic_reserve,
            )

            room = max(0, int(policy["N"]) - int(snapshot["buffer_assigned"]))
            over_capacity = (
                current > room
                or int(snapshot["buffer_total"]) > int(policy["N"])
            )
            cooldown = self.cooldown_seconds(compute_type_value)
            self._note_target(compute_type_value, desired, cooldown)

            if current > desired or snapshot["buffer_available"] > desired:
                waiting = (
                    None
                    if over_capacity
                    else self._cooldown_blocks_scale_down(
                        compute_type_value, desired, cooldown
                    )
                )
                if waiting is not None:
                    logger.info(
                        "[BufferScaleDownDeferred] compute_type=%s reason=%r "
                        "desired=%s current=%s available=%s cooldown_s=%.1f "
                        "remaining_s=%.1f",
                        compute_type_value,
                        "target was higher inside the cooldown window",
                        desired,
                        current,
                        snapshot["buffer_available"],
                        cooldown,
                        waiting,
                    )
                    result = {
                        **base_result,
                        "status": "deferred",
                        "reason": "scale_down_cooldown",
                        "cooldown_s": round(cooldown, 2),
                        "cooldown_remaining_s": round(waiting, 2),
                    }
                    self._record_status(compute_type_value, **result)
                    self._schedule_after(waiting)
                    return result
                result = self._scale_down(
                    compute_type_value,
                    policy,
                    base_result,
                )
                self._record_status(compute_type_value, **result)
                return result

            result = {**base_result, "status": "converged"}
            self._record_status(compute_type_value, **result)
            return result
        except Exception as exc:
            logger.exception(
                "[Failed] operation=buffer_capacity_reconcile compute_type=%s reason=%r",
                compute_type_value,
                str(exc),
            )
            result = {
                "compute_type": compute_type_value,
                "status": "error",
                "reason": str(exc),
                "retry": True,
            }
            self._record_status(compute_type_value, **result)
            return result

    def _scale_up(self, compute_type: str, base_result: Dict) -> Dict:
        deadline = time.monotonic() + min(1.0, self.wait_timeout_seconds)
        lock_token = self._acquire_allocator_lock_until(
            compute_type,
            deadline,
            threading.Event(),
        )
        if not lock_token:
            return {
                **base_result,
                "status": "deferred",
                "reason": "allocator_lock_timeout",
                "retry": True,
            }

        try:
            policy = self.queues.get_buffer_policy(compute_type)
            if not policy:
                return {
                    **base_result,
                    "status": "blocked",
                    "reason": "policy_unavailable",
                }
            snapshot = self.provider.list_buffer_snapshot(compute_type)
            desired = self.desired_replicas(
                policy["R"],
                policy["N"],
                snapshot["buffer_assigned"],
                self._queue_busy(compute_type),
            )
            current = self.provider.read_deployment_replicas(
                policy["deployment_name"]
            )

            # N 을 넘지 않는 마지막 관문. 목표 공식은 N - assigned 로 이미 묶여 있지만
            # 그것만으로는 부족하다 - 파드가 배정되면 ReplicaSet 의 selector 에서 빠지고
            # ReplicaSet 은 replicas 를 맞추려 한 개를 더 만든다. 배정 자체는 available
            # 을 assigned 로 옮길 뿐이라 합이 그대로인데, 이 backfill 이 합을 올린다.
            # 목표를 낮춰 두면 다음 패스가 금방 따라잡지만, 삭제가 늦거나 락이 밀리는
            # 순간에는 그 사이에 넘을 수 있다.
            #
            # buffer_total 은 terminating 을 뺀 available + assigned 다. 삭제 중인 파드는
            # 자리를 곧 비우므로 세지 않는다 - 상한이 지켜야 하는 것은 "일하고 있는 것과
            # 그것을 기다리는 것"의 합이다. 그 수가 N 을 넘게 만드는 확대는 하지 않는다.
            headroom = int(policy["N"]) - int(snapshot["buffer_total"])
            if desired > current and desired - current > headroom:
                capped = current + max(0, headroom)
                if capped != desired:
                    logger.info(
                        "[BufferScaleUpCapped] compute_type=%s desired=%s->%s "
                        "current=%s total=%s N=%s",
                        compute_type,
                        desired,
                        capped,
                        current,
                        snapshot["buffer_total"],
                        policy["N"],
                    )
                desired = capped

            refreshed = {
                **base_result,
                "R": policy["R"],
                "N": policy["N"],
                "buffer_total": snapshot["buffer_total"],
                "buffer_available": snapshot["buffer_available"],
                "buffer_assigned": snapshot["buffer_assigned"],
                "current_replicas": current,
                "desired_replicas": desired,
            }
            if current > desired or snapshot["buffer_available"] > desired:
                return {
                    **refreshed,
                    "status": "deferred",
                    "reason": "scale_down_required",
                    "retry": True,
                }
            if current == desired:
                return {**refreshed, "status": "converged"}
            if not self._has_write_authority():
                return {
                    **refreshed,
                    "status": "blocked",
                    "reason": "leadership_not_valid",
                    "retry": True,
                }

            patched = self.provider.patch_deployment_replicas(
                policy["deployment_name"],
                desired,
            )
            logger.info(
                "[BufferScaled] compute_type=%s direction=up replicas=%s->%s "
                "assigned=%s R=%s N=%s",
                compute_type,
                current,
                desired,
                snapshot["buffer_assigned"],
                policy["R"],
                policy["N"],
            )
            return {
                **refreshed,
                "status": "scaled_up",
                "patched_replicas": patched,
            }
        finally:
            self.queues.release_allocator_lock(compute_type, lock_token)

    def _scale_down(
        self,
        compute_type: str,
        initial_policy: Dict,
        base_result: Dict,
    ) -> Dict:
        gate_token = self.queues.acquire_scale_down_gate(compute_type)
        if not gate_token:
            return {
                **base_result,
                "status": "deferred",
                "reason": "scale_down_gate_busy",
                "retry": True,
            }

        heartbeat_stop = threading.Event()
        heartbeat_lost = threading.Event()
        heartbeat = threading.Thread(
            target=self._renew_gate_loop,
            args=(compute_type, gate_token, heartbeat_stop, heartbeat_lost),
            name=f"buffer-gate-{compute_type}",
            daemon=True,
        )
        heartbeat.start()

        patched = False
        deletion_observed = False
        desired = int(base_result["desired_replicas"])
        try:
            barrier_deadline = time.monotonic() + self.wait_timeout_seconds
            if not self._wait_for_allocator_barrier(
                compute_type,
                barrier_deadline,
                heartbeat_lost,
            ):
                return {
                    **base_result,
                    "status": "deferred",
                    "reason": "allocator_barrier_timeout",
                    "retry": True,
                }

            orphan_deadline = time.monotonic() + self.wait_timeout_seconds
            if not self._wait_for_assigned_orphans(
                compute_type,
                orphan_deadline,
                heartbeat_lost,
            ):
                return {
                    **base_result,
                    "status": "deferred",
                    "reason": "assigned_orphan_timeout",
                    "retry": True,
                }

            lock_deadline = time.monotonic() + self.wait_timeout_seconds
            lock_token = self._acquire_allocator_lock_until(
                compute_type,
                lock_deadline,
                heartbeat_lost,
            )
            if not lock_token:
                return {
                    **base_result,
                    "status": "deferred",
                    "reason": "allocator_lock_timeout",
                    "retry": True,
                }

            try:
                if heartbeat_lost.is_set():
                    return {
                        **base_result,
                        "status": "deferred",
                        "reason": "scale_down_gate_lost",
                        "retry": True,
                    }

                policy = self.queues.get_buffer_policy(compute_type)
                if not policy:
                    return {
                        **base_result,
                        "status": "blocked",
                        "reason": "policy_unavailable",
                    }

                snapshot = self.provider.list_buffer_snapshot(compute_type)
                desired = self.desired_replicas(
                    policy["R"],
                    policy["N"],
                    snapshot["buffer_assigned"],
                    self._queue_busy(compute_type),
                )
                current = self.provider.read_deployment_replicas(
                    policy["deployment_name"]
                )
                refreshed_result = {
                    **base_result,
                    "deployment_name": policy["deployment_name"],
                    "R": policy["R"],
                    "N": policy["N"],
                    "buffer_total": snapshot["buffer_total"],
                    "buffer_available": snapshot["buffer_available"],
                    "buffer_assigned": snapshot["buffer_assigned"],
                    "current_replicas": current,
                    "desired_replicas": desired,
                }
                if current < desired:
                    return {
                        **refreshed_result,
                        "status": "deferred",
                        "reason": "scale_up_required",
                        "retry": True,
                    }

                if current == desired and snapshot["buffer_available"] <= desired:
                    return {
                        **refreshed_result,
                        "status": "converged",
                    }

                if current > desired:
                    if heartbeat_lost.is_set() or not self._has_write_authority():
                        return {
                            **refreshed_result,
                            "status": "deferred",
                            "reason": "leadership_or_gate_lost",
                            "retry": True,
                        }
                    self.provider.patch_deployment_replicas(
                        policy["deployment_name"],
                        desired,
                    )
                    patched = True
                initial_policy = policy
                base_result = refreshed_result
            finally:
                self.queues.release_allocator_lock(compute_type, lock_token)

            observation_deadline = time.monotonic() + self.wait_timeout_seconds
            deletion_observed = self._wait_for_scale_down_observation(
                compute_type,
                desired,
                observation_deadline,
                heartbeat_lost,
            )
            result = {
                **base_result,
                "deployment_name": initial_policy["deployment_name"],
                "status": "scaled_down" if patched else "converged",
                "desired_replicas": desired,
                "deletion_observed": deletion_observed,
            }
            if patched:
                result["patched_replicas"] = desired
            if not deletion_observed:
                # A delayed ReplicaSet delete may still target a Pod selected
                # from its older cache. Stop every allocator before releasing
                # the per-type gate; the next successful policy sync and
                # observation will publish readiness again.
                self._clear_policy_ready_best_effort()
                self.request_policy_refresh(retry=True)
                result.update(
                    {
                        "status": "deferred",
                        "reason": "scale_down_observation_timeout",
                        "retry": True,
                    }
                )
            logger.info(
                "[BufferScaleDownObserved] compute_type=%s patched=%s "
                "replicas=%s->%s deletion_observed=%s",
                compute_type,
                patched,
                base_result["current_replicas"],
                desired,
                deletion_observed,
            )
            return result
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=max(1.0, self.gate_renew_seconds + 0.5))
            try:
                self.queues.release_scale_down_gate(compute_type, gate_token)
            finally:
                if deletion_observed and self.on_capacity_available is not None:
                    try:
                        self.on_capacity_available(compute_type)
                    except Exception as exc:
                        logger.debug(
                            "[BufferQueueKickSkipped] compute_type=%s reason=%r",
                            compute_type,
                            str(exc),
                        )
            if patched and heartbeat_lost.is_set():
                logger.warning(
                    "[Warning] operation=buffer_scale_down compute_type=%s "
                    "reason=%r",
                    compute_type,
                    "scale-down gate expired after replicas patch",
                )

    def _renew_gate_loop(
        self,
        compute_type: str,
        gate_token: str,
        stop_event: threading.Event,
        lost_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.gate_renew_seconds):
            if not self._has_write_authority():
                lost_event.set()
                return
            try:
                if not self.queues.renew_scale_down_gate(
                    compute_type,
                    gate_token,
                ):
                    lost_event.set()
                    return
            except Exception:
                lost_event.set()
                logger.exception(
                    "[Failed] operation=scale_down_gate_renew compute_type=%s",
                    compute_type,
                )
                return

    def _wait_for_allocator_barrier(
        self,
        compute_type: str,
        deadline: float,
        gate_lost: threading.Event,
    ) -> bool:
        token = self._acquire_allocator_lock_until(
            compute_type,
            deadline,
            gate_lost,
        )
        if not token:
            return False
        self.queues.release_allocator_lock(compute_type, token)
        return True

    def _acquire_allocator_lock_until(
        self,
        compute_type: str,
        deadline: float,
        gate_lost: threading.Event,
    ) -> Optional[str]:
        retry_seconds = 0.02
        while (
            not self._stop_event.is_set()
            and self._has_write_authority()
            and not gate_lost.is_set()
            and time.monotonic() < deadline
        ):
            token = self.queues.acquire_allocator_lock(compute_type)
            if token:
                return token
            self._wait_for_event(min(retry_seconds, max(0.0, deadline - time.monotonic())))
            retry_seconds = min(0.2, retry_seconds * 1.5)
        return None

    def _wait_for_assigned_orphans(
        self,
        compute_type: str,
        deadline: float,
        gate_lost: threading.Event,
    ) -> bool:
        while (
            not self._stop_event.is_set()
            and self._has_write_authority()
            and not gate_lost.is_set()
            and time.monotonic() < deadline
        ):
            snapshot = self.provider.list_buffer_snapshot(compute_type)
            if snapshot["assigned_with_replicaset_owner"] == 0:
                return True
            self._wait_for_event(min(0.5, max(0.0, deadline - time.monotonic())))
        return False

    def _wait_for_scale_down_observation(
        self,
        compute_type: str,
        desired: int,
        deadline: float,
        gate_lost: threading.Event,
    ) -> bool:
        while (
            not self._stop_event.is_set()
            and self._has_write_authority()
            and not gate_lost.is_set()
            and time.monotonic() < deadline
        ):
            snapshot = self.provider.list_buffer_snapshot(compute_type)
            if snapshot["buffer_available"] <= desired:
                return True
            self._wait_for_event(min(0.5, max(0.0, deadline - time.monotonic())))
        return False

    def _wait_for_event(self, timeout: float) -> None:
        if timeout <= 0:
            return
        with self._condition:
            if self._stop_event.is_set():
                return
            self._condition.wait(timeout=timeout)

    def _record_status(self, type_name: str, **fields) -> None:
        with self._status_lock:
            current = dict(self._status.get(type_name, {}))
            current.update(fields)
            current["observed_at"] = datetime.now(timezone.utc).isoformat()
            self._status[type_name] = current
