import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set

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
        queued: Optional[int] = None,
    ) -> int:
        """available 파드를 몇 개 들고 있을지.

        queued=None 이 고정 정책이고 기본값이다. 동적 경로를 끈 실행은 예전과 한
        글자도 다르지 않다.

        대기 중인 티켓 수를 그대로 목표로 쓴다. 다른 신호를 세 번 시도했지만
        (도착률, 반납률, 큐 유무) 앞의 둘은 공급 병목에서 리틀의 법칙 항등식이 되어
        하한에 붙었고, 셋째는 큐가 1건만 있어도 자리를 다 채워 배정보다 많은 파드를
        버렸다. 큐 길이는 공급이 모자란 만큼 쌓이므로 그 셋과 달리 눈이 멀지 않는다.

        목표가 수요를 따라 출렁이는 것은 축소 쿨다운이 막는다. 상승은 즉시, 하강은
        쿨다운 뒤 - 모자란 비용(사용자가 기다림)과 과한 비용(파드 한 개)이 다르므로
        비대칭이 맞다.

        자리는 N - assigned 가 막는다. 할당과 ReplicaSet 의 backfill 사이에서 합이
        잠깐 N 을 넘지만 (실측 한 번에 중앙값 3.4초) 곧 되돌아오는 일시적 현상이다.
        """
        room = max(0, int(buffer_capacity) - int(assigned))
        if queued is None:
            return min(int(buffer_reserve), room)
        return min(max(int(buffer_reserve), int(queued)), room)

    def _queued_for_target(self, compute_type: str) -> Optional[int]:
        """대기 중인 티켓 수. 고정 정책을 유지할 때는 None.

        Redis 가 흔들리면 이번 패스만 고정 정책으로 물러난다. R 개를 드는 것은 언제나
        안전한 답이고 다음 패스가 다시 읽는다.
        """
        if not self.dynamic_reserve:
            return None
        try:
            return int(self.queues.queued_count(compute_type))
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
            local = list(self._churn_state(compute_type)["creates"])
        creates = sorted(self._shared_or_local(compute_type, "create", local))
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
                # 이 프로세스가 이 타입의 목표를 보기 시작한 시각. targets 가 쿨다운
                # 창을 아직 덮지 못했는지 판정한다.
                "observing_since": None,
            },
        )

    def _share_sample(self, compute_type: str, kind: str, value: float) -> None:
        """표본을 컨트롤러 사이에 공유한다. 실패는 무시한다.

        쿨다운 표본이 프로세스 메모리에만 있으면 컨트롤러가 재시작되거나 리더가 바뀔 때
        사라지고, 다시 열 개가 쌓일 때까지 쿨다운이 0 이 되어 수요 축소가 무조건 통과한다.
        표본 자체는 "이 클러스터에서 파드를 세우는 데 걸리는 시간" 과 "수요가 가라앉아
        있는 시간" 이라 누가 관측했는지와 무관하므로 공유해도 된다.

        쓰는 것은 리더만 한다. 양쪽이 같은 파드의 Ready 를 보고 각자 넣으면 같은 값이
        두 번 들어가고, 중복 제거는 프로세스별이라 막지 못한다. 읽기는 둘 다 한다 -
        리더가 바뀐 쪽이 이어서 쓰는 것이 이 수정의 목적이다.

        실험 시작 때는 reset_server 가 Redis 를 비우므로 표본이 없는 상태로 출발한다.
        그것은 의도된 것이다(매 실행을 같은 조건에서 시작). 이 수정이 덮는 것은 실행
        중간의 재시작과 리더 교체다.
        """
        try:
            if not self._has_write_authority():
                return
            self.queues.record_churn_sample(
                compute_type, kind, float(value), self.cooldown_samples
            )
        except Exception as exc:
            logger.debug(
                "[BufferChurnSampleSkipped] compute_type=%s kind=%s reason=%r",
                compute_type,
                kind,
                str(exc),
            )

    def _shared_or_local(
        self,
        compute_type: str,
        kind: str,
        local: List[float],
    ) -> List[float]:
        """공유 표본이 더 많으면 그것을 쓰고, 읽을 수 없으면 메모리 쪽으로 물러선다."""
        try:
            shared = self.queues.churn_samples(compute_type, kind)
        except Exception:
            return local
        return shared if len(shared) >= len(local) else local

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
            local_dips = list(self._churn_state(compute_type)["dips"])
        dips = sorted(self._shared_or_local(compute_type, "dip", local_dips))

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
        shared_dip = None
        with self._churn_lock:
            state = self._churn_state(compute_type)
            if state["observing_since"] is None:
                state["observing_since"] = now

            previous = state["last_target"]
            if previous is not None:
                if state["dip_started_at"] is None:
                    if target < previous:
                        state["dip_started_at"] = now
                elif target > previous:
                    dip = now - state["dip_started_at"]
                    state["dips"].append(dip)
                    if len(state["dips"]) > self.cooldown_samples:
                        del state["dips"][: len(state["dips"]) - self.cooldown_samples]
                    state["dip_started_at"] = None
                    shared_dip = dip
            state["last_target"] = target

            targets = state["targets"]
            targets.append((now, target))
            while targets and targets[0][0] < cutoff:
                targets.pop(0)
        if shared_dip is not None:
            self._share_sample(compute_type, "dip", shared_dip)

    def _cooldown_blocks_scale_down(
        self,
        compute_type: str,
        desired: int,
        cooldown: float,
    ) -> Optional[float]:
        """아직 기다려야 하는 초, 또는 줄여도 될 때 None.

        창 안에서 목표가 지금보다 높았던 적이 있으면 아직 출렁이는 중이므로 기다린다.
        내내 지금 이하였다면 수요가 가라앉은 것이고, 그때의 축소는 되돌려지지 않는다.

        이 함수가 막아도 N 이 열린 채로 남지는 않는다. replicas 중 N - assigned 를
        넘는 부분은 호출자가 먼저 줄이고, 여기서 기다리는 것은 그 아래 부분뿐이다.
        """
        if cooldown <= 0:
            return None
        now = time.monotonic()
        with self._churn_lock:
            state = self._churn_state(compute_type)
            state["cooldown"] = cooldown
            observing_since = state["observing_since"]
            # 이력이 창을 아직 덮지 못했으면 "창 안에 더 높은 목표가 없었다" 를 말할 수
            # 없다. 이 프로세스가 방금 떴거나 리더가 막 바뀐 상태이고, 직전 리더가 1초
            # 전에 본 높은 목표를 우리는 모른다. 덮을 때까지는 기다린다 - 수요 축소를
            # 미루는 쪽은 파드를 들고 있는 방향이라 되돌릴 수 있고, 반대로 통과시키면
            # 되돌릴 수 없는 삭제가 된다. N 보장은 용량 트림이 따로 하므로 이 보류가
            # 한도를 열어 두지는 않는다.
            if observing_since is not None:
                observed = now - observing_since
                if observed < cooldown:
                    state["held_back"] += 1
                    return cooldown - observed
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
        self._share_sample(compute_type, "create", seconds)

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

    def _schedule_after(self, delay: float, compute_type: Optional[str] = None) -> None:
        """쿨다운이 끝나는 시점에 깨어난다. 이벤트를 기다리지 않는다.

        이 예약이 없으면 조용해진 버퍼가 남은 파드를 다음 resync(기본 60초)까지
        들고 있는다.

        타입을 함께 적어야 한다. 예약 실행은 _pending_types 에 적힌 타입만 돌리므로
        (아래 _run 의 scheduled_run 분기) 깨우기만 하면 아무것도 reconcile 하지 않고
        다음 이벤트나 resync 까지 밀린다. 용량 트림이 "남은 축소는 쿨다운 뒤에 다시
        본다" 를 이 예약에 기대므로, 조용한 구간에서 쿨다운이 60초까지 늘어나면
        측정한 쿨다운 정책과 다른 것을 재게 된다.
        """
        with self._condition:
            if compute_type:
                self._pending_types.add(compute_type)
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
            queued = self._queued_for_target(compute_type_value)
            desired = self.desired_replicas(
                policy["R"],
                policy["N"],
                snapshot["buffer_assigned"],
                queued,
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

            # 목표를 먼저 적는다. 확대 패스보다 앞이어야 한다 - 전에는 축소 쪽에만
            # 있어서, 올라간 목표(스파이크)가 이력에 남지 않았다. 그러면 쿨다운의
            # "창 안에 지금보다 높은 목표가 있었나" 가 거짓이 되어 직후 트림이
            # 그대로 통과한다. 실측에서 42->73 확대 직후의 73->69 축소가 이 경로로
            # 쿨다운을 빠져나갔다.
            cooldown = self.cooldown_seconds(compute_type_value)
            self._note_target(compute_type_value, desired, cooldown)

            if current < desired:
                result = self._scale_up(
                    compute_type_value,
                    base_result,
                )
                self._record_status(compute_type_value, **result)
                return result

            # 축소는 두 부분으로 나눠 다룬다.
            #
            #   replicas 중 N - assigned 를 넘는 부분 - 쿨다운 없이 바로 줄인다.
            #       할당된 파드는 selector 에서 빠지고 ReplicaSet 이 그 자리를 새
            #       파드로 채우므로, replicas 를 그대로 두면 기다리는 내내 N 을
            #       넘는다. 몇 초 뒤 저절로 풀리는 backfill 지연과 다르다.
            #   N - assigned 아래에서 목표까지 내려가는 부분 - 쿨다운을 거친다.
            #       수요가 줄어서 생긴 축소라 곧 되돌려질 수 있다.
            #
            # 801f39f 는 앞부분을 목표까지 한 번에 줄였고, _scale_down 이 락을 잡은
            # 뒤 목표를 다시 계산해 큐가 빈 순간 "75로 트림" 이 "2로 붕괴" 가 됐다
            # (실측 66건 중 25건). 즉시 줄이는 폭을 N - assigned 까지로 묶고 재계산을
            # 없애면 그 붕괴가 생기지 않는다. 같은 큐 길이 신호에 이 우회가 있던
            # 3a4f71e 는 Ready 후 쓰이지 않은 파드가 5.6% 였다.
            #
            # 판정은 replicas 로 한다. 실제 파드 합(buffer_total)이 N 을 넘는 것은
            # 할당과 backfill 사이의 일시적 현상이라 (한 번에 중앙값 3.4초) 그것으로
            # 줄이면 ReplicaSet 이 지우고 있는 파드를 한 번 더 지우게 된다.
            room = max(0, int(policy["N"]) - int(snapshot["buffer_assigned"]))
            logger.info(
                "[BufferTarget] compute_type=%s queued=%s desired=%s current=%s "
                "room=%s assigned=%s available=%s total=%s R=%s N=%s dynamic=%s",
                compute_type_value,
                queued,
                desired,
                current,
                room,
                snapshot["buffer_assigned"],
                snapshot["buffer_available"],
                snapshot["buffer_total"],
                policy["R"],
                policy["N"],
                self.dynamic_reserve,
            )

            if current > desired or snapshot["buffer_available"] > desired:
                waiting = self._cooldown_blocks_scale_down(
                    compute_type_value, desired, cooldown
                )
                if waiting is not None and current > room:
                    logger.info(
                        "[BufferCapacityTrim] compute_type=%s replicas=%s->%s "
                        "desired=%s assigned=%s N=%s cooldown_remaining_s=%.1f",
                        compute_type_value,
                        current,
                        room,
                        desired,
                        snapshot["buffer_assigned"],
                        policy["N"],
                        waiting,
                    )
                    result = self._scale_down(
                        compute_type_value,
                        policy,
                        {
                            **base_result,
                            "desired_replicas": room,
                            "target_replicas": desired,
                            "capacity_trim": True,
                        },
                    )
                    self._record_status(compute_type_value, **result)
                    # 목표까지 남은 축소는 쿨다운이 끝나면 다시 본다.
                    self._schedule_after(waiting, compute_type_value)
                    return result
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
                    self._schedule_after(waiting, compute_type_value)
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
                self._queued_for_target(compute_type),
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
                # 결정 시점의 목표는 "이보다 더 줄이지 않는다" 는 바닥으로만 쓴다.
                # 이 지점까지 오는 데 게이트와 락으로 수초가 걸리고 (실측 2.2~17.6초)
                # 그 사이 큐가 비면 다시 계산한 목표가 하한으로 떨어진다. 그러면
                # "75로 트림" 으로 시작한 축소가 "2로 붕괴" 로 실행된다 - 실측 66건 중
                # 25건이 그랬고 524개를 더 지웠다.
                #
                # 반대 방향도 막아야 한다. 기다리는 동안 상태는 한쪽으로만 움직인다 -
                # 게이트가 서 있으면 할당기는 새로 할당하지 않고(allocator 의
                # is_scale_down_gated) 반납은 게이트를 보지 않으므로, assigned 는 줄고
                # 자리는 늘고 큐는 쌓인다. 결정 시점 값만 그대로 실행하면 그사이
                # 필요해진 파드를 지웠다가 다음 패스에서 다시 만든다 (고정 R 에서
                # 자리 1 -> 2 로 늘었는데 1 로 줄여 Ready 파드를 버리는 경우, 동적 R
                # 에서 큐가 30 건 쌓였는데 2 로 붕괴하는 경우).
                #
                # 그래서 지금 값으로 다시 계산한 목표를 바닥에 깔고, 자리(N - assigned)
                # 로 천장을 씌운다. 고정 정책에서는 이 식이 예전의 재계산과 정확히 같고,
                # 동적 경로에서는 붕괴 방지만 더해진다.
                room = max(0, int(policy["N"]) - int(snapshot["buffer_assigned"]))
                fresh = self.desired_replicas(
                    policy["R"],
                    policy["N"],
                    snapshot["buffer_assigned"],
                    self._queued_for_target(compute_type),
                )
                desired = min(
                    max(int(base_result["desired_replicas"]), fresh),
                    room,
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
