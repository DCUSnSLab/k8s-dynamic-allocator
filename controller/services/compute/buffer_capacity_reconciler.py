import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Set, Tuple

from config import settings

from ..queue import QueueUnavailableError

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

        self.policy_ready_ttl_seconds = max(
            1.0, float(settings.BUFFER_POLICY_READY_TTL_SECONDS)
        )

        self._condition = threading.Condition()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._restart_thread: Optional[threading.Thread] = None
        self._pending_types: Set[str] = set()
        self._policy_refresh_pending = True
        self._next_run_at: Optional[float] = None
        self._known_policy_types: Set[str] = set()
        self._last_queued_counts: Dict[str, int] = {}
        self._retry_counts: Dict[str, int] = {}
        self._policy_retry_count = 0
        self._policy_ready_token = ""
        self._policy_ready = False
        self._last_policy_sync_succeeded = False
        # 깃발 게시·해제·갱신은 reconcile 스레드와 갱신 타이머 두 곳에서 일어난다.
        # 상태 전이와 Redis 쓰기를 한 단위로 묶어야, 늦게 끝난 갱신 실패가 방금
        # 게시한 깃발의 로컬 상태를 지우는 일이 없다.
        self._policy_ready_lock = threading.Lock()
        self._policy_ready_renewed_at: Optional[float] = None
        self._renewer_generation = 0
        # Deployment 이름별 (compute-type 라벨, R, N). replicas·status 만 바뀐
        # 이벤트로 정책 새로고침을 하지 않기 위한 비교 기준이다.
        self._policy_fingerprints: Dict[str, Tuple[str, ...]] = {}
        # 깃발 게시를 막는 두 경우만 남긴다. (1) 리더가 된 뒤 첫 게시는 그 바퀴의
        # reconcile 이 재시도 없이 끝나야 한다 - 이전 리더가 남긴 상태를 한 번은
        # 정리하고 연다. (2) 축소 뒤 ReplicaSet 삭제를 관측하지 못한 타입은 그
        # 타입의 reconcile 이 재시도 없이 끝날 때까지 막는다. 둘 다 아닌 reconcile
        # 실패는 깃발에 영향을 주지 않는다.
        self._initial_publish_pending = True
        # 타입 -> 축소 PATCH 로 보낸 replicas.
        self._policy_ready_holds: Dict[str, int] = {}

        # ---- 공급(refill): replicas 올리기 + 게이트 해제
        #
        # reconcile 바퀴와 할당 락에서 떼어 낸 리더 전용 작업이다. 그 둘에 묶여
        # 있던 시절 포화 구간에서 게이트 대기가 평균 5.0초, replicas 증가의 46% 가
        # 할당 락 timeout 이었다(10/11 run 051521). 전용 스레드 하나에서 돌고
        # 공급 락만 잡는다. 파드 이벤트로 바로 깨어난다.
        self.refill_batch_limit = max(
            1, int(getattr(settings, "BUFFER_REFILL_BATCH_LIMIT", 4) or 4)
        )
        # 실패·충돌·후보 없음 뒤의 재예약. 지수 백오프를 쓰지 않는다 - 포화에서는
        # 몇 초 밀리는 것이 그대로 슬롯 손실이다.
        self.refill_retry_seconds = 0.5
        # 같은 순간 몰려오는 파드 이벤트를 한 바퀴로 합친다.
        self.refill_debounce_seconds = 0.05
        # 공급 락 대기. 공급 락을 다투는 것은 축소·정책 게시뿐이라 짧게 둔다.
        self.supply_lock_wait_seconds = 0.5
        self._refill_condition = threading.Condition()
        self._refill_due: Dict[str, float] = {}
        self._refill_requested_at: Dict[str, float] = {}
        self._refill_trigger: Dict[str, str] = {}
        self._refill_generation = 0
        self._refill_not_before = 0.0
        # 타입 -> {파드 이름: PATCH 가 test 한 resourceVersion}. 응답을 못 받은
        # 게이트 PATCH 는 서버에 반영됐을 수 있으므로 "풀었다" 로 센다.
        self._gate_unknown: Dict[str, Dict[str, str]] = {}
        # (타입, 파드) -> 처음 미상이 된 시각. 요청이 서버에 남아 있을 수 있는 시간은
        # apiserver 의 요청 timeout(기본 --request-timeout 60s)이 상한이다. 그 뒤에는
        # 서버가 요청을 끝냈으므로 더 적용될 수 없다. 그보다 오래된 미상은 정리해
        # 예산이 영구히 새지 않게 한다(재전송이 계속 거절되는 경우 등).
        self._gate_unknown_since: Dict[Tuple[str, str], float] = {}
        self.gate_unknown_max_age_seconds = 60.0
        # 리더를 잠깐 잃었다 되찾은 경우(같은 프로세스)를 가리기 위한 값. 그 사이
        # 다른 복제본이 리더였을 수 없는 시간 = lease 기간 - 갱신 주기
        # (lease 는 마지막 갱신 + 기간에 만료되고, 마지막 갱신은 멈춘 시각보다 최대
        # 갱신 주기만큼 이르다). leader.py 와 같은 env 를 읽는다.
        self._refill_stopped_at: Optional[float] = None
        self._same_leader_window_seconds = max(
            0.0,
            float(os.getenv("LEASE_DURATION_SECONDS", "15"))
            - float(os.getenv("LEASE_RENEW_INTERVAL_SECONDS", "5")),
        )
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
        """R, 대기 수요, 남은 N 자리로 available 목표를 정한다."""
        return BufferCapacityReconciler.desired_replicas_detail(
            buffer_reserve,
            buffer_capacity,
            assigned,
            queued,
        )["desired"]

    @staticmethod
    def desired_replicas_detail(
        buffer_reserve: int,
        buffer_capacity: int,
        assigned: int,
        queued: Optional[int] = None,
    ) -> Dict:
        """목표 복제 수와 그것을 제한한 항목을 돌려준다.

        queued=None 은 고정 정책이다. 동적 정책은 대기 수요를 R 이상으로 반영하고
        두 정책 모두 N-assigned 를 넘지 않는다. 생성 파이프라인 추정치는 Deployment
        복제 수와 단위가 달라 목표의 상한이나 하한으로 사용하지 않는다.
        """
        reserve = int(buffer_reserve)
        room = max(0, int(buffer_capacity) - int(assigned))
        if queued is None:
            if room < reserve:
                return {"desired": room, "bound_by": "room"}
            return {"desired": reserve, "bound_by": "reserve"}

        want = int(queued)
        bound = "queued"
        if reserve >= want:
            want = reserve
            bound = "reserve"
        if room < want:
            return {"desired": room, "bound_by": "room"}
        return {"desired": want, "bound_by": bound}

    @staticmethod
    def _defer_demand_scale_down(
        current: int,
        room: int,
        desired: int,
        available: int,
        starting: int,
        starting_age_seconds: Optional[float],
        grace_seconds: float,
    ) -> bool:
        """수요가 줄어 생긴 축소를, 방금 들인 파드가 Ready 가 될 때까지만 미룬다.

        게이트를 푼 직후의 파드를 지우면 노드가 이미 시작한 일을 버리게 되고, 그
        삭제가 같은 노드에서 세우는 중인 다른 파드까지 밀어낸다 (10/9 실측에서
        취소 236건, 생성 후 중앙값 1.5초). 그래서 "들였지만 아직 Ready 가 아닌"
        파드가 있으면 수요 축소를 미룬다.

        **반드시 나이 상한이 있어야 한다.** 상한이 없으면 끝내 Ready 가 되지 않는
        파드 하나가 축소를 영구히 막는다. 그 교착은 이론이 아니다:

          - 스케줄 불가 파드(노드 자리 없음)는 `status.startTime` 이 nil 이라
            cleanup 의 회수 판정(`_pod_not_ready_since`)이 None 을 돌려주고, 그
            함수는 "한 번도 Ready 가 된 적 없는 파드" 를 **의도적으로** 제외한다.
            즉 cleanup 이 치워 줄 것이라는 기대는 성립하지 않는다.
          - 축소가 영구히 막히면 replicas 가 고정되고, 그러면 게이트 해제 예산
            (min(replicas, N-assigned, desired))도 고정되어 남은 게이트 파드가
            영구히 안 풀린다. 그 상태에서는 롤링 업데이트의 maxUnavailable 이
            회복되지 않아 이미지 전개까지 멈춘다.

        상한은 grace_seconds 로 받는다. 호출자가 관측값으로 만든다
        (_starting_grace_seconds). 나이를 알 수 없으면 미루지 않는다 - 상한을
        걸 수 없는 보류는 위의 교착과 같아지고, 미루지 않아서 생기는 손해는
        되돌릴 수 있는 재생성뿐이다.

        나이의 기준은 파드 생성이 아니라 **게이트를 푼 시각**이다. 생성 시각으로
        재면 포화에서 오래 게이트에 묶여 있던 파드가 들이는 순간 이미 상한을
        넘어 보호를 전혀 못 받는다.

        용량 보정(current > room)은 이 보류를 타지 않는다. N 보장은 늦출 수 없다.
        """
        if starting <= 0:
            return False
        if current > room:
            return False
        if not (current > desired or available > desired):
            return False
        if starting_age_seconds is None:
            return False
        return float(starting_age_seconds) <= float(grace_seconds)

    def _starting_grace_seconds(self, compute_type: str) -> float:
        """들인 파드가 Ready 가 될 때까지 기다려 줄 시간. 관측값에서 만든다.

        건강한 파드가 Ready 가 되는 데 걸리는 시간의 p90 과, cleanup 이 미준비
        파드를 회수하는 기준(COMPUTE_NOT_READY_GRACE_SECONDS) 중 큰 쪽이다.

        p90 만 쓰면 혼잡 구간에서 상한이 너무 짧아진다 - 실측 생성 시간이 4초와
        12~21초의 두 봉으로 갈리므로, 표본이 빠른 봉에 몰린 순간의 p90 으로
        자르면 정상적으로 늦게 뜨는 파드를 지워 10/9 의 취소가 되살아난다.
        반대로 회수 기준만 쓰면 관측과 무관한 고정값이 된다. 큰 쪽을 쓰면 둘 중
        어느 쪽이 커도 안전하다.

        표본이 모자라면 p90 이 0 이고 회수 기준만 남는다. 기동 직후가 그 상태다.
        """
        observed = self.create_time_seconds(compute_type)
        grace = float(getattr(settings, "COMPUTE_NOT_READY_GRACE_SECONDS", 0) or 0)
        return max(float(observed or 0.0), grace)

    @staticmethod
    def _held_available(snapshot: Dict) -> int:
        """목표와 견줄 "들고 있는 미할당 파드" 수. 게이트 걸린 파드는 뺀다.

        게이트를 도입하면서 buffer_available 의 의미가 바뀌었다. 이제 그 안에는
        아직 노드에 올라가지도 않은 게이트 파드가 섞여 있고, ReplicaSet 은
        selector 수가 replicas 보다 적으면 곧바로 그런 파드를 하나 만든다. 그래서
        포화에서는 buffer_available = room + backfill > desired = room 이 거의
        항상 참이 된다.

        그 값을 목표와 비교하던 자리들을 그대로 두면 이런 일이 생긴다:
          - 줄일 것이 없는데 축소 분기에 들어가고,
          - _scale_down 이 **축소 게이트를 쥔 채** "available 이 목표 아래로
            내려오는 것" 을 최대 10초 기다린다. 그 10초 동안 모든 복제본의 할당이
            capacity_blocked=scale_down 으로 막힌다.
          - 수렴 판정이 서지 않아 같은 일이 매 패스 반복된다.

        목표가 통제하려는 것은 노드 자원을 쥔 파드 수다. 게이트 파드는 자원을 0
        쓰고, replicas 를 내릴 때 가장 먼저(미스케줄 + deletion-cost -100) 지워질
        대상이므로 이 비교에 넣지 않는다.

        키가 없는 스냅샷(게이트 이전 형태)은 buffer_available 로 물러선다.
        """
        value = snapshot.get("ungated_available")
        if value is None:
            return int(snapshot["buffer_available"])
        return int(value)

    @staticmethod
    def _starting_age_seconds(oldest_started_at) -> Optional[float]:
        """가장 오래 들여둔 미준비 파드의 경과 시간. 알 수 없으면 None."""
        if not isinstance(oldest_started_at, datetime):
            return None
        started = oldest_started_at
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - started).total_seconds()

    def pipeline_width(self, compute_type: str, buffer_capacity: int) -> Optional[float]:
        """생성·보유 시간으로 계산한 포화 공급 폭. 진단 로그에만 사용한다."""
        create = self.create_time_seconds(compute_type, quantile=0.5)
        work = self.work_time_seconds(compute_type)
        if create <= 0 or work <= 0:
            return None
        return float(buffer_capacity) * create / (create + work)

    def work_time_seconds(self, compute_type: str, quantile: float = 0.5) -> float:
        """할당된 파드가 자리를 쥐고 있던 시간의 분위수. 표본이 모자라면 0.

        releaser 가 반납할 때마다 남기는 session_ms 가 표본이다.
        """
        works = sorted(self._shared_or_local(compute_type, "work", []))
        if len(works) < self.cooldown_min_samples:
            return 0.0
        return float(works[min(len(works) - 1, int(len(works) * quantile))])

    def _queued_for_target(self, compute_type: str) -> Optional[int]:
        """대기 중인 티켓 수. 고정 정책을 유지할 때는 None.

        동적 정책에서 조회가 실패하면 목표를 알 수 없으므로 이번 패스를 재시도한다.
        None 으로 바꾸면 대기 수요가 있어도 고정 R 까지 축소될 수 있다.
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
            raise QueueUnavailableError(
                f"Queued demand unavailable for {compute_type}: {exc}"
            ) from exc

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

    def _sample_count(self, compute_type: str, kind: str) -> int:
        """진단 로그에 기록할 생성·보유 표본 수."""
        local: List[float] = []
        if kind == "create":
            with self._churn_lock:
                local = list(self._churn_state(compute_type)["creates"])
        return len(self._shared_or_local(compute_type, kind, local))

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
            with self._policy_ready_lock:
                self._policy_ready_token = uuid.uuid4().hex
                self._policy_ready = False
                self._policy_ready_renewed_at = None
                try:
                    self.queues.clear_buffer_policy_ready()
                except QueueUnavailableError as exc:
                    logger.warning(
                        "[Warning] operation=buffer_policy_ready_invalidate reason=%r",
                        str(exc),
                    )
            self._last_policy_sync_succeeded = False
            # 새 리더는 이전 리더가 본 Deployment 를 모른다. 비워 두면 처음 오는
            # 이벤트(감시 재시작의 SYNC 포함)가 전부 새로고침을 일으킨다.
            self._policy_fingerprints.clear()
            self._initial_publish_pending = True
            self._policy_ready_holds.clear()
            self._stop_event.clear()
            self._policy_refresh_pending = True
            self._next_run_at = time.monotonic()
            self._thread = threading.Thread(
                target=self._run,
                name="buffer-capacity-reconciler",
                daemon=True,
            )
            self._thread.start()
            # 이전 갱신 타이머가 아직 깨어나지 않았어도 세대가 바뀌면 스스로 끝난다.
            # Event 만으로 끝내면 stop 직후 start 가 clear 한 사이에 옛 타이머가
            # 남거나, 반대로 새 타이머를 띄우지 못하는 경우가 생긴다.
            self._renewer_generation += 1
            threading.Thread(
                target=self._run_policy_ready_renewer,
                args=(self._renewer_generation,),
                name="buffer-policy-ready-renewer",
                daemon=True,
            ).start()
            # 공급 작업. 결과 미상 목록은 리더마다 따로라 새 리더는 이전 리더의
            # 응답 없는 게이트 PATCH 를 모른다. 그 요청이 늦게 반영될 수 있는 시간
            # (API 요청 timeout + 여유) 동안은 게이트를 풀지 않는다.
            #
            # 단 같은 프로세스가 잠깐 잃었다 되찾은 경우(lease 갱신 예외 한 번)는
            # 그 사이 다른 리더가 있을 수 없다. 이때는 자기 미상 목록을 그대로 쓰고
            # 기다리지 않는다 - 포화에서 갱신 예외마다 공급이 8초씩 멈추면 안 된다.
            with self._refill_condition:
                now = time.monotonic()
                same_leader = (
                    self._refill_stopped_at is not None
                    and now - self._refill_stopped_at < self._same_leader_window_seconds
                )
                if same_leader:
                    self._refill_not_before = now
                else:
                    self._gate_unknown.clear()
                    self._gate_unknown_since.clear()
                    self._refill_not_before = now + self._leader_refill_grace_seconds()
                self._refill_due.clear()
                self._refill_requested_at.clear()
                self._refill_trigger.clear()
                self._refill_generation += 1
                generation = self._refill_generation
            threading.Thread(
                target=self._run_refill,
                args=(generation,),
                name="buffer-refill",
                daemon=True,
            ).start()
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
        with self._refill_condition:
            self._refill_stopped_at = time.monotonic()
            self._refill_condition.notify_all()
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=max(2.0, self.wait_timeout_seconds + 1.0))
        with self._condition:
            if self._thread is thread and (not thread or not thread.is_alive()):
                self._thread = None
        self._clear_policy_ready_best_effort("stopped")
        logger.info("[BufferCapacityReconcilerStopped]")

    def on_buffer_event(self, event_type: str, pod, source: str = "watch") -> None:
        del source
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = labels.get(self.provider.LABEL_COMPUTE_TYPE)
        if compute_type:
            self._note_create_time(compute_type, pod)
            self.request_reconcile(compute_type)
            # 공급이 할 일이 생기는 순간들: ReplicaSet 이 게이트 파드를 만듦(ADDED),
            # 파드가 사라짐(DELETED), 삭제가 시작됨(deletionTimestamp - 반납된
            # 파드는 이때 assigned 에서 빠져 자리가 난다). 1초 타이머나 reconcile
            # 바퀴를 기다리지 않는다. Ready 같은 상태 변화는 공급과 무관해 거른다.
            if (
                event_type in ("ADDED", "DELETED")
                or getattr(metadata, "deletion_timestamp", None) is not None
            ):
                self.request_refill(compute_type, trigger="event")

    def _note_create_time(self, compute_type: str, pod) -> None:
        """파드가 Ready 가 된 순간, 스케줄 허용 이후 준비 시간을 표본에 넣는다.

        쿨다운의 바닥이 되는 값이다. Ready 이벤트는 같은 파드에 여러 번 오므로
        uid 로 한 번만 센다. 시각이 둘 다 있을 때만 쓰고, 없으면 조용히 넘어간다 -
        추정기는 표본이 모자라면 스스로 비켜선다.
        """
        try:
            if not self.provider._pod_is_ready(pod):
                return
            uid = getattr(getattr(pod, "metadata", None), "uid", None)
            metadata = getattr(pod, "metadata", None)
            created = getattr(metadata, "creation_timestamp", None)
            ready = self.provider._pod_ready_at(pod)
            if not uid or created is None or ready is None:
                return
            annotations = getattr(metadata, "annotations", None) or {}
            release_key = getattr(
                self.provider,
                "ANNOTATION_WARM_SLOT_RELEASED_AT",
                "k8s-dynamic-allocator/warm-slot-released-at",
            )
            released_at = annotations.get(release_key)
            started = (
                datetime.fromisoformat(str(released_at).replace("Z", "+00:00"))
                if released_at
                else created
            )
            seconds = (ready - started).total_seconds()
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
        del source
        if not self._has_write_authority() or self._stop_event.is_set():
            return
        metadata = getattr(deployment, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = self.queues.normalize_compute_type(
            labels.get(self.provider.LABEL_COMPUTE_TYPE)
        )
        self.request_reconcile(compute_type)
        if self._policy_inputs_changed(event_type, deployment):
            self.request_policy_refresh()

    def _policy_inputs_changed(self, event_type: str, deployment) -> bool:
        """이 이벤트가 정책 새로고침을 해야 하는지 판단한다.

        replicas 를 바꿀 때마다 Deployment MODIFIED 가 온다. 예전에는 그때마다
        새로고침을 했고, 새로고침 바퀴의 reconcile 이 락 경합으로 미뤄지면 정책
        깃발이 내려가 모든 할당이 멈췄다(10/11 계측: 포화 구간의 약 3분의 1).
        generation 으로 거르면 안 된다 - replicas 변경도 spec 변경이라 오른다.

        sync_policies 가 읽는 입력은 Deployment 의 존재, compute-type 라벨,
        R/N 어노테이션, 그리고 유효성 판정에 쓰는 selector·템플릿 라벨이다.
        그래서 MODIFIED 가 아닌 이벤트(ADDED, DELETED, 감시 재시작의 SYNC)와
        처음 보는 Deployment 는 무조건 새로고침하고, MODIFIED 는 그 값들이
        바뀌었을 때만 한다. 놓치더라도 주기 resync 가 정책을 다시 읽는다.
        """
        metadata = getattr(deployment, "metadata", None)
        name = getattr(metadata, "name", None) or ""
        labels = getattr(metadata, "labels", None) or {}
        annotations = getattr(metadata, "annotations", None) or {}
        spec = getattr(deployment, "spec", None)
        selector = getattr(getattr(spec, "selector", None), "match_labels", None) or {}
        template_labels = (
            getattr(
                getattr(getattr(spec, "template", None), "metadata", None),
                "labels",
                None,
            )
            or {}
        )
        # parse_deployment_policy 는 selector 와 템플릿 라벨로 정책의 유효성도
        # 판정한다. 그것이 바뀌면 정책이 무효가 되거나 다시 유효해진다.
        fingerprint = (
            str(labels.get(self.provider.LABEL_COMPUTE_TYPE) or ""),
            str(annotations.get(self.provider.ANNOTATION_BUFFER_RESERVE) or ""),
            str(annotations.get(self.provider.ANNOTATION_BUFFER_CAPACITY) or ""),
            repr(sorted((str(k), str(v)) for k, v in dict(selector).items())),
            repr(sorted((str(k), str(v)) for k, v in dict(template_labels).items())),
        )
        with self._condition:
            if event_type == "DELETED":
                self._policy_fingerprints.pop(name, None)
                return True
            previous = self._policy_fingerprints.get(name)
            self._policy_fingerprints[name] = fingerprint
        if event_type != "MODIFIED" or not name:
            return True
        return previous != fingerprint

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
        next_demand_poll_at = time.monotonic()
        had_authority = True
        try:
            while not self._stop_event.is_set():
                if not self._has_write_authority():
                    self._clear_policy_ready_best_effort("leadership_lost")
                    had_authority = False
                    self._stop_event.wait(1.0)
                    continue
                if not had_authority:
                    # 리더 유효성이 잠깐 끊겼다 돌아오면 stop/start 콜백 없이
                    # 이어진다. 깃발은 새로고침 바퀴에서만 다시 게시되므로 여기서
                    # 바로 예약하지 않으면 주기 resync(60초)까지 할당이 멈춘다.
                    had_authority = True
                    self.request_policy_refresh()
                refresh_policies = False
                demand_poll_due = False
                compute_types: Set[str] = set()

                with self._condition:
                    while not self._stop_event.is_set():
                        now = time.monotonic()
                        due_at = next_resync_at
                        if self._next_run_at is not None:
                            due_at = min(due_at, self._next_run_at)
                        if self.dynamic_reserve and self._known_policy_types:
                            due_at = min(due_at, next_demand_poll_at)
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
                    demand_poll_due = (
                        self.dynamic_reserve
                        and bool(self._known_policy_types)
                        and now >= next_demand_poll_at
                    )
                    if demand_poll_due:
                        next_demand_poll_at = now + 1.0

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
                    publish_policy_ready = (
                        self._last_policy_sync_succeeded
                        and not self._stop_event.is_set()
                        and self._has_write_authority()
                    )
                    if not publish_policy_ready:
                        self._clear_policy_ready_best_effort("policy_sync_failed")
                        self.request_policy_refresh(retry=True)

                if demand_poll_due:
                    # This leader-side poll observes tickets enqueued through a
                    # different controller replica, even while its queue worker
                    # is occupied by mounts. Reconcile only changed demand.
                    compute_types.update(self._poll_demand_changes())

                # 깃발 갱신은 _run_policy_ready_renewer 가 따로 한다. 여기서 하면
                # 한 바퀴가 TTL 보다 길어질 때(10/11 계측: 최대 17초) 갱신이
                # 빠져 깃발이 만료되고 할당이 멈춘다.
                initial_reconcile_failed = False
                for compute_type in sorted(compute_types):
                    if (
                        self._stop_event.is_set()
                        or not self._has_write_authority()
                    ):
                        break
                    pass_started = time.monotonic()
                    result = self.reconcile_type(compute_type)
                    reconciled_at = time.monotonic()
                    # 공급(replicas 올리기 + 게이트 해제)은 이 바퀴에서 하지 않는다.
                    # 바퀴가 축소 관측으로 십수 초씩 걸리는 동안 공급이 멈추고, 락
                    # 경합으로 retry 가 나오면 그 바퀴는 게이트를 아예 안 풀었다.
                    # 여기서는 공급 작업을 깨우기만 한다(싸다 - 합쳐진다).
                    if not result.get("retry") and result.get("status") != "blocked":
                        self.request_refill(compute_type, trigger="reconcile")
                    timing.emit(
                        "ReconcilePass",
                        compute_type=compute_type,
                        resync=int(periodic_resync),
                        scheduled=int(scheduled_run),
                        demand_poll=int(demand_poll_due),
                        status=result.get("status", "-"),
                        reason=result.get("reason", "-"),
                        reconcile_ms=(reconciled_at - pass_started) * 1000,
                    )
                    # reconcile 의 결과(미뤄짐이든 오류든)는 reconcile 재시도만
                    # 일으키고 정책 깃발에는 손대지 않는다. 깃발은 할당의 1번
                    # 관문이라, 내리면 두 복제본의 모든 티켓이 policy_not_ready 로
                    # 돌아간다. 그런데 reconcile 실패는 대부분 일시적이다 - 락
                    # 경합(allocator_lock_timeout), 아직 안 끝난 확대·축소, API
                    # timeout. 10/11 계측에서 이 경로로 포화 구간의 약 3분의 1 동안
                    # 할당이 멈췄다. 깃발이 지켜야 하는 것은 "정책이 동기화됐다" 뿐이고
                    # 그것은 sync_policies 의 성공 여부가 판정한다. N 은 할당이 락
                    # 안에서 N - assigned 를 다시 세고 게이트 예산이 노드 위 파드를
                    # 묶으므로, reconcile 이 늦어도 넘지 않는다.
                    # 예외는 __init__ 의 _initial_publish_pending / _policy_ready_holds
                    # 두 가지뿐이다.
                    if result.get("retry"):
                        self.request_reconcile(compute_type, retry=True)
                        if (
                            self._initial_publish_pending
                            and compute_type in ready_candidate_types
                        ):
                            initial_reconcile_failed = True
                        continue

                    with self._condition:
                        hold_target = self._policy_ready_holds.get(compute_type)
                    if hold_target is not None and self._scale_down_hold_resolved(
                        compute_type, hold_target
                    ):
                        with self._condition:
                            self._policy_ready_holds.pop(compute_type, None)
                        self.request_policy_refresh()
                    elif self._initial_publish_pending and not refresh_policies:
                        # 첫 게시가 막혀 백오프(최대 60초) 중이어도, 깨끗한 바퀴가
                        # 나오면 그 백오프를 기다리지 않고 바로 다시 게시를 시도한다.
                        self.request_policy_refresh()
                    with self._condition:
                        self._retry_counts.pop(compute_type, None)

                if publish_policy_ready:
                    with self._condition:
                        held = bool(self._policy_ready_holds)
                    blocked_reason = None
                    if (
                        self._stop_event.is_set()
                        or not self._has_write_authority()
                    ):
                        blocked_reason = "leadership_lost"
                    elif initial_reconcile_failed:
                        blocked_reason = "initial_reconcile_pending"
                    elif held:
                        blocked_reason = "scale_down_unobserved"
                    if blocked_reason:
                        self._clear_policy_ready_best_effort(blocked_reason)
                        self.request_policy_refresh(retry=True)
                    else:
                        try:
                            if not self._publish_policy_ready("policy_synced"):
                                continue
                            self._initial_publish_pending = False
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
                            with self._policy_ready_lock:
                                self._policy_ready = False
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
            self._clear_policy_ready_best_effort("reconciler_exit")
            with self._condition:
                if self._thread is threading.current_thread():
                    self._thread = None

    def _poll_demand_changes(self) -> Set[str]:
        changed: Set[str] = set()
        for compute_type in sorted(self._known_policy_types):
            try:
                queued = int(self.queues.queued_count(compute_type))
            except Exception as exc:
                logger.warning(
                    "[Warning] operation=buffer_demand_poll "
                    "compute_type=%s reason=%r",
                    compute_type,
                    str(exc),
                )
                continue
            with self._condition:
                previous = self._last_queued_counts.get(compute_type)
                self._last_queued_counts[compute_type] = queued
            if previous is None or previous != queued:
                changed.add(compute_type)
        return changed

    # ---------------------------------------------------------------- 공급(refill)

    def _leader_refill_grace_seconds(self) -> float:
        """새 리더가 게이트를 풀기 전에 기다리는 시간.

        이전 리더가 응답을 못 받은 게이트 PATCH 는 그 리더의 결과 미상 목록에만
        있다. 그 요청이 늦게 반영될 수 있는 시간(연결 + 읽기 timeout) 동안 풀지
        않으면, 새 리더가 같은 자리에 하나 더 풀어 N+1 이 되는 일이 없다.
        """
        timeout = getattr(self.provider, "api_request_timeout", None)
        try:
            if isinstance(timeout, (tuple, list)):
                seconds = float(sum(float(value) for value in timeout))
            else:
                seconds = float(timeout)
        except (TypeError, ValueError):
            seconds = 7.0
        return max(0.0, seconds) + 1.0

    def request_refill(
        self,
        compute_type: str,
        *,
        delay: Optional[float] = None,
        trigger: str = "event",
    ) -> None:
        """공급 작업을 예약한다. 같은 타입의 요청은 가장 이른 시각 하나로 합쳐진다."""
        compute_type_value = self.queues.normalize_compute_type(compute_type)
        now = time.monotonic()
        due = now + (self.refill_debounce_seconds if delay is None else max(0.0, delay))
        with self._refill_condition:
            current = self._refill_due.get(compute_type_value)
            if current is None or due < current:
                self._refill_due[compute_type_value] = due
            self._refill_requested_at.setdefault(compute_type_value, now)
            self._refill_trigger.setdefault(compute_type_value, trigger)
            self._refill_condition.notify_all()

    def _run_refill(self, generation: int) -> None:
        """리더 전용 공급 스레드. 스레드가 하나라 프로세스 안의 공급은 직렬이다.

        공급 락은 리더가 바뀌는 순간의 겹침과 축소·정책 게시를 막는 용도다.
        reconcile 은 더 이상 replicas 를 올리지 않으므로 이 스레드가 죽으면 공급이
        조용히 멈춘다. 그래서 한 바퀴의 예외가 루프를 끝내지 않게 한다.
        """
        streaks: Dict[str, Tuple[str, int]] = {}
        while not self._stop_event.is_set() and generation == self._refill_generation:
            try:
                self._refill_round(generation, streaks)
            except Exception:
                logger.exception("[Failed] operation=buffer_refill_loop")
                if self._stop_event.wait(self.refill_retry_seconds):
                    return

    def _refill_round(self, generation: int, streaks: Dict[str, Tuple[str, int]]) -> None:
        batch = []
        with self._refill_condition:
            while (
                not self._stop_event.is_set()
                and generation == self._refill_generation
            ):
                now = time.monotonic()
                ready = [
                    compute_type
                    for compute_type, due in self._refill_due.items()
                    if due <= now
                ]
                if ready:
                    for compute_type in sorted(ready):
                        self._refill_due.pop(compute_type, None)
                        batch.append(
                            (
                                compute_type,
                                self._refill_trigger.pop(compute_type, "event"),
                                self._refill_requested_at.pop(compute_type, now),
                            )
                        )
                    break
                timeout = 1.0
                if self._refill_due:
                    timeout = max(0.0, min(self._refill_due.values()) - now)
                self._refill_condition.wait(timeout=timeout)
        for compute_type, trigger, requested_at in batch:
            if self._stop_event.is_set() or generation != self._refill_generation:
                return
            if not self._has_write_authority():
                continue
            try:
                result = self._refill(
                    compute_type, trigger, requested_at, generation
                )
            except Exception:
                logger.exception(
                    "[Failed] operation=buffer_refill compute_type=%s",
                    compute_type,
                )
                result = {"reason": "exception", "retry_after": self.refill_retry_seconds}
            retry_after = result.get("retry_after")
            if retry_after is None:
                streaks.pop(compute_type, None)
                continue
            # 같은 사유로 연달아 실패하면(ReplicaSet 이 파드를 못 만드는 등) 0.5초
            # 마다 목록을 읽지 않게 상한 2초까지 늘린다. 파드 이벤트는 여전히
            # 바로 깨우므로(가장 이른 예약이 이긴다) 정상 경로는 늦어지지 않는다.
            reason = result.get("reason", "-")
            previous = streaks.get(compute_type)
            count = previous[1] + 1 if previous and previous[0] == reason else 1
            streaks[compute_type] = (reason, count)
            if reason in ("no_candidate", "settling", "patch_failed", "exception"):
                retry_after = min(2.0, retry_after * (2 ** (count - 1)))
            self.request_refill(
                compute_type, delay=retry_after, trigger="retry"
            )

    def _refill(
        self,
        compute_type: str,
        trigger: str,
        requested_at: float,
        generation: Optional[int] = None,
    ) -> Dict:
        """계측 포장. RefillPass 한 줄에 이번 공급의 내역을 남긴다.

        예전 GateReleasePass(게이트 해제)와 ScaleUpTiming(replicas 증가)을 대신한다.
        """
        if not timing.ENABLED:
            return self._refill_impl(compute_type, requested_at, generation)
        with timing.scope(purpose="refill"), timing.phase_timer() as timer:
            result = None
            try:
                result = self._refill_impl(compute_type, requested_at, generation)
                return result
            finally:
                r = result or {}
                timing.emit(
                    "RefillPass",
                    compute_type=compute_type,
                    trigger=trigger,
                    reason=r.get("reason", "exception"),
                    raised=r.get("raised", 0),
                    trimmed=r.get("trimmed", 0),
                    cas_conflicts=r.get("cas_conflicts", 0),
                    released=r.get("released", 0),
                    unknown=r.get("unknown", 0),
                    budget=r.get("budget", "-"),
                    ungated=r.get("ungated", "-"),
                    candidates=r.get("candidates", "-"),
                    wake_to_patch_ms=r.get("wake_to_patch_ms", "-"),
                    total_ms=timer.total_ms(),
                )

    def _supply_still_valid(
        self, compute_type: str, token: str, generation: Optional[int] = None
    ) -> bool:
        # 세대: start() 가 _stop_event 를 다시 내리면, 목록 읽기·PATCH 에 걸려 있던
        # 옛 스레드가 깨어나 계속 쓸 수 있다. 세대가 바뀌었으면 거기서 멈춘다.
        return (
            not self._stop_event.is_set()
            and (generation is None or generation == self._refill_generation)
            and self._has_write_authority()
            and bool(self.queues.renew_supply_lock(compute_type, token))
            and not self.queues.is_scale_down_gated(compute_type)
        )

    def _settle_unknown_gates(self, compute_type: str, snapshot: Dict) -> int:
        """결과 미상 게이트 PATCH 를 방금 읽은 목록과 맞춰 보고, 남은 수를 돌려준다.

        목록 읽기는 consistent read 라 판단 근거가 된다. 그 파드가
          - 사라졌거나 삭제 중이면: 더 셀 것이 없다.
          - 게이트가 풀려 있으면: ungated_available 에 이미 들어 있다.
          - resourceVersion 이 PATCH 가 test 한 값과 다르면: 그 PATCH 는 test 에
            걸려 영원히 적용되지 않는다.
        셋 다 아니면(같은 버전에 게이트가 그대로) 아직 도착하지 않았을 수 있으므로
        계속 "풀었다" 로 센다. 목록에 파드 정보가 없으면 판단을 못 하므로 그대로 둔다.
        """
        with self._refill_condition:
            unknown = dict(self._gate_unknown.get(compute_type, {}))
        if not unknown:
            return 0
        pods = snapshot.get("pods")
        if pods is None:
            return len(unknown)
        by_name = {pod.get("name"): pod for pod in pods}
        remaining = {}
        now = time.monotonic()
        for name, version in unknown.items():
            since = self._gate_unknown_since.get((compute_type, name), now)
            if now - since > self.gate_unknown_max_age_seconds:
                logger.warning(
                    "[BufferGateUnknownExpired] compute_type=%s compute_pod=%s age_s=%.1f",
                    compute_type,
                    name,
                    now - since,
                )
                continue
            pod = by_name.get(name)
            if pod is None or pod.get("terminating"):
                continue
            if not pod.get("scheduling_gated"):
                continue
            if (pod.get("resource_version") or "") != version:
                continue
            remaining[name] = version
        with self._refill_condition:
            if remaining:
                self._gate_unknown[compute_type] = remaining
            else:
                self._gate_unknown.pop(compute_type, None)
            for name in unknown:
                if name not in remaining:
                    self._gate_unknown_since.pop((compute_type, name), None)
        return len(remaining)

    def _refill_impl(
        self,
        compute_type: str,
        requested_at: float,
        generation: Optional[int] = None,
    ) -> Dict:
        """공급 락 아래에서 replicas 를 목표까지 올리고 게이트를 예산만큼 푼다.

        N 안전: 노드 위 파드 수(게이트 풀린 available + assigned)를 늘리는 것은
        게이트 해제뿐이다. claim 은 게이트 풀린 available 하나를 assigned 로 바꿀
        뿐이라 합이 그대로이고, 반납(삭제)은 합을 줄인다. 그래서 게이트 해제끼리만
        직렬화하면 되고(공급 락), 할당 락은 잡지 않는다. 예산은 예전 그대로
        min(replicas, N - assigned, desired) 이다. 읽은 뒤 claim 이 끼어들어도
        "풀린 수 + assigned" 가 그대로라 예산을 넘지 않는다.

        replicas 쓰기는 CAS 로만 한다. 할당기가 claim 직후 replicas 를 내리는
        것(할당 락)과 동시에 일어날 수 있기 때문이다.
        """
        out: Dict = {
            "raised": 0,
            "trimmed": 0,
            "cas_conflicts": 0,
            "released": 0,
            "unknown": 0,
            "reason": "-",
            "retry_after": None,
        }

        def done(reason: str, retry: bool = False, delay: Optional[float] = None) -> Dict:
            out["reason"] = reason
            if retry:
                out["retry_after"] = self.refill_retry_seconds if delay is None else delay
            return out

        if self._stop_event.is_set() or not self._has_write_authority():
            return done("not_leader")
        grace = self._refill_not_before - time.monotonic()
        if grace > 0:
            return done("leader_grace", retry=True, delay=grace)
        policy = self.queues.get_buffer_policy(compute_type)
        if not policy:
            # 정책 문제는 reconcile·정책 동기화가 다룬다.
            return done("no_policy")
        if self.queues.is_scale_down_gated(compute_type):
            # 축소가 진행 중이다. Redis 키 하나만 보므로 자주 다시 봐도 싸다.
            return done("scale_down_gated", retry=True)

        for _ in range(max(1, int(policy["N"]) * 2)):
            # 바퀴마다 공급 락을 잡고 놓는다. 공급 락을 함께 쓰는 쪽(축소, 정책 게시,
            # 그리고 재사용 비교군의 반납)이 공급의 여러 바퀴 사이에 끼어들 수 있어야
            # 한다. 포화에서 공급이 락을 한 번에 몇 초씩 쥐면 그쪽이 굶는다.
            deadline = time.monotonic() + self.supply_lock_wait_seconds
            token = self._acquire_supply_lock_until(
                compute_type, deadline, threading.Event()
            )
            if not token:
                return done("supply_lock_timeout", retry=True)
            try:
                if not self._supply_still_valid(compute_type, token, generation):
                    return done("lost_or_gated", retry=True)
                policy = self.queues.get_buffer_policy(compute_type)
                if not policy:
                    return done("no_policy")
                # replicas 를 먼저, 목록을 나중에 읽는다. 반대 순서였을 때(d6cb40c) 두
                # 읽기 사이에 claim 과 할당기의 낮추기가 끼어들면, claim 전 목록(assigned
                # 가 하나 적음)으로 계산한 목표로 방금 낮춘 replicas 를 다시 올렸다.
                # 포화 6분 사전 점검에서 이 진동이 용량 교정 42번을 불렀다. 이 순서면
                # 낮추기가 replicas 읽기 뒤에 오면 CAS 가 실패하고, 앞에 왔다면 그 앞의
                # claim 이 나중에 읽은 목록에 이미 들어 있다. 목록 뒤 CAS 전에 claim 만
                # 끼어드는 좁은 틈은 남으므로 아래 room 초과 낮추기가 안전망이다.
                current = int(
                    self.provider.read_deployment_replicas(policy["deployment_name"])
                )
                snapshot = self.provider.list_buffer_snapshot(compute_type)
                pending_unknown = self._settle_unknown_gates(compute_type, snapshot)
                assigned = int(snapshot["buffer_assigned"])
                room = max(0, int(policy["N"]) - assigned)
                queued = self._queued_for_target(compute_type)
                desired = self.desired_replicas(policy["R"], policy["N"], assigned, queued)

                # 1) replicas 올리기. 계산은 예전 _scale_up 과 같다.
                if current < desired and self._held_available(snapshot) <= desired:
                    target = desired
                    # N 을 넘지 않는 마지막 관문. 배정은 available 을 assigned 로
                    # 옮길 뿐이지만 그 backfill 이 합을 올리므로, buffer_total 이 N 을
                    # 넘게 만드는 확대는 하지 않는다.
                    headroom = int(policy["N"]) - int(snapshot["buffer_total"])
                    if target - current > headroom:
                        capped = current + max(0, headroom)
                        logger.info(
                            "[BufferScaleUpCapped] compute_type=%s desired=%s->%s "
                            "current=%s total=%s N=%s unowned_available=%s",
                            compute_type,
                            target,
                            capped,
                            current,
                            snapshot["buffer_total"],
                            policy["N"],
                            snapshot.get("unowned_available", 0),
                        )
                        target = capped
                    if target > current:
                        if not self._supply_still_valid(compute_type, token, generation):
                            return done("lost_or_gated", retry=True)
                        if self.provider.raise_deployment_replicas(
                            policy["deployment_name"], current, target
                        ):
                            logger.info(
                                "[BufferScaled] compute_type=%s direction=up "
                                "replicas=%s->%s assigned=%s R=%s N=%s",
                                compute_type,
                                current,
                                target,
                                assigned,
                                policy["R"],
                                policy["N"],
                            )
                            out["raised"] += target - current
                            current = target
                        else:
                            # 그사이 할당기가 내렸다. 다시 읽고 다시 계산한다.
                            out["cas_conflicts"] += 1
                            if out["cas_conflicts"] >= 3:
                                return done("cas_conflict", retry=True)
                            continue

                # 2) replicas 가 room 을 넘으면 room 까지 내린다.
                #
                # 공급은 할당 락 없이 목록을 읽으므로, 읽은 뒤 claim 이 끼어들면
                # room 보다 높게 올릴 수 있다(할당기는 이미 낮추기를 마친 뒤라
                # 다시 보지 않는다). 기다리면 reconcile 의 용량 트림이 축소 게이트를
                # 세워 그동안 모든 claim 을 막는다. 여기서 할당기와 같은 CAS 낮추기를
                # 한다. room 까지만 내리고 게이트 풀린 파드는 room 이하이므로,
                # ReplicaSet 은 노드에 없는 게이트 파드(deletion-cost -100)부터 지운다.
                if current > room:
                    # 게이트 없이 내려도 되는 근거는 "게이트 풀린 파드 <= room" 이다. 그러면
                    # 지울 수만큼 노드에 없는 게이트 파드가 있고 ReplicaSet 은 그것부터
                    # 지운다(노드 미배치가 deletion-cost 보다 앞선 순위). 평소에는 N
                    # 불변식이 이를 보장하지만 운영자가 N 을 낮춘 직후에는 깨진다. 그때는
                    # Ready 파드(claim 중일 수도 있다)가 지워질 수 있으므로 여기서 내리지
                    # 않고 reconcile 의 축소(축소 게이트로 할당을 막는다)에 맡긴다.
                    if self._held_available(snapshot) > room:
                        return done("needs_scale_down")
                    if not self._supply_still_valid(compute_type, token, generation):
                        return done("lost_or_gated", retry=True)
                    if self.provider.lower_deployment_replicas(
                        policy["deployment_name"], current, room
                    ):
                        logger.info(
                            "[BufferRefillTrim] compute_type=%s replicas=%s->%s "
                            "assigned=%s n=%s",
                            compute_type,
                            current,
                            room,
                            assigned,
                            policy["N"],
                        )
                        out["trimmed"] += current - room
                    else:
                        out["cas_conflicts"] += 1
                        if out["cas_conflicts"] >= 3:
                            return done("cas_conflict", retry=True)
                    continue

                # 3) 게이트 해제.
                unowned = int(snapshot.get("unowned_available", 0))
                owned_available = max(0, int(snapshot["buffer_available"]) - unowned)
                if owned_available > current:
                    # replicas 를 내린 뒤 ReplicaSet 이 아직 지우지 않았다. 삭제
                    # 이벤트가 바로 다시 깨운다.
                    return done("settling", retry=True)
                if int(snapshot["buffer_total"]) > int(policy["N"]):
                    logger.warning(
                        "[BufferGateOvershoot] compute_type=%s total=%s N=%s "
                        "unowned_available=%s",
                        compute_type,
                        snapshot["buffer_total"],
                        policy["N"],
                        unowned,
                    )
                budget = min(current, room, desired)
                ungated = int(snapshot["ungated_available"])
                with self._refill_condition:
                    unknown = dict(self._gate_unknown.get(compute_type, {}))

                # 결과 미상 파드는 같은 resourceVersion 으로 다시 보낸다. 두 요청이
                # 같은 버전을 test 하므로 최대 하나만 적용된다 - 이중으로 세지 않는다.
                # 다시 보내지 않으면, 요청이 서버에 닿지도 않은 경우(연결 실패)에는
                # 그 파드의 버전이 바뀌지 않아 미상이 영구히 남고 예산 하나가 샌다.
                # 그 자리는 아래 free 계산에서 이미 빼 두었으므로 새 예산을 쓰지 않는다.
                gated_by_name = {
                    candidate["name"]: candidate
                    for candidate in snapshot["gated_available_candidates"]
                }
                for name, version in unknown.items():
                    candidate = gated_by_name.get(name)
                    if candidate is None or candidate["resource_version"] != version:
                        continue
                    if not self._supply_still_valid(compute_type, token, generation):
                        return done("lost_or_gated", retry=True)
                    try:
                        ok = self.provider.release_pod_scheduling_gate(
                            name, version, candidate["gate_index"]
                        )
                    except Exception as exc:
                        # 재전송이 실패해도(403 같은 확정 거절이라도) 첫 PATCH 는 아직
                        # 서버에서 처리 중일 수 있으므로 미상에서 빼지 않는다. 대신
                        # _settle_unknown_gates 가 나이 상한으로 정리해 예산이 영구히
                        # 새지 않게 한다. 재전송 자체가 응답 없이 끝났으면(timeout·5xx)
                        # 그 요청도 늦게 적용될 수 있으므로 나이를 그 시각부터 다시 잰다.
                        status = getattr(exc, "status", None)
                        if not (isinstance(status, int) and 400 <= status < 500):
                            with self._refill_condition:
                                self._gate_unknown_since[(compute_type, name)] = (
                                    time.monotonic()
                                )
                        continue
                    if ok:
                        with self._refill_condition:
                            entry = self._gate_unknown.get(compute_type, {})
                            entry.pop(name, None)
                            if not entry:
                                self._gate_unknown.pop(compute_type, None)
                            self._gate_unknown_since.pop((compute_type, name), None)
                        out["released"] += 1
                        logger.info(
                            "[BufferGateReleased] compute_type=%s compute_pod=%s "
                            "ungated=%s budget=%s assigned=%s n=%s",
                            compute_type,
                            name,
                            ungated + 1,
                            budget,
                            assigned,
                            policy["N"],
                        )

                candidates = [
                    candidate
                    for candidate in snapshot["gated_available_candidates"]
                    if candidate["name"] not in unknown
                ]
                out.update(budget=budget, ungated=ungated, candidates=len(candidates))
                free = budget - ungated - pending_unknown
                if free <= 0:
                    if unowned and snapshot["gated_available_candidates"]:
                        logger.info(
                            "[BufferGateDeferred] compute_type=%s reason=%r "
                            "ungated=%s budget=%s unowned_available=%s",
                            compute_type,
                            "unowned available Pods consume admission budget",
                            ungated,
                            budget,
                            unowned,
                        )
                    return done("budget_full")
                if not candidates:
                    # ReplicaSet 이 아직 게이트 파드를 만들지 않았다. ADDED 이벤트가
                    # 깨우지만 감시가 끊겨 있을 수 있으므로 재예약도 걸어 둔다.
                    return done("no_candidate", retry=True)

                # 후보를 하나만 보고 포기하지 않는다. 맨 앞 파드의 패치가 계속
                # 실패하면(외부에서 어노테이션이 바뀜, 막 사라지는 중) 그 뒤가 영구히
                # 안 풀린다. 실패한 것은 건너뛰고 다음 후보로 간다.
                limit = min(free, self.refill_batch_limit)
                released_now = 0
                for candidate in candidates:
                    if released_now >= limit:
                        break
                    if not self._supply_still_valid(compute_type, token, generation):
                        return done("lost_or_gated", retry=True)
                    try:
                        ok = self.provider.release_pod_scheduling_gate(
                            candidate["name"],
                            candidate["resource_version"],
                            candidate["gate_index"],
                        )
                    except Exception as exc:
                        status = getattr(exc, "status", None)
                        if isinstance(status, int) and 400 <= status < 500:
                            # 400/403/429 등은 서버가 거절했다는 확정 응답이다.
                            # 적용되지 않았으므로 미상으로 세지 않는다(403 같은 영구
                            # 오류에서 미상이 쌓여 예산이 새는 것을 막는다).
                            logger.warning(
                                "[BufferGateSkipped] compute_type=%s compute_pod=%s "
                                "reason=%s",
                                compute_type,
                                candidate["name"],
                                "http_%s" % status,
                            )
                            continue
                        # timeout·연결 오류·5xx: 서버에 반영됐는지 모른다. 반영됐다고
                        # 치고, 목록에서 결판이 날 때까지 예산에서 뺀다. 예전 코드는
                        # 여기서 그냥 빠져나가 다음 바퀴가 같은 자리에 하나 더 풀 수
                        # 있었다(N+1).
                        with self._refill_condition:
                            self._gate_unknown.setdefault(compute_type, {})[
                                candidate["name"]
                            ] = candidate["resource_version"]
                            self._gate_unknown_since.setdefault(
                                (compute_type, candidate["name"]), time.monotonic()
                            )
                        out["unknown"] += 1
                        logger.warning(
                            "[BufferGateUnknown] compute_type=%s compute_pod=%s "
                            "reason=%r",
                            compute_type,
                            candidate["name"],
                            str(exc),
                        )
                        return done("patch_unknown", retry=True)
                    if not ok:
                        logger.info(
                            "[BufferGateSkipped] compute_type=%s compute_pod=%s "
                            "reason=%s",
                            compute_type,
                            candidate["name"],
                            "patch_conflict_or_gone",
                        )
                        continue
                    if "wake_to_patch_ms" not in out:
                        out["wake_to_patch_ms"] = (time.monotonic() - requested_at) * 1000
                    released_now += 1
                    out["released"] += 1
                    logger.info(
                        "[BufferGateReleased] compute_type=%s compute_pod=%s "
                        "ungated=%s budget=%s assigned=%s n=%s",
                        compute_type,
                        candidate["name"],
                        ungated + released_now,
                        budget,
                        assigned,
                        policy["N"],
                    )
                if released_now == 0:
                    return done("patch_failed", retry=True)
                if released_now >= free:
                    return done("released")
                # 묶음 상한에 걸렸다. 목록을 다시 읽고 남은 예산을 본다 - 그사이 큐가
                # 비어 목표가 내려갔으면 더 풀지 않는다.
            finally:
                try:
                    self.queues.release_supply_lock(compute_type, token)
                except Exception as exc:
                    logger.warning(
                        "[Warning] operation=buffer_supply_lock_release "
                        "compute_type=%s reason=%r",
                        compute_type,
                        str(exc),
                    )
        return done("loop_end", retry=True)

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
        supply_token = None
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
            # 공급 작업도 정책의 N 으로 게이트 예산을 센다. 정책이 바뀌는 순간
            # 옛 N 으로 푸는 일이 없게 공급 락도 잡는다(할당 락 -> 공급 락 순서).
            supply_token = self._acquire_supply_lock_until(
                compute_type_value,
                deadline,
                threading.Event(),
            )
            if not supply_token:
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
            if supply_token:
                self.queues.release_supply_lock(compute_type_value, supply_token)
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

    def _publish_policy_ready(self, reason: str) -> bool:
        """깃발을 게시한다. 실패하면 QueueUnavailableError 를 그대로 올린다.

        권한 검사와 게시 사이에 리더를 잃었으면 게시하지 않고 False 를 돌려준다.
        """
        with self._policy_ready_lock:
            if self._stop_event.is_set() or not self._has_write_authority():
                return False
            was_ready = self._policy_ready
            self.queues.publish_buffer_policy_ready(self._policy_ready_token)
            self._policy_ready = True
            self._policy_ready_renewed_at = time.monotonic()
        if not was_ready:
            # 깃발이 없던 시간을 run 로그에서 구간으로 재려고 ms 시각을 싣는다.
            logger.info(
                "[BufferPolicyReadyPublished] at_ms=%d reason=%s",
                int(time.time() * 1000),
                reason,
            )
        return True

    def _clear_policy_ready_best_effort(self, reason: str = "-") -> None:
        with self._policy_ready_lock:
            token = self._policy_ready_token
            was_ready = self._policy_ready
            self._policy_ready = False
            self._policy_ready_renewed_at = None
            if token:
                try:
                    self.queues.clear_buffer_policy_ready(token)
                except QueueUnavailableError as exc:
                    logger.warning(
                        "[Warning] operation=buffer_policy_ready_clear reason=%r",
                        str(exc),
                    )
        if was_ready:
            logger.info(
                "[BufferPolicyReadyCleared] at_ms=%d reason=%s",
                int(time.time() * 1000),
                reason,
            )

    def _renew_policy_ready_once(self) -> None:
        """게시된 깃발의 TTL 을 한 번 연장한다.

        연장이 실패하면 키가 이미 없다는 뜻이다(TTL 만료, 또는 다른 토큰으로
        바뀜). 그 사이 할당은 이미 policy_not_ready 로 막혔으므로, 새로고침을
        요청해 reconcile 바퀴가 다시 게시하게 한다.
        """
        if self._stop_event.is_set():
            return
        if not self._has_write_authority():
            with self._policy_ready_lock:
                was_ready = self._policy_ready
            self._clear_policy_ready_best_effort("leadership_lost")
            if was_ready:
                # 유효성이 잠깐 끊긴 경우 _run 이 그 순간을 못 볼 수 있다. 권한이
                # 돌아오면 바로 다시 게시하도록 새로고침을 걸어 둔다.
                self.request_policy_refresh()
            return
        with self._policy_ready_lock:
            if not self._policy_ready:
                return
            renewed_at = self._policy_ready_renewed_at
            try:
                renewed = self.queues.renew_buffer_policy_ready(
                    self._policy_ready_token
                )
                reason = "expired_or_replaced"
            except QueueUnavailableError as exc:
                renewed = False
                reason = "redis_error"
                logger.warning(
                    "[Warning] operation=buffer_policy_ready_renew reason=%r",
                    str(exc),
                )
            now = time.monotonic()
            if renewed:
                self._policy_ready_renewed_at = now
                return
            if (
                reason == "redis_error"
                and renewed_at is not None
                and now - renewed_at < self.policy_ready_ttl_seconds
            ):
                # 키는 아직 TTL 안에 살아 있을 수 있다. 여기서 로컬 상태를 버리면
                # 이후 갱신이 멈춰 키가 정말로 만료된다. 다음 틱에 다시 시도한다.
                return
            self._policy_ready = False
            self._policy_ready_renewed_at = None
        gap_ms = (now - renewed_at) * 1000 if renewed_at is not None else -1
        logger.warning(
            "[BufferPolicyReadyLost] at_ms=%d reason=%s since_renew_ms=%.0f",
            int(time.time() * 1000),
            reason,
            gap_ms,
        )
        self.request_policy_refresh()

    def _scale_down_hold_resolved(self, compute_type: str, target: int) -> bool:
        """관측 못 한 축소가 이제 끝났는지 본다.

        "reconcile 이 재시도 없이 끝났다" 로 풀면 안 된다. 관측 timeout 직후의
        바퀴는 쿨다운·pods_starting 보류처럼 재시도 없이 끝나는 경로를 타는데,
        그때도 ReplicaSet 은 아직 옛 캐시로 고른 파드를 지울 수 있다. 그래서
        삭제가 실제로 보일 때만 푼다. 들고 있는 파드가 보낸 목표 이하이거나,
        이후 replicas 가 그 수 이상으로 올라가 ReplicaSet 이 더 지울 이유가 없을
        때다. 읽기가 실패하면 보류를 유지한다(fail-closed).
        """
        try:
            snapshot = self.provider.list_buffer_snapshot(compute_type)
            policy = self.queues.get_buffer_policy(compute_type)
            current = (
                int(self.provider.read_deployment_replicas(policy["deployment_name"]))
                if policy
                else int(target)
            )
        except Exception as exc:
            logger.warning(
                "[Warning] operation=buffer_scale_down_hold_check compute_type=%s "
                "reason=%r",
                compute_type,
                str(exc),
            )
            return False
        return self._held_available(snapshot) <= max(int(target), current)

    def _run_policy_ready_renewer(self, generation: int) -> None:
        """reconcile 바퀴와 독립된 깃발 갱신 타이머.

        갱신이 reconcile 루프 안에 있으면 바퀴 하나가 TTL 을 넘길 때 깃발이
        만료된다. 10/11 계측 run 에서 17.5초 바퀴가 실제로 있었고(TTL 15초),
        그 직후 할당 7건이 policy_not_ready 로 막혔다. 여기서는 Redis 키 하나만
        만지므로 allocator 락이나 Kubernetes 호출에 묶이지 않는다.
        """
        while (
            not self._stop_event.wait(self.policy_ready_renew_seconds)
            and generation == self._renewer_generation
        ):
            try:
                self._renew_policy_ready_once()
            except Exception:
                logger.exception("[Failed] operation=buffer_policy_ready_renewer")

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
            pipeline = self.pipeline_width(compute_type_value, policy["N"])
            decision = self.desired_replicas_detail(
                policy["R"],
                policy["N"],
                snapshot["buffer_assigned"],
                queued,
            )
            desired = decision["desired"]
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
                "bound_by": decision["bound_by"],
            }

            # 목표를 먼저 적는다. 확대 패스보다 앞이어야 한다 - 전에는 축소 쪽에만
            # 있어서, 올라간 목표(스파이크)가 이력에 남지 않았다. 그러면 쿨다운의
            # "창 안에 지금보다 높은 목표가 있었나" 가 거짓이 되어 직후 트림이
            # 그대로 통과한다. 실측에서 42->73 확대 직후의 73->69 축소가 이 경로로
            # 쿨다운을 빠져나갔다.
            cooldown = self.cooldown_seconds(compute_type_value)
            self._note_target(compute_type_value, desired, cooldown)

            # 자리는 아래 축소 분기와 로그가 함께 쓴다.
            room = max(0, int(policy["N"]) - int(snapshot["buffer_assigned"]))
            # 확대 패스도 포함해 목표 결정 근거를 남긴다. pipeline 은 진단값이다.
            logger.info(
                "[BufferTarget] compute_type=%s queued=%s desired=%s current=%s "
                "room=%s assigned=%s available=%s ready_available=%s total=%s "
                "R=%s N=%s dynamic=%s bound_by=%s pipeline=%s create_s=%s "
                "create_high_s=%s work_s=%s cooldown_s=%.1f create_samples=%s "
                "work_samples=%s min_samples=%s",
                compute_type_value,
                queued if queued is not None else "-",
                desired,
                current,
                room,
                snapshot["buffer_assigned"],
                snapshot["buffer_available"],
                snapshot.get("ready_available", "-"),
                snapshot["buffer_total"],
                policy["R"],
                policy["N"],
                self.dynamic_reserve,
                decision["bound_by"],
                f"{pipeline:.2f}" if pipeline is not None else "-",
                f"{self.create_time_seconds(compute_type_value, quantile=0.5):.2f}",
                f"{self.create_time_seconds(compute_type_value):.2f}",
                f"{self.work_time_seconds(compute_type_value):.2f}",
                cooldown,
                self._sample_count(compute_type_value, "create"),
                self._sample_count(compute_type_value, "work"),
                self.cooldown_min_samples,
            )

            if current < desired:
                # 올리기는 공급 작업이 공급 락 아래서 CAS 로 한다. 여기서 할당 락을
                # 기다리던 시절 포화에서 46% 가 timeout 으로 미뤄졌다. 재시도가
                # 아니다 - 공급 작업이 스스로 재예약한다.
                self.request_refill(compute_type_value, trigger="reconcile")
                result = {**base_result, "status": "refill_requested"}
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
            if current > desired or self._held_available(snapshot) > desired:
                # 용량 교정(replicas > room)은 축소 게이트 없이 공급 작업이 CAS 로 한다.
                # 축소 게이트를 세우는 경로가 셋(시작 중 파드, 쿨다운 대기, 그리고 목표가
                # room 과 같은 포화의 일반 축소)이었고, 포화 6분 사전 점검에서 이것이
                # 42번 서서 그때마다 할당과 공급이 모두 멈췄다(할당이 막힌 시간 8.6%).
                # room 까지 내리는 것은 할당기가 claim 직후 게이트 없이 하는 것과 같은
                # 조작이다. 단 게이트 풀린 파드가 room 이하일 때만 - 아니면(N 을 낮춘
                # 직후) Ready 파드가 지워질 수 있으므로 아래 축소 경로로 간다. room 아래
                # 목표까지의 수요 축소는 다음 바퀴에서 지금처럼 쿨다운과 게이트를 거친다.
                if current > room and self._held_available(snapshot) <= room:
                    self.request_refill(compute_type_value, trigger="reconcile")
                    result = {
                        **base_result,
                        "status": "refill_requested",
                        "reason": "capacity_trim",
                    }
                    self._record_status(compute_type_value, **result)
                    return result
                # schedulable_not_ready 와 oldest_schedulable_not_ready_since 는
                # 같은 집합(게이트가 하나도 없고 아직 Ready 가 아닌 RS 소유 파드)을
                # 두고 함께 계산된 쌍이다. ungated_not_ready 는 "우리 게이트만"
                # 기준이라 외부 게이트가 걸린 파드를 들여온 것으로 세고, 그 파드는
                # 영구히 Ready 가 되지 않으므로 보류를 끊을 나이도 없다.
                starting = int(snapshot.get("schedulable_not_ready", 0))
                starting_age = self._starting_age_seconds(
                    snapshot.get("oldest_schedulable_not_ready_since")
                )
                grace = self._starting_grace_seconds(compute_type_value)
                if self._defer_demand_scale_down(
                    current,
                    room,
                    desired,
                    self._held_available(snapshot),
                    starting,
                    starting_age,
                    grace,
                ):
                    result = {
                        **base_result,
                        "status": "deferred",
                        "reason": "pods_starting",
                        "schedulable_not_ready": starting,
                    }
                    self._record_status(compute_type_value, **result)
                    return result
                if starting > 0 and starting_age is not None and starting_age > grace:
                    # 상한을 넘겼다. 더 기다리지 않고 축소를 진행한다 - 이 줄이
                    # 없으면 끝내 Ready 가 안 되는 파드가 축소를 영구히 막는다.
                    logger.warning(
                        "[BufferStartingGraceExpired] compute_type=%s "
                        "schedulable_not_ready=%s age_s=%.1f grace_s=%.1f "
                        "current=%s desired=%s",
                        compute_type_value,
                        starting,
                        starting_age,
                        grace,
                        current,
                        desired,
                    )
                if starting > 0 and current > room:
                    # N correction cannot wait for readiness. Gated Pods have
                    # lower deletion cost, but ReplicaSet choice is best effort.
                    logger.warning(
                        "[BufferCapacityTrim] compute_type=%s replicas=%s->%s "
                        "ungated_not_ready=%s reason=%r",
                        compute_type_value,
                        current,
                        room,
                        starting,
                        "capacity correction while Pods are starting",
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
                    return result
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

    def _scale_down(
        self,
        compute_type: str,
        initial_policy: Dict,
        base_result: Dict,
    ) -> Dict:
        """계측 포장. 축소가 barrier 로 잡는 락도 scale_down 으로 집계된다."""
        with timing.scope(purpose="scale_down"):
            return self._scale_down_impl(compute_type, initial_policy, base_result)

    def _scale_down_impl(
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
        patch_attempted = False
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

            supply_token = None
            try:
                # 공급 락도 잡는다(할당 락 -> 공급 락 순서). 축소 게이트가 이미 서
                # 있으므로 공급 작업은 다음 확인에서 멈추고 락을 놓는다. 여기서는
                # 진행 중이던 게이트 해제 묶음이 끝나기만 기다리면 된다.
                supply_token = self._acquire_supply_lock_until(
                    compute_type,
                    time.monotonic() + self.wait_timeout_seconds,
                    heartbeat_lost,
                )
                if not supply_token:
                    return {
                        **base_result,
                        "status": "deferred",
                        "reason": "supply_lock_timeout",
                        "retry": True,
                    }
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
                starting = int(snapshot.get("schedulable_not_ready", 0))
                starting_age = self._starting_age_seconds(
                    snapshot.get("oldest_schedulable_not_ready_since")
                )
                grace = self._starting_grace_seconds(compute_type)
                if self._defer_demand_scale_down(
                    current,
                    room,
                    desired,
                    self._held_available(snapshot),
                    starting,
                    starting_age,
                    grace,
                ):
                    return {
                        **refreshed_result,
                        "status": "deferred",
                        "reason": "pods_starting",
                        "schedulable_not_ready": starting,
                    }
                if starting > 0 and current > room and desired < room:
                    desired = room
                    refreshed_result["desired_replicas"] = room
                    logger.warning(
                        "[BufferCapacityTrim] compute_type=%s replicas=%s->%s "
                        "ungated_not_ready=%s reason=%r",
                        compute_type,
                        current,
                        room,
                        starting,
                        "capacity correction while Pods are starting",
                    )
                if current < desired:
                    return {
                        **refreshed_result,
                        "status": "deferred",
                        "reason": "scale_up_required",
                        "retry": True,
                    }

                if current == desired and self._held_available(snapshot) <= desired:
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
                    # 응답이 timeout 나도 서버에는 반영됐을 수 있으므로 "보냈다" 를
                    # 먼저 적는다. 삭제를 관측하기 전에는 이것이 안전 정지의 근거다.
                    patch_attempted = True
                    self.provider.patch_deployment_replicas(
                        policy["deployment_name"],
                        desired,
                    )
                    patched = True
                initial_policy = policy
                base_result = refreshed_result
            finally:
                try:
                    if supply_token:
                        self.queues.release_supply_lock(compute_type, supply_token)
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
                # (정지 자체는 아래 finally 에서 게이트를 풀기 전에 한다.)
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
            if patch_attempted and not deletion_observed:
                # 안전 정지 - 일시적 경합이 아니다. 축소 PATCH 를 보낸 뒤 삭제를
                # 관측하지 못했으면(관측 timeout 이든 그 사이 예외든) 게이트를
                # 풀기 전에 깃발을 내리고, 이 타입을 보류로 남긴다. 보류는 삭제가
                # 실제로 관측될 때만 풀린다(reconcile 바퀴의 hold 해제 조건).
                # reconcile 결과와 깃발을 떼어 낸 뒤 남은 두 예외 중 하나다.
                with self._condition:
                    self._policy_ready_holds[compute_type] = desired
                self._clear_policy_ready_best_effort("scale_down_unobserved")
                self.request_policy_refresh(retry=True)
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
        return self._acquire_lock_until(
            self.queues.acquire_allocator_lock, "alloc", compute_type, deadline, gate_lost
        )

    def _acquire_supply_lock_until(
        self,
        compute_type: str,
        deadline: float,
        gate_lost: threading.Event,
    ) -> Optional[str]:
        return self._acquire_lock_until(
            self.queues.acquire_supply_lock, "supply", compute_type, deadline, gate_lost
        )

    def _acquire_lock_until(
        self,
        acquire: Callable[[str], Optional[str]],
        lock_name: str,
        compute_type: str,
        deadline: float,
        gate_lost: threading.Event,
    ) -> Optional[str]:
        retry_seconds = 0.02
        wait_started = time.monotonic()
        while (
            not self._stop_event.is_set()
            and self._has_write_authority()
            and not gate_lost.is_set()
            and time.monotonic() < deadline
        ):
            token = acquire(compute_type)
            if token:
                timing.emit(
                    "LockWait",
                    purpose=timing.current_purpose(),
                    wait_ms=(time.monotonic() - wait_started) * 1000,
                    ok=1,
                    lock=lock_name,
                )
                return token
            self._wait_for_event(min(retry_seconds, max(0.0, deadline - time.monotonic())))
            retry_seconds = min(0.2, retry_seconds * 1.5)
        timing.emit(
            "LockWait",
            purpose=timing.current_purpose(),
            wait_ms=(time.monotonic() - wait_started) * 1000,
            ok=0,
            lock=lock_name,
        )
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
            if self._held_available(snapshot) <= desired:
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
