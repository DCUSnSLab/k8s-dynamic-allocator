"""웜 버퍼의 목표 정책: 동적 R 과 축소 쿨다운. 집행(파드 생성·삭제)과 떼어 둔다.

  desired = min(max(R, 수요), N - 받아 간 요청)
  목표    = 쿨다운 창 안의 desired 최댓값
  쿨다운  = 관측값 두 개(파드 생성 → Ready p90, 목표가 가라앉아 있던 시간 p90)의 큰 쪽

R 은 바닥이다. 수요가 R 보다 크면 수요를 따른다(동적 R). 고정 R 정책은 없다.
쿨다운 길이는 설정이 아니라 관측값에서 나온다.

develop 의 BufferCapacityReconciler 에 있던 정책 계산을 그대로 옮겼다. 그쪽은
Deployment 집행(replicas, 게이트, 공급 락)과 한 클래스에 섞여 있었다.
"""

import logging
import threading
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

from config import settings

logger = logging.getLogger(__name__)


class BufferPolicy:
    # 쿨다운에 쓰는 분위. 높은 쪽이 맞다 - "줄였다가 되돌려지지 않으려면 얼마나
    # 기다려야 하나"의 답이고, 중앙값으로 맞추면 절반은 되돌려진다.
    COOLDOWN_QUANTILE = 0.9

    def __init__(self, queues):
        self.queues = queues
        self.cooldown_samples = max(1, int(settings.BUFFER_SCALE_DOWN_COOLDOWN_SAMPLES))
        self.cooldown_min_samples = max(1, int(settings.BUFFER_SCALE_DOWN_COOLDOWN_MIN_SAMPLES))
        self._leadership_validator: Callable[[], bool] = lambda: True
        # 비교군별로: 목표가 가라앉아 있던 시간들(쿨다운 추정에 씀), 진행 중인
        # 꺼짐의 시작 시각, 쿨다운이 막아낸 축소 횟수. 다른 스레드가 상태를 읽으므로
        # _churn_lock 으로 보호한다.
        self._churn_lock = threading.Lock()
        self._churn: Dict[str, Dict] = {}

    # ------------------------------------------------------------------ 목표

    @staticmethod
    def desired_detail(reserve: int, capacity: int, settled: int, demand: int) -> Dict:
        """R, 수요, 남은 N 자리로 버퍼 목표를 정한다. 무엇이 목표를 정했는지도 돌려준다.

        settled 는 Ready 파드를 받아 간 요청 수다. 아직 파드를 받지 못한 요청의 파드는
        버퍼로 센다(호출자가 수요에 넣는다).
        """
        room = max(0, int(capacity) - int(settled))
        want = int(demand)
        bound = "demand"
        if int(reserve) >= want:
            want = int(reserve)
            bound = "reserve"
        if room < want:
            return {"desired": room, "bound_by": "room"}
        return {"desired": want, "bound_by": bound}

    def window_max(self, compute_type: str) -> Optional[int]:
        """쿨다운 창 안에서 본 목표의 최댓값. 아직 본 것이 없으면 None."""
        with self._churn_lock:
            values = [value for _, value in self._churn_state(compute_type)["targets"]]
        return max(values) if values else None

    # ------------------------------------------------------------------ 권한

    def set_leadership_validator(self, validator: Callable[[], bool]) -> None:
        self._leadership_validator = validator

    def _has_write_authority(self) -> bool:
        try:
            return bool(self._leadership_validator())
        except Exception:
            logger.exception("[Failed] operation=leadership_validation")
            return False

    # ------------------------------------------------------------------ 쿨다운

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
        사라지고, 다시 쌓일 때까지 쿨다운이 0 이 되어 수요 축소가 무조건 통과한다.
        표본 자체는 "이 클러스터에서 파드를 세우는 데 걸리는 시간" 과 "수요가 가라앉아
        있는 시간" 이라 누가 관측했는지와 무관하므로 공유해도 된다.

        쓰는 것은 리더만 한다. 양쪽이 같은 파드의 Ready 를 보고 각자 넣으면 같은 값이
        두 번 들어가고, 중복 제거는 프로세스별이라 막지 못한다. 읽기는 둘 다 한다.

        실험 시작 때는 reset_server 가 Redis 를 비우므로 표본이 없는 상태로 출발한다.
        그것은 의도된 것이다(매 실행을 같은 조건에서 시작).
        """
        try:
            if not self._has_write_authority():
                return
            self.queues.record_churn_sample(compute_type, kind, float(value), self.cooldown_samples)
        except Exception as exc:
            logger.debug(
                "[BufferChurnSampleSkipped] compute_type=%s kind=%s reason=%r",
                compute_type,
                kind,
                str(exc),
            )

    def _shared_or_local(self, compute_type: str, kind: str, local: List[float]) -> List[float]:
        """공유 표본이 더 많으면 그것을 쓰고, 읽을 수 없으면 메모리 쪽으로 물러선다."""
        try:
            shared = self.queues.churn_samples(compute_type, kind)
        except Exception:
            return local
        return shared if len(shared) >= len(local) else local

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

    def cooldown_seconds(self, compute_type: str) -> float:
        """관측값 두 개의 큰 쪽. 설정에 적힌 숫자가 아니다.

            생성 시간   파드를 다시 세우는 데 걸리는 시간. 줄였다가 바로 필요해지면
                        사용자가 그만큼 기다린다. 그보다 짧게 기다릴 이유가 없다.
            꺼짐 길이   목표가 가라앉아 있던 시간. 그 안에 줄이면 되돌려진다.

        둘 다 표본이 모자란 때는 0 을 돌려준다. 그 시점은 기동 직후뿐이다.

        목표가 꺼졌다 돌아오는 시간을 쓰는 이유: 쿨다운이 답해야 할 질문이 그것이다 -
        지금 줄이면 얼마 뒤에 되돌려지는가. 목표값은 수요를 그대로 따라가고 쿨다운이
        손대지 않으므로, 이 신호는 쿨다운 자신의 동작에 오염되지 않는다. 축소에서
        재확대까지의 간격을 쓰면 쿨다운이 밀어낸 시간이 그 안에 섞여 추정치가 자기
        자신을 키운다. 중앙값이 아니라 높은 분위를 쓴다.
        """
        with self._churn_lock:
            local_dips = list(self._churn_state(compute_type)["dips"])
        dips = sorted(self._shared_or_local(compute_type, "dip", local_dips))

        def high(values):
            if len(values) < self.cooldown_min_samples:
                return None
            return float(values[min(len(values) - 1, int(len(values) * self.COOLDOWN_QUANTILE))])

        create = self.create_time_seconds(compute_type) or None
        candidates = [v for v in (create, high(dips)) if v is not None]
        return max(candidates) if candidates else 0.0

    def note_target(self, compute_type: str, desired: int, cooldown: float) -> None:
        """이번 패스의 목표를 적고, 꺼짐이 끝났으면 그 길이를 표본에 넣는다.

        꺼짐은 목표가 내려간 순간 시작되고 올라간 순간 끝난다. 기준값을 고정해
        "그 수준으로 돌아올 때까지"로 재면, 목표가 계단식으로 내려가 다시 올라오지
        않는 구간에서 꺼짐이 영구히 열린 채로 남아 그 뒤의 표본을 하나도 못 모은다.
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

    def cooldown_blocks_scale_down(self, compute_type: str, target: int, cooldown: float) -> Optional[float]:
        """아직 기다려야 하는 초, 또는 줄여도 될 때 None.

        창 안에서 목표가 지금보다 높았던 적이 있으면 아직 출렁이는 중이므로 기다린다.
        내내 지금 이하였다면 수요가 가라앉은 것이고, 그때의 축소는 되돌려지지 않는다.

        이력이 창을 아직 덮지 못했으면(이 프로세스가 방금 떴거나 리더가 막 바뀜)
        "창 안에 더 높은 목표가 없었다" 를 말할 수 없으므로 기다린다 - 축소를 미루는
        쪽은 파드를 들고 있는 방향이라 되돌릴 수 있다. N 은 이 보류와 무관하게
        지켜진다(만들 때 N 여유 안에서만 만든다).
        """
        if cooldown <= 0:
            return None
        now = time.monotonic()
        with self._churn_lock:
            state = self._churn_state(compute_type)
            state["cooldown"] = cooldown
            observing_since = state["observing_since"]
            if observing_since is not None:
                observed = now - observing_since
                if observed < cooldown:
                    state["held_back"] += 1
                    return cooldown - observed
            higher = [(at, value) for at, value in state["targets"] if value > int(target)]
            if not higher:
                return None
            newest_at = max(at for at, _ in higher)
            state["held_back"] += 1
        remaining = cooldown - (now - newest_at)
        return remaining if remaining > 0 else None

    def note_create_time(self, compute_type: str, uid: str, created: datetime, ready: datetime) -> None:
        """파드가 Ready 가 된 순간, 생성 → Ready 시간을 표본에 넣는다.

        쿨다운의 바닥이 되는 값이다. Ready 이벤트는 같은 파드에 여러 번 오므로 uid 로
        한 번만 센다. 시각이 없으면 조용히 넘어간다 - 추정기는 표본이 모자라면 스스로
        비켜선다.
        """
        if not uid or created is None or ready is None:
            return
        try:
            seconds = (ready - created).total_seconds()
        except Exception:
            return
        if seconds < 0:
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

