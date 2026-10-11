import logging
from typing import Dict, Optional

from config import settings

from .compute import DirectBufferProvider
from .queue import ComputeQueues
from .status import ControllerStatus

logger = logging.getLogger(__name__)

# 할당을 요청받으면 돌려주는 사유. 이 arm 에서 요청이 오면 실험 구성이 잘못된 것이다.
BASELINE_REFUSAL = "baseline arm: commands run on the user pod; compute pods are not allocated"


class Orchestrator:
    """exp-baseline: 작업은 고사양 user pod 에서 직접 돈다. compute 파드를 쓰지 않는다.

    compute 공급(리더 조정)·할당(큐 작업자)·파드 이벤트 감시·리더 선출을 만들지도
    시작하지도 않는다. R=N=0 을 넣어 같은 루프를 빈 채로 돌리는 것이 아니라 루프 자체가
    없다. 그래서 할당 요청은 큐에 넣지 않고 바로 거절한다.

    남는 일은 상태 보고 하나다. 배포 smoke 와 run 기록이 "compute 파드 0" 을 확인한다
    (다른 arm 이 남긴 compute 파드는 배포가 지운다).
    """

    def __init__(self):
        # compute 파드를 만들지 않는다. 남아 있는 것을 세는 목록 조회에만 쓴다.
        self.provider = DirectBufferProvider()
        self.queues = ComputeQueues()
        self.tickets = self.queues.tickets
        self.status = ControllerStatus(self.provider, self.queues, self.tickets)
        self.startup_completed = False

    def health_check(self) -> str:
        return "Orchestrator healthy"

    def initialize_buffer(self) -> Dict:
        return {"created": [], "existing": [], "failed": [], "templates": []}

    def start(self) -> Dict:
        if not self.startup_completed:
            logger.info(
                "Baseline arm (mode=%s): compute supply, allocation and leader election are not started",
                settings.COMPUTE_ALLOCATION_MODE,
            )
            self.startup_completed = True
        return self.initialize_buffer()

    def stop(self) -> None:
        return None

    def _refuse(self, **fields) -> Dict:
        logger.warning("[Refused] operation=compute_allocation reason=%r %s", BASELINE_REFUSAL, fields)
        return {"status": "failed", "message": BASELINE_REFUSAL}

    def execute_command(
        self,
        username: str,
        command: str,
        user_pod_ip: str,
        user_pod: str = "",
        compute_type: Optional[str] = None,
        request_at_ms: Optional[int] = None,
        ticket_id: Optional[str] = None,
    ) -> Dict:
        del command, user_pod_ip, compute_type, request_at_ms
        return self._refuse(user_pod=user_pod or username, ticket_id=ticket_id)

    def cancel_ticket(self, ticket_id: str, reason: str = "") -> Dict:
        del reason
        return self._refuse(ticket_id=ticket_id)

    def release_compute_pod(
        self,
        compute_pod: str,
        request_context: Optional[Dict[str, object]] = None,
    ) -> Dict:
        del request_context
        return self._refuse(compute_pod=compute_pod)

    def process_wait_queues(self) -> Dict:
        return {"status": "skipped", "message": BASELINE_REFUSAL}

    def get_assigned_request_context(self, compute_pod: str) -> Optional[Dict[str, object]]:
        del compute_pod
        return None

    def get_buffer_status(self) -> Dict:
        response = self.status.get_buffer_status()
        # 이 arm 에는 R 도 N 도 없다(0 이 아니라 없는 값이다).
        response.update(
            {
                "allocation_mode": settings.COMPUTE_ALLOCATION_MODE,
                "R": None,
                "N": None,
                "capacity": None,
            }
        )
        return response

    def get_queue_status(self, compute_type: Optional[str] = None) -> Dict:
        return self.status.get_queue_status(compute_type=compute_type)

    def get_ticket(self, ticket_id: str) -> Dict:
        return self.status.get_ticket(ticket_id)

    def touch_ticket_poll(self, ticket_id: str) -> None:
        del ticket_id
