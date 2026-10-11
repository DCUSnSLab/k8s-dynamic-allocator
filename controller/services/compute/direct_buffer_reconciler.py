"""리더 전용(cold start): 미리 만드는 파드가 없다. 파드는 요청마다 할당(drain)이 만든다.

리더가 하는 일은 두 가지다.
  - 남은 idle 파드 정리. 다른 arm 이 남긴 idle 파드가 있으면 지운다. 이 arm 의 할당은
    idle 을 내주지 않으므로, 그대로 두면 자리(N)만 쥐고 cold 측정을 좁힌다.
  - 주기 정리(고아 파드 회수, 남은 compute Deployment 내리기). 할당된 파드마다 API 를
    부르므로 길어서 별도 스레드에서 한다.

삭제는 uid + resourceVersion 전제 조건이라 락이 필요 없다. 지우면 자리가 줄 뿐
늘지 않으므로 N 계산과 경쟁하지 않는다.
"""

import logging
import threading
import time
from typing import Callable, Dict, List, Optional

from config import settings

from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class DirectBufferReconciler:
    def __init__(
        self,
        provider,
        queues,
        on_periodic_cleanup: Optional[Callable[[], Dict]] = None,
    ):
        self.provider = provider
        self.queues = queues
        self.on_periodic_cleanup = on_periodic_cleanup

        self.resync_seconds = max(0.2, float(settings.WARM_BUFFER_RESYNC_SECONDS))
        self.cleanup_seconds = max(1.0, float(settings.BUFFER_RECONCILE_RESYNC_SECONDS))
        self.pending_ttl_seconds = max(1.0, float(settings.WARM_BUFFER_PENDING_CREATE_SECONDS))
        # 같은 순간 몰려오는 파드 이벤트를 한 바퀴로 합친다.
        self.debounce_seconds = 0.05

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

    def _still_in_charge(self, generation: Optional[int]) -> bool:
        """이 바퀴의 결정을 계속 집행해도 되나. 메모리 안의 값만 본다(원격 조회 없음)."""
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
        """idle 파드 이벤트에만 깨어난다. 요청 파드 이벤트에는 할 일이 없다."""
        del event_type, source
        labels = getattr(getattr(pod, "metadata", None), "labels", None) or {}
        if labels.get(self.provider.LABEL_STATUS) == self.provider.STATUS_IDLE:
            self.request(labels.get(self.provider.LABEL_COMPUTE_TYPE))

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

            for compute_type in self._compute_types():
                if self._nothing_to_do(compute_type):
                    continue
                try:
                    self.reconcile_type(compute_type, generation)
                except Exception as exc:
                    logger.exception(
                        "[Failed] operation=direct_buffer_reconcile compute_type=%s reason=%r",
                        compute_type,
                        str(exc),
                    )

    def _maintenance_loop(self, generation: int) -> None:
        # 리더가 되자마자 한 번(남은 Deployment 버퍼를 먼저 내린다), 그다음 주기마다.
        while not self._stop_event.is_set() and self._generation == generation:
            if self._still_in_charge(generation):
                self._periodic_maintenance()
            if self._stop_event.wait(self.cleanup_seconds):
                return

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
        """직전 바퀴에 지울 idle 이 남지 않았고 그 기록이 resync 주기 안이면 목록을 읽지 않는다.

        idle 이 생기면 그 이벤트가 깨우고, 이벤트를 놓쳐도 resync 주기 뒤에는 다시 본다.
        """
        ct = self.queues.normalize_compute_type(compute_type)
        with self._status_lock:
            last = self._status.get(ct)
        if not last:
            return False
        if time.time() - float(last.get("updated_at") or 0.0) >= self.resync_seconds:
            return False
        return int(last.get("idle") or 0) - int(last.get("deleted") or 0) <= 0

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
        snapshot = self.provider.snapshot(ct)
        # 목록에 보인 이름은 생성 예약에서 뺀다(제안 arm 의 리더와 같다). 이제 목록이
        # 그 자리를 센다. 빼지 않으면 세션이 끝나 파드가 사라진 뒤에도 예약 상한(60초)
        # 까지 그 이름이 자리를 쥐어 N 이 그만큼 줄어든다.
        listed = {pod["name"] for pod in snapshot["pods"]}
        seen = [name for name in self.queues.pending_creates(ct, self.pending_ttl_seconds) if name in listed]
        if seen:
            self.queues.remove_pending_creates(ct, seen)
        idle = list(snapshot["idle"])
        deleted = self._delete_idle(idle, generation)
        record = {
            "idle": len(idle),
            "deleted": deleted,
            "pending_seen": len(seen),
            "assigned": int(snapshot["assigned"]),
            "active": len(snapshot["active_names"]),
            "N": self.provider.read_capacity(ct),
        }
        self._record_status(ct, record)
        if idle:
            logger.info(
                "[ColdStartIdleCleanup] compute_type=%s idle=%s deleted=%s",
                ct,
                len(idle),
                deleted,
            )
        return {"status": "ok", "record": record}

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
