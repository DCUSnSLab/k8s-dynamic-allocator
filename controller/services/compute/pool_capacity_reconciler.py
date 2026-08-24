import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Dict, Optional, Set

from config import settings

from ..queue import QueueUnavailableError

logger = logging.getLogger(__name__)


class PoolCapacityReconciler:
    """Leader-only R/N policy cache and Deployment scale reconciler."""

    def __init__(
        self,
        pool,
        queues,
        on_capacity_available: Optional[Callable[[Optional[str]], None]] = None,
        on_periodic_cleanup: Optional[Callable[[], Dict]] = None,
    ):
        self.pool = pool
        self.queues = queues
        self.on_capacity_available = on_capacity_available
        self.on_periodic_cleanup = on_periodic_cleanup

        self.debounce_seconds = max(
            0.0,
            float(settings.POOL_RECONCILE_DEBOUNCE_SECONDS),
        )
        self.resync_seconds = max(
            1.0,
            float(settings.POOL_RECONCILE_RESYNC_SECONDS),
        )
        self.wait_timeout_seconds = max(
            0.1,
            float(settings.POOL_SCALE_DOWN_WAIT_TIMEOUT_SECONDS),
        )
        self.gate_renew_seconds = max(
            0.1,
            min(
                float(settings.POOL_SCALE_DOWN_GATE_RENEW_SECONDS),
                max(0.1, float(settings.POOL_SCALE_DOWN_GATE_TTL_SECONDS) / 2.0),
            ),
        )
        self.policy_ready_renew_seconds = max(
            0.1,
            min(
                float(settings.POOL_POLICY_READY_RENEW_SECONDS),
                max(0.1, float(settings.POOL_POLICY_READY_TTL_SECONDS) / 2.0),
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

    @staticmethod
    def desired_replicas(pool_available_min: int, pool_total_max: int, assigned: int) -> int:
        return min(
            int(pool_available_min),
            max(0, int(pool_total_max) - int(assigned)),
        )

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
                        name="pool-capacity-restart",
                        daemon=True,
                    )
                    self._restart_thread.start()
                return
            self._policy_ready_token = uuid.uuid4().hex
            self._policy_ready = False
            self._last_policy_sync_succeeded = False
            try:
                self.queues.clear_pool_policy_ready()
            except QueueUnavailableError as exc:
                logger.warning(
                    "[Warning] operation=pool_policy_ready_invalidate reason=%r",
                    str(exc),
                )
            self._stop_event.clear()
            self._policy_refresh_pending = True
            self._next_run_at = time.monotonic()
            self._thread = threading.Thread(
                target=self._run,
                name="pool-capacity-reconciler",
                daemon=True,
            )
            self._thread.start()
            self._condition.notify_all()
        logger.info("[PoolCapacityReconcilerStarted]")

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
                self.queues.clear_pool_policy_ready(token)
            except QueueUnavailableError as exc:
                logger.warning(
                    "[Warning] operation=pool_policy_ready_clear reason=%r",
                    str(exc),
                )
        self._policy_ready = False
        logger.info("[PoolCapacityReconcilerStopped]")

    def on_pool_event(self, event_type: str, pod, source: str = "watch") -> None:
        del event_type, source
        metadata = getattr(pod, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        compute_type = labels.get(self.pool.LABEL_COMPUTE_TYPE)
        if compute_type:
            self.request_reconcile(compute_type)

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
            labels.get(self.pool.LABEL_COMPUTE_TYPE)
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
                            "[Warning] operation=reservation_journal_cleanup "
                            "reason=%r",
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
                        renewed = self.queues.renew_pool_policy_ready(
                            self._policy_ready_token
                        )
                    except QueueUnavailableError as exc:
                        renewed = False
                        logger.warning(
                            "[Warning] operation=pool_policy_ready_renew reason=%r",
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
                            self.queues.publish_pool_policy_ready(
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
                                            "[PoolQueueKickSkipped] "
                                            "compute_type=%s reason=%r",
                                            compute_type,
                                            str(exc),
                                        )
                        except QueueUnavailableError as exc:
                            self._policy_ready = False
                            next_ready_renew_at = float("inf")
                            logger.warning(
                                "[Warning] operation=pool_policy_ready_publish "
                                "reason=%r",
                                str(exc),
                            )
                            self.request_policy_refresh(retry=True)
        except Exception as exc:
            logger.exception(
                "[Failed] operation=pool_capacity_reconciler reason=%r",
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
            deployments = self.pool.list_pool_deployments()
        except Exception as exc:
            logger.warning(
                "[Warning] operation=pool_policy_sync reason=%r",
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
                labels.get(self.pool.LABEL_COMPUTE_TYPE)
            )
            deployments_by_type.setdefault(compute_type, []).append(deployment)

        valid_policies: Dict[str, Dict] = {}
        for compute_type, matching in deployments_by_type.items():
            if len(matching) != 1:
                invalid_by_type[compute_type] = (
                    "Exactly one warm-pool Deployment is required per compute-type"
                )
                continue
            try:
                valid_policies[compute_type] = self.pool.parse_deployment_policy(
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
                    "[Warning] operation=pool_policy_clear compute_type=%s reason=%r",
                    compute_type,
                    str(exc),
                )
            error = invalid_by_type.get(
                compute_type,
                "No warm-pool Deployment exists for this compute-type",
            )
            self._record_status(
                compute_type,
                policy_valid=False,
                policy_error=error,
            )
            logger.error(
                "[PoolPolicyInvalid] compute_type=%s reason=%r",
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
                    "[Warning] operation=pool_policy_store compute_type=%s reason=%r",
                    compute_type,
                    str(exc),
                )

        if stored_types:
            try:
                self.queues.register_compute_types(sorted(stored_types))
            except QueueUnavailableError as exc:
                sync_succeeded = False
                logger.warning(
                    "[Warning] operation=pool_policy_register reason=%r",
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
        current = self.queues.get_pool_policy(compute_type_value)
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
            current = self.queues.get_pool_policy(compute_type_value)
            if self._same_policy(current, policy):
                return True

            if policy is None:
                self.queues.clear_pool_policy(compute_type_value)
            else:
                self.queues.set_pool_policy(
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
                        "[PoolQueueKickSkipped] compute_type=%s reason=%r",
                        compute_type_value,
                        str(exc),
                    )

    def _clear_policy_ready_best_effort(self) -> None:
        token = self._policy_ready_token
        self._policy_ready = False
        if not token:
            return
        try:
            self.queues.clear_pool_policy_ready(token)
        except QueueUnavailableError as exc:
            logger.warning(
                "[Warning] operation=pool_policy_ready_clear reason=%r",
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
            policy = self.queues.get_pool_policy(compute_type_value)
            if not policy:
                result = {
                    "compute_type": compute_type_value,
                    "status": "blocked",
                    "reason": "policy_unavailable",
                }
                self._record_status(compute_type_value, **result)
                return result

            snapshot = self.pool.list_pool_snapshot(compute_type_value)
            desired = self.desired_replicas(
                policy["R"],
                policy["N"],
                snapshot["pool_assigned"],
            )
            current = self.pool.read_deployment_replicas(
                policy["deployment_name"]
            )

            base_result = {
                "compute_type": compute_type_value,
                "deployment_name": policy["deployment_name"],
                "R": policy["R"],
                "N": policy["N"],
                "pool_total": snapshot["pool_total"],
                "pool_available": snapshot["pool_available"],
                "pool_assigned": snapshot["pool_assigned"],
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

            if current > desired:
                result = self._scale_down(
                    compute_type_value,
                    policy,
                    base_result,
                )
                self._record_status(compute_type_value, **result)
                return result

            if snapshot["pool_available"] > desired:
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
                "[Failed] operation=pool_capacity_reconcile compute_type=%s reason=%r",
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
            policy = self.queues.get_pool_policy(compute_type)
            if not policy:
                return {
                    **base_result,
                    "status": "blocked",
                    "reason": "policy_unavailable",
                }
            snapshot = self.pool.list_pool_snapshot(compute_type)
            desired = self.desired_replicas(
                policy["R"],
                policy["N"],
                snapshot["pool_assigned"],
            )
            current = self.pool.read_deployment_replicas(
                policy["deployment_name"]
            )
            refreshed = {
                **base_result,
                "R": policy["R"],
                "N": policy["N"],
                "pool_total": snapshot["pool_total"],
                "pool_available": snapshot["pool_available"],
                "pool_assigned": snapshot["pool_assigned"],
                "current_replicas": current,
                "desired_replicas": desired,
            }
            if current > desired or snapshot["pool_available"] > desired:
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

            patched = self.pool.patch_deployment_replicas(
                policy["deployment_name"],
                desired,
            )
            logger.info(
                "[PoolScaled] compute_type=%s direction=up replicas=%s->%s "
                "assigned=%s R=%s N=%s",
                compute_type,
                current,
                desired,
                snapshot["pool_assigned"],
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
            name=f"pool-gate-{compute_type}",
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

                policy = self.queues.get_pool_policy(compute_type)
                if not policy:
                    return {
                        **base_result,
                        "status": "blocked",
                        "reason": "policy_unavailable",
                    }

                snapshot = self.pool.list_pool_snapshot(compute_type)
                desired = self.desired_replicas(
                    policy["R"],
                    policy["N"],
                    snapshot["pool_assigned"],
                )
                current = self.pool.read_deployment_replicas(
                    policy["deployment_name"]
                )
                refreshed_result = {
                    **base_result,
                    "deployment_name": policy["deployment_name"],
                    "R": policy["R"],
                    "N": policy["N"],
                    "pool_total": snapshot["pool_total"],
                    "pool_available": snapshot["pool_available"],
                    "pool_assigned": snapshot["pool_assigned"],
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

                if current == desired and snapshot["pool_available"] <= desired:
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
                    self.pool.patch_deployment_replicas(
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
                "[PoolScaleDownObserved] compute_type=%s patched=%s "
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
                            "[PoolQueueKickSkipped] compute_type=%s reason=%r",
                            compute_type,
                            str(exc),
                        )
            if patched and heartbeat_lost.is_set():
                logger.warning(
                    "[Warning] operation=pool_scale_down compute_type=%s "
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
            snapshot = self.pool.list_pool_snapshot(compute_type)
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
            snapshot = self.pool.list_pool_snapshot(compute_type)
            if snapshot["pool_available"] <= desired:
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
