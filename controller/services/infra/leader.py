"""
Lease-based leader election for controller replicas

- Uses coordination.k8s.io/v1 Lease objects
- Elects a single leader among controller pods
- Runs leader-only callbacks while leadership is held
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

from .kubernetes_client import KubernetesClient

logger = logging.getLogger(__name__)


class LeaseLeaderElector(KubernetesClient):
    """
    Simple Lease-based leader election.

    Every controller replica participates in the same Lease. Only the replica
    holding the Lease is treated as leader and allowed to run leader-only work.
    """

    def __init__(
        self,
        lease_name: str = None,
        namespace: str = None,
        identity: str = None,
        lease_duration_seconds: int = 15,
        renew_interval_seconds: int = 5,
        retry_interval_seconds: int = 2,
        on_started_leading: Optional[Callable[[], None]] = None,
        on_stopped_leading: Optional[Callable[[], None]] = None,
    ):
        super().__init__(namespace=namespace)
        self.coordination_v1 = client.CoordinationV1Api()
        self.lease_name = lease_name or os.getenv("CONTROLLER_LEASE_NAME", "controller-leader")
        self.identity = identity or os.getenv("HOSTNAME", "controller-unknown")
        self.lease_duration_seconds = int(
            os.getenv("LEASE_DURATION_SECONDS", str(lease_duration_seconds))
        )
        self.renew_interval_seconds = int(
            os.getenv("LEASE_RENEW_INTERVAL_SECONDS", str(renew_interval_seconds))
        )
        self.retry_interval_seconds = int(
            os.getenv("LEASE_RETRY_INTERVAL_SECONDS", str(retry_interval_seconds))
        )
        self.on_started_leading = on_started_leading
        self.on_stopped_leading = on_stopped_leading

        self._is_leader = False
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the election loop in a background thread."""
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self._run,
            name="lease-leader-elector",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Lease elector started: lease=%s namespace=%s identity=%s",
            self.lease_name,
            self.namespace,
            self.identity,
        )

    def stop(self) -> None:
        """Stop the election loop."""
        self._stop_event.set()
        self._set_leader(False)

    def is_leader(self) -> bool:
        with self._lock:
            return self._is_leader

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                currently_leader = self.is_leader()
                if currently_leader:
                    success = self._renew_lease()
                    if not success:
                        logger.warning("[Warning] operation=lease_renew status=lost_leadership identity=%s", self.identity)
                        self._set_leader(False)
                else:
                    success = self._try_acquire_lease()
                    if success:
                        self._set_leader(True)
            except Exception as e:
                logger.warning("[Warning] operation=lease_election_loop identity=%s reason=%r", self.identity, str(e))
                self._set_leader(False)

            interval = (
                self.renew_interval_seconds if self.is_leader() else self.retry_interval_seconds
            )
            self._stop_event.wait(interval)

    def _read_lease(self):
        try:
            return self.coordination_v1.read_namespaced_lease(
                name=self.lease_name,
                namespace=self.namespace,
            )
        except ApiException as e:
            if e.status == 404:
                return None
            raise

    def _try_acquire_lease(self) -> bool:
        lease = self._read_lease()
        now = self._now()

        if lease is None:
            body = client.V1Lease(
                api_version="coordination.k8s.io/v1",
                kind="Lease",
                metadata=client.V1ObjectMeta(name=self.lease_name),
                spec=client.V1LeaseSpec(
                    holder_identity=self.identity,
                    acquire_time=now,
                    renew_time=now,
                    lease_duration_seconds=self.lease_duration_seconds,
                    lease_transitions=0,
                ),
            )
            try:
                self.coordination_v1.create_namespaced_lease(
                    namespace=self.namespace,
                    body=body,
                )
                logger.info("Leadership acquired by creating lease: %s", self.identity)
                return True
            except ApiException as e:
                if e.status == 409:
                    return False
                raise

        holder = self._holder_identity(lease)
        if holder == self.identity:
            return self._renew_lease(existing_lease=lease)

        if not self._is_lease_expired(lease):
            return False

        previous_transitions = lease.spec.lease_transitions or 0
        body = client.V1Lease(
            api_version="coordination.k8s.io/v1",
            kind="Lease",
            metadata=client.V1ObjectMeta(
                name=self.lease_name,
                resource_version=lease.metadata.resource_version,
            ),
            spec=client.V1LeaseSpec(
                holder_identity=self.identity,
                acquire_time=now,
                renew_time=now,
                lease_duration_seconds=self.lease_duration_seconds,
                lease_transitions=previous_transitions + 1,
            ),
        )

        try:
            self.coordination_v1.replace_namespaced_lease(
                name=self.lease_name,
                namespace=self.namespace,
                body=body,
            )
            logger.info(
                "Leadership acquired from expired holder: new=%s previous=%s",
                self.identity,
                holder or "none",
            )
            return True
        except ApiException as e:
            if e.status in (404, 409):
                return False
            raise

    def _renew_lease(self, existing_lease=None) -> bool:
        lease = existing_lease or self._read_lease()
        if lease is None:
            return False

        holder = self._holder_identity(lease)
        if holder != self.identity:
            return False

        now = self._now()
        acquire_time = lease.spec.acquire_time or now
        transitions = lease.spec.lease_transitions or 0

        body = client.V1Lease(
            api_version="coordination.k8s.io/v1",
            kind="Lease",
            metadata=client.V1ObjectMeta(
                name=self.lease_name,
                resource_version=lease.metadata.resource_version,
            ),
            spec=client.V1LeaseSpec(
                holder_identity=self.identity,
                acquire_time=acquire_time,
                renew_time=now,
                lease_duration_seconds=self.lease_duration_seconds,
                lease_transitions=transitions,
            ),
        )

        try:
            self.coordination_v1.replace_namespaced_lease(
                name=self.lease_name,
                namespace=self.namespace,
                body=body,
            )
            return True
        except ApiException as e:
            if e.status in (404, 409):
                return False
            raise

    def _set_leader(self, new_state: bool) -> None:
        callback = None

        with self._lock:
            if self._is_leader == new_state:
                return
            self._is_leader = new_state
            callback = self.on_started_leading if new_state else self.on_stopped_leading

        if new_state:
            logger.info("This controller is now leader: %s", self.identity)
        else:
            logger.info("This controller is now follower: %s", self.identity)

        if callback:
            try:
                callback()
            except Exception as e:
                logger.warning("[Warning] operation=leader_transition_callback identity=%s reason=%r", self.identity, str(e))

    @staticmethod
    def _holder_identity(lease) -> str:
        if not lease or not lease.spec:
            return ""
        return lease.spec.holder_identity or ""

    @staticmethod
    def _now():
        return datetime.now(timezone.utc)

    def _is_lease_expired(self, lease) -> bool:
        if not lease or not lease.spec:
            return True

        renew_time = lease.spec.renew_time
        duration = lease.spec.lease_duration_seconds

        if renew_time is None or duration is None:
            return True

        if renew_time.tzinfo is None:
            renew_time = renew_time.replace(tzinfo=timezone.utc)

        expires_at = renew_time.timestamp() + duration
        return time.time() > expires_at
