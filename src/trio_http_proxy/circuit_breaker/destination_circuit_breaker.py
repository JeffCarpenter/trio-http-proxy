"""DestinationCircuitBreaker Trio-native circuit breaker implementation."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque

from .circuit_permit import CircuitPermit
from .circuit_state import CircuitState

logger = logging.getLogger(__name__)


class DestinationCircuitBreaker:
    """Explicit Trio-native circuit breaker for a destination (host, port).

    Implements:
    - Tokenized lease admission via CircuitPermit preventing pre-open completion theft.
    - Strict half-open single-probe discipline with probe lease tracking.
    - Deadlock recovery for dropped probe permits after TTL expiration.
    - O(1) amortized sliding window via collections.deque and rolling error counts.
    - Strict breaker and epoch identity checking on completion recording.
    """

    def __init__(
        self,
        errors_rate: int = 50,
        period: float = 60.0,
        ttl: float = 30.0,
        min_calls: int = 3,
        epoch: int = 0,
    ) -> None:
        self.errors_rate = errors_rate
        self.period = period
        self.ttl = ttl
        self.min_calls = min_calls
        self.state: CircuitState = CircuitState.CLOSED
        self.opened_at: float | None = None
        self.epoch: int = epoch
        self._permit_seq: int = 0
        self._half_open_in_flight: bool = False
        self._active_probe_id: int | None = None
        self._probe_started_at: float | None = None
        self._calls: deque[tuple[float, bool]] = deque()
        self._window_error_count: int = 0
        self._lock = threading.Lock()

    def _prune_calls(self, now: float) -> None:
        cutoff = now - self.period
        while self._calls and self._calls[0][0] < cutoff:
            _, old_err = self._calls.popleft()
            if old_err:
                self._window_error_count -= 1

    def acquire_permit(self, now: float | None = None) -> CircuitPermit | None:
        if now is None:
            now = time.monotonic()
        with self._lock:
            if self.state == CircuitState.OPEN:
                opened_at = self.opened_at
                if opened_at is not None and now - opened_at >= self.ttl:
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_in_flight = False
                    self._active_probe_id = None
                    self._probe_started_at = None
                else:
                    return None

            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_in_flight:
                    # Check for dropped probe lease timeout recovery (§6.9)
                    if (
                        self._probe_started_at is not None
                        and now - self._probe_started_at >= self.ttl
                    ):
                        logger.warning(
                            "Probe lease %s timed out after %ss in HALF_OPEN; admitting new probe",
                            self._active_probe_id,
                            self.ttl,
                        )
                        self._half_open_in_flight = False
                        self._active_probe_id = None
                    else:
                        return None
                self._half_open_in_flight = True
                self._permit_seq += 1
                self._active_probe_id = self._permit_seq
                self._probe_started_at = now
                return CircuitPermit(
                    breaker=self,
                    epoch=self.epoch,
                    permit_id=self._permit_seq,
                    is_probe=True,
                )

            # CircuitState.CLOSED
            self._permit_seq += 1
            return CircuitPermit(
                breaker=self,
                epoch=self.epoch,
                permit_id=self._permit_seq,
                is_probe=False,
            )

    def can_execute(self, now: float | None = None) -> bool:
        """Legacy check returning boolean (does not track permit identity)."""
        if now is None:
            now = time.monotonic()
        with self._lock:
            if self.state == CircuitState.OPEN:
                opened_at = self.opened_at
                if opened_at is not None and now - opened_at >= self.ttl:
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_in_flight = False
                    self._active_probe_id = None
                    self._probe_started_at = None
                else:
                    return False

            if self.state == CircuitState.HALF_OPEN:
                if self._half_open_in_flight:
                    if (
                        self._probe_started_at is not None
                        and now - self._probe_started_at >= self.ttl
                    ):
                        self._half_open_in_flight = False
                        self._active_probe_id = None
                    else:
                        return False
                self._half_open_in_flight = True
                self._probe_started_at = now
                return True

            return True

    def _abort_permit(self, permit: CircuitPermit) -> None:
        with self._lock:
            if (
                permit.breaker is self
                and permit.epoch == self.epoch
                and self.state == CircuitState.HALF_OPEN
                and permit.is_probe
                and (
                    self._active_probe_id is None
                    or self._active_probe_id == permit.permit_id
                )
            ):
                self._half_open_in_flight = False
                self._active_probe_id = None
                self._probe_started_at = None

    def record_result(
        self,
        exc: Exception | None,
        now: float | None = None,
        permit: CircuitPermit | None = None,
    ) -> None:
        if now is None:
            now = time.monotonic()
        is_error = exc is not None
        with self._lock:
            # 1. Invalidation checks: breaker identity, epoch, and abort status (§6.6)
            if permit is not None:
                if permit.breaker is not self:
                    return
                if permit.epoch != self.epoch:
                    return
                if not permit.mark_completed():
                    return

            # 2. State-specific transitions (§6.7, §6.8)
            if self.state == CircuitState.HALF_OPEN:
                # Permitless callers or non-probe leases cannot transition HALF_OPEN
                if permit is None or not permit.is_probe:
                    logger.warning(
                        "DestinationCircuitBreaker received record_result in HALF_OPEN without a valid probe permit (permit=%r); ignoring",
                        permit,
                    )
                    return
                if (
                    self._active_probe_id is not None
                    and permit.permit_id != self._active_probe_id
                ):
                    return

                self._half_open_in_flight = False
                self._active_probe_id = None
                self._probe_started_at = None
                if is_error:
                    self.state = CircuitState.OPEN
                    self.opened_at = now
                    self.epoch += 1
                else:
                    self.state = CircuitState.CLOSED
                    self.epoch += 1
                    self._calls.clear()
                    self._window_error_count = 0
                    self.opened_at = None
            elif self.state == CircuitState.CLOSED:
                self._calls.append((now, is_error))
                if is_error:
                    self._window_error_count += 1
                self._prune_calls(now)
                if len(self._calls) >= self.min_calls:
                    rate = (self._window_error_count / len(self._calls)) * 100.0
                    if rate >= self.errors_rate:
                        self.state = CircuitState.OPEN
                        self.opened_at = now
                        self.epoch += 1
                        self._calls.clear()
                        self._window_error_count = 0


__all__ = ["DestinationCircuitBreaker"]
