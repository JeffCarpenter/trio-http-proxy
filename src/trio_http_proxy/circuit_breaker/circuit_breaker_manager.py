"""CircuitBreakerManager for destination-based circuit breaking."""

from __future__ import annotations

import ipaddress
import threading
import time
from collections import OrderedDict

from ..config import Settings, get_settings
from .circuit_permit import CircuitPermit
from .destination_circuit_breaker import (
    CircuitState,
    DestinationCircuitBreaker,
)


def canonicalize_destination(host: str, port: int) -> tuple[str, int]:
    """Normalize destination host and port for circuit breaker identity mapping."""
    dest_host = host.strip().rstrip(".").lower()
    clean_host = dest_host.strip("[]")
    try:
        ip = ipaddress.ip_address(clean_host)
        dest_host = ip.compressed
    except ValueError:
        pass
    return dest_host, int(port)


class CircuitBreakerManager:
    """Manages DestinationCircuitBreaker instances per canonicalized (host, port)."""

    def __init__(
        self,
        errors_rate: int | None = None,
        period: float | None = None,
        ttl: float | None = None,
        min_calls: int | None = None,
        max_destinations: int | None = None,
        settings: Settings | None = None,
    ) -> None:
        base = settings if settings is not None else get_settings()
        errors_rate = (
            errors_rate if errors_rate is not None else base.circuit_breaker_errors_rate
        )
        period = period if period is not None else base.circuit_breaker_period_seconds
        ttl = ttl if ttl is not None else base.circuit_breaker_ttl_seconds
        min_calls = (
            min_calls if min_calls is not None else base.circuit_breaker_min_calls
        )
        max_destinations = (
            max_destinations
            if max_destinations is not None
            else base.circuit_breaker_max_destinations
        )

        self.settings = settings
        self.errors_rate = errors_rate
        self.period = period
        self.ttl = ttl
        self.min_calls = min_calls
        self.max_destinations = max(1, max_destinations)
        self._breakers: OrderedDict[tuple[str, int], DestinationCircuitBreaker] = (
            OrderedDict()
        )
        self._epoch_counter: int = 0
        self._lock = threading.Lock()

    def get_breaker(self, host: str, port: int) -> DestinationCircuitBreaker:
        key = canonicalize_destination(host, port)
        with self._lock:
            breaker = self._breakers.get(key)
            if breaker is not None:
                self._breakers.move_to_end(key)
                return breaker

            if len(self._breakers) >= self.max_destinations:
                self._evict_one_locked()

            self._epoch_counter += 1
            breaker = DestinationCircuitBreaker(
                errors_rate=self.errors_rate,
                period=self.period,
                ttl=self.ttl,
                min_calls=self.min_calls,
                epoch=self._epoch_counter,
            )
            self._breakers[key] = breaker
            return breaker

    def _evict_one_locked(self) -> None:
        """Evict an idle CLOSED breaker, avoiding eviction of active OPEN breakers."""
        if not self._breakers:
            return
        now = time.monotonic()
        # 1. Look for idle CLOSED breaker with empty calls (after pruning) and no in-flight probe (§6.14)
        for k, b in list(self._breakers.items()):
            with b._lock:
                if b.state == CircuitState.CLOSED and not b._half_open_in_flight:
                    b._prune_calls(now)
                    if len(b._calls) == 0:
                        del self._breakers[k]
                        return

        # 2. Fallback: any CLOSED breaker without active probe
        for k, b in list(self._breakers.items()):
            with b._lock:
                if b.state == CircuitState.CLOSED and not b._half_open_in_flight:
                    del self._breakers[k]
                    return

        # 3. Fallback: any non-OPEN breaker
        for k, b in list(self._breakers.items()):
            with b._lock:
                if b.state != CircuitState.OPEN and not b._half_open_in_flight:
                    del self._breakers[k]
                    return

        # 4. Last resort: evict oldest entry if any exist
        if self._breakers:
            self._breakers.popitem(last=False)

    def acquire_permit(
        self, host: str, port: int, now: float | None = None
    ) -> CircuitPermit | None:
        return self.get_breaker(host, port).acquire_permit(now=now)

    def record_result(
        self,
        host: str,
        port: int,
        exc: Exception | None,
        now: float | None = None,
        permit: CircuitPermit | None = None,
    ) -> None:
        self.get_breaker(host, port).record_result(exc, now=now, permit=permit)

    def clear(self) -> None:
        with self._lock:
            for breaker in self._breakers.values():
                with breaker._lock:
                    breaker.epoch += 1
                    breaker.state = CircuitState.CLOSED
                    breaker._half_open_in_flight = False
                    breaker._active_probe_id = None
                    breaker._probe_started_at = None
                    breaker._calls.clear()
                    breaker._window_error_count = 0
            self._epoch_counter += 10
            self._breakers.clear()


__all__ = [
    "CircuitBreakerManager",
    "canonicalize_destination",
]
