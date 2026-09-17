"""CircuitBreakerManager for destination-based circuit breaking."""

from __future__ import annotations

import ipaddress
import threading
import time
from collections import OrderedDict

from cashews.ttl import ttl_to_seconds

from ..config import CircuitBreakerSettings
from .circuit_permit import CircuitPermit
from .circuit_state import CircuitState
from .destination_circuit_breaker import DestinationCircuitBreaker

DEFAULT_MAX_DESTINATIONS: int = 10_000


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
        errors_rate: int = 50,
        period: float = 60.0,
        ttl: float = 30.0,
        min_calls: int = 3,
        max_destinations: int = DEFAULT_MAX_DESTINATIONS,
        settings: CircuitBreakerSettings | None = None,
    ) -> None:
        if settings is not None:
            errors_rate = settings.circuit_breaker_errors_rate
            parsed_period = ttl_to_seconds(settings.circuit_breaker_period)
            if parsed_period is not None:
                period = float(parsed_period)
            parsed_ttl = ttl_to_seconds(settings.circuit_breaker_ttl)
            if parsed_ttl is not None:
                ttl = float(parsed_ttl)
            min_calls = settings.circuit_breaker_min_calls
            max_destinations = settings.circuit_breaker_max_destinations

        self.settings = settings
        self.errors_rate = errors_rate
        self.period = period
        self.ttl = ttl
        self.min_calls = min_calls
        self.max_destinations = max_destinations
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
        """Evict an idle CLOSED breaker or oldest entry if none idle."""
        now = time.monotonic()
        # 1. Look for idle CLOSED breaker with empty calls (after pruning) and no in-flight probe (§6.14)
        for k, b in self._breakers.items():
            with b._lock:
                if b.state == CircuitState.CLOSED and not b._half_open_in_flight:
                    b._prune_calls(now)
                    if len(b._calls) == 0:
                        del self._breakers[k]
                        return

        # 2. Fallback: any CLOSED breaker without active probe
        for k, b in self._breakers.items():
            with b._lock:
                if b.state == CircuitState.CLOSED and not b._half_open_in_flight:
                    del self._breakers[k]
                    return

        # 3. Last resort: evict oldest entry
        self._breakers.popitem(last=False)

    def acquire_permit(
        self, host: str, port: int, now: float | None = None
    ) -> CircuitPermit | None:
        return self.get_breaker(host, port).acquire_permit(now=now)

    def can_execute(self, host: str, port: int, now: float | None = None) -> bool:
        return self.get_breaker(host, port).can_execute(now=now)

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
    "DEFAULT_MAX_DESTINATIONS",
    "CircuitBreakerManager",
    "canonicalize_destination",
]
