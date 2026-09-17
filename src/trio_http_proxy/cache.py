import asyncio
import enum
import ipaddress
import logging
import math
import queue
import re
import socket
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Callable, Coroutine
from os import getenv
from typing import Any, Self, TypeVar, cast

import cashews
import trio
from cashews import Cache, CircuitBreakerOpen
from cashews.ttl import TTL, ttl_to_seconds

from trio_http_proxy.util import redact_url

logger = logging.getLogger(__name__)

NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class LifecycleState(enum.Enum):
    """Lifecycle states of a ProxyCache instance."""

    NEW = "NEW"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


T = TypeVar("T")

_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


def _int_env(
    name: str,
    default: int,
    min_val: int | None = None,
    max_val: int | None = None,
    _getenv: Callable[..., str | None] = getenv,
) -> int:
    val = _getenv(name)
    if val is None or not val.strip():
        res = default
    else:
        try:
            res = int(val)
        except ValueError:
            raise ValueError(f"Invalid integer for {name}: {val!r}") from None
    if min_val is not None and res < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {res}")
    if max_val is not None and res > max_val:
        raise ValueError(f"{name} must be <= {max_val}, got {res}")
    return res


def _bool_env(
    name: str, default: bool, _getenv: Callable[..., str | None] = getenv
) -> bool:
    val = _getenv(name)
    if val is None or not val.strip():
        return default
    norm = val.strip().lower()
    if norm in _TRUE_VALUES:
        return True
    if norm in _FALSE_VALUES:
        return False
    raise ValueError(
        f"Invalid boolean value for {name}: {val!r}. Expected one of {sorted(_TRUE_VALUES | _FALSE_VALUES)}"
    )


DEFAULT_CACHE_URL = getenv("CACHE_URL", "mem://")
DEFAULT_CACHE_ENABLED = _bool_env("CACHE_ENABLED", True)
DEFAULT_CACHE_NAMESPACE = getenv("CACHE_NAMESPACE", "trio_http_proxy")
DEFAULT_DNS_CACHE_TTL = getenv("DNS_CACHE_TTL", "0s")
DEFAULT_CIRCUIT_BREAKER_ENABLED = _bool_env("CIRCUIT_BREAKER_ENABLED", True)
DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE = _int_env(
    "CIRCUIT_BREAKER_ERRORS_RATE", 50, min_val=1, max_val=99
)
DEFAULT_CIRCUIT_BREAKER_PERIOD = getenv("CIRCUIT_BREAKER_PERIOD", "60s")
DEFAULT_CIRCUIT_BREAKER_TTL = getenv("CIRCUIT_BREAKER_TTL", "30s")
DEFAULT_CIRCUIT_BREAKER_MIN_CALLS = _int_env("CIRCUIT_BREAKER_MIN_CALLS", 3, min_val=1)
DEFAULT_RATE_LIMIT_ENABLED = _bool_env("RATE_LIMIT_ENABLED", False)
DEFAULT_RATE_LIMIT = _int_env("RATE_LIMIT", 0, min_val=0)
DEFAULT_RATE_LIMIT_PERIOD = getenv("RATE_LIMIT_PERIOD", "60s")
DEFAULT_RATE_LIMIT_TTL = getenv("RATE_LIMIT_TTL")


def _failure_policy(value: str) -> str:
    value = value.strip().lower()
    if value not in {"fail-open", "fail-closed"}:
        raise ValueError(
            "CACHE_FAILURE_POLICY must be either 'fail-open' or 'fail-closed'"
        )
    return value


CACHE_FAILURE_POLICY = _failure_policy(getenv("CACHE_FAILURE_POLICY", "fail-open"))
DEFAULT_CACHE_FAILURE_POLICY = CACHE_FAILURE_POLICY
DEFAULT_PROBE_INTERVAL: float = 30.0


class CacheBackendUnavailable(RuntimeError):
    """The configured cache backend could not service an operation."""


class RateLimitError(cashews.RateLimitError):
    """Rate-limit rejection with an optional HTTP Retry-After value."""

    def __init__(
        self,
        *args: Any,
        retry_after: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after

    @property
    def retry_after_seconds(self) -> float | None:
        return float(self.retry_after) if self.retry_after is not None else None


BACKEND_EXCEPTIONS: tuple[type[Exception], ...] = (
    cashews.exceptions.CacheError,
    ConnectionError,
    TimeoutError,
    OSError,
    socket.error,
)


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


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


class CircuitPermit:
    """Tokenized lease representing admission into a destination circuit breaker."""

    def __init__(
        self,
        breaker: DestinationCircuitBreaker,
        epoch: int,
        permit_id: int,
        is_probe: bool = False,
    ) -> None:
        self.breaker = breaker
        self.epoch = epoch
        self.permit_id = permit_id
        self.is_probe = is_probe
        self.was_aborted: bool = False
        self.was_completed: bool = False
        self._lock = threading.Lock()

    def abort(self) -> None:
        """Abort this lease, releasing any probe reservation immediately."""
        with self._lock:
            if self.was_aborted or self.was_completed:
                return
            self.was_aborted = True
        self.breaker._abort_permit(self)

    def mark_completed(self) -> bool:
        with self._lock:
            if self.was_aborted or self.was_completed:
                return False
            self.was_completed = True
            return True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        with self._lock:
            if not self.was_completed and not self.was_aborted:
                self.was_aborted = True
                should_abort = True
            else:
                should_abort = False
        if should_abort:
            self.breaker._abort_permit(self)


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


DEFAULT_MAX_DESTINATIONS: int = 10_000


class CircuitBreakerManager:
    """Manages DestinationCircuitBreaker instances per canonicalized (host, port)."""

    def __init__(
        self,
        errors_rate: int = 50,
        period: float = 60.0,
        ttl: float = 30.0,
        min_calls: int = 3,
        max_destinations: int = DEFAULT_MAX_DESTINATIONS,
    ) -> None:
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


class ProxyCache:
    """Bridges cashews caching, Trio-native circuit breaking, and rate limiting.

    Underlying architecture:
    - DNS caching: Cashews decorator with namespace-isolated keys.
    - Circuit breaking: Explicit Trio-native state machine with gating and
      half-open probe discipline (zero Cashews private key dependencies).
    - Rate limiting: Owned protocol using public increment and expiration methods.
    - Failure policy: Backend configured with suppress=False; ProxyCache is sole
      owner of fail-open vs fail-closed policy.
    """

    def __init__(
        self,
        cache_url: str = DEFAULT_CACHE_URL,
        enabled: bool = DEFAULT_CACHE_ENABLED,
        namespace: str = DEFAULT_CACHE_NAMESPACE,
        dns_cache_ttl: str | None = None,
        circuit_breaker_enabled: bool = DEFAULT_CIRCUIT_BREAKER_ENABLED,
        circuit_breaker_errors_rate: int = DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE,
        circuit_breaker_period: str = DEFAULT_CIRCUIT_BREAKER_PERIOD,
        circuit_breaker_ttl: str = DEFAULT_CIRCUIT_BREAKER_TTL,
        circuit_breaker_min_calls: int = DEFAULT_CIRCUIT_BREAKER_MIN_CALLS,
        circuit_breaker_max_destinations: int = DEFAULT_MAX_DESTINATIONS,
        rate_limit_enabled: bool = DEFAULT_RATE_LIMIT_ENABLED,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_limit_period: str = DEFAULT_RATE_LIMIT_PERIOD,
        rate_limit_ttl: str | None = DEFAULT_RATE_LIMIT_TTL,
        failure_policy: str = CACHE_FAILURE_POLICY,
        cache_failure_policy: str | None = None,
        startup_timeout: float = 5.0,
    ) -> None:
        # Semantic boundary validation in constructor
        if not isinstance(namespace, str) or not namespace.strip():
            raise ValueError("Cache namespace must be a non-empty string")
        if not NAMESPACE_PATTERN.match(namespace):
            raise ValueError(
                f"Cache namespace must match '^[a-zA-Z0-9_-]+$', got {namespace!r}"
            )
        if not 1 <= circuit_breaker_errors_rate <= 99:
            raise ValueError(
                f"circuit_breaker_errors_rate must be between 1 and 99, got {circuit_breaker_errors_rate}"
            )
        if circuit_breaker_min_calls < 1:
            raise ValueError(
                f"circuit_breaker_min_calls must be >= 1, got {circuit_breaker_min_calls}"
            )
        if rate_limit < 0:
            raise ValueError(f"rate_limit must be >= 0, got {rate_limit}")
        if startup_timeout <= 0:
            raise ValueError(f"startup_timeout must be > 0, got {startup_timeout}")

        self.cache_url = cache_url
        self.enabled = enabled
        self.namespace = namespace.strip()
        self._explicit_dns_cache_ttl = dns_cache_ttl is not None
        actual_dns_ttl = (
            dns_cache_ttl if dns_cache_ttl is not None else DEFAULT_DNS_CACHE_TTL
        )
        self.dns_cache_ttl: TTL = actual_dns_ttl
        self.circuit_breaker_enabled = circuit_breaker_enabled
        self.circuit_breaker_errors_rate = circuit_breaker_errors_rate
        self.circuit_breaker_period: TTL = circuit_breaker_period
        self.circuit_breaker_ttl: TTL = circuit_breaker_ttl
        self.circuit_breaker_min_calls = circuit_breaker_min_calls
        self.circuit_breaker_max_destinations = circuit_breaker_max_destinations
        self.rate_limit_enabled = rate_limit_enabled
        self.rate_limit = rate_limit
        self.rate_limit_period: TTL = rate_limit_period
        self.rate_limit_ttl: TTL = (
            rate_limit_ttl if rate_limit_ttl is not None else self.rate_limit_period
        )
        try:
            parsed_period = ttl_to_seconds(self.rate_limit_period)
        except (ValueError, TypeError):
            parsed_period = None
        if parsed_period is None or parsed_period <= 0:
            raise ValueError(
                f"rate_limit_period must evaluate to > 0 seconds, got {rate_limit_period!r}"
            )
        self.rate_limit_period_sec = float(parsed_period)

        try:
            parsed_ttl = ttl_to_seconds(self.rate_limit_ttl)
        except (ValueError, TypeError):
            parsed_ttl = None
        if parsed_ttl is None or parsed_ttl <= 0:
            raise ValueError(
                f"rate_limit_ttl must evaluate to > 0 seconds, got {rate_limit_ttl!r}"
            )
        self.rate_limit_ttl_sec = float(parsed_ttl)

        try:
            parsed_dns_ttl = ttl_to_seconds(self.dns_cache_ttl)
        except (ValueError, TypeError):
            parsed_dns_ttl = None
        self.dns_cache_ttl_sec = (
            float(parsed_dns_ttl) if parsed_dns_ttl is not None and parsed_dns_ttl > 0 else 0.0
        )
        if cache_failure_policy is not None:
            failure_policy = cache_failure_policy
        self.failure_policy = _failure_policy(failure_policy)
        self.cache_failure_policy = self.failure_policy
        self.startup_timeout = startup_timeout

        self.cache = Cache(name="trio_http_proxy")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._lifecycle_state: LifecycleState = LifecycleState.NEW
        self._startup_aborted: threading.Event = threading.Event()
        self._started = False
        self._stopped = False
        self._stopping = False
        self._stop_complete = threading.Event()
        self._degraded = False
        self._last_probe_time: float = 0.0
        self._probe_lock = threading.Lock()

        cb_period = ttl_to_seconds(self.circuit_breaker_period)
        cb_ttl = ttl_to_seconds(self.circuit_breaker_ttl)
        self._circuit_manager = CircuitBreakerManager(
            errors_rate=self.circuit_breaker_errors_rate,
            period=float(cb_period) if cb_period is not None else 60.0,
            ttl=float(cb_ttl) if cb_ttl is not None else 30.0,
            min_calls=self.circuit_breaker_min_calls,
            max_destinations=self.circuit_breaker_max_destinations,
        )

        self._resolve_dns_fn: Callable[[str], Coroutine[Any, Any, list[str]]] | None = (
            None
        )

    def _has_backend_features(self) -> bool:
        """True if remote backend caching features are active and not explicitly disabled."""
        if not self.enabled or not bool(self.cache_url and self.cache_url.strip()):
            return False
        return not (
            self._explicit_dns_cache_ttl
            and self.dns_cache_ttl_sec <= 0
            and not self.rate_limit_enabled
        )

    @property
    def configured_disabled(self) -> bool:
        """True if the cache is disabled by user configuration."""
        return not self.enabled

    @property
    def lifecycle_state(self) -> LifecycleState:
        """Current LifecycleState of the ProxyCache instance."""
        with self._lock:
            return self._lifecycle_state

    @property
    def is_degraded(self) -> bool:
        """True if the cache encountered a backend failure in fail-open mode."""
        return self._degraded

    @property
    def is_running(self) -> bool:
        """True if the cache is active, healthy, and not stopped or degraded."""
        with self._lock:
            return (
                self._lifecycle_state == LifecycleState.RUNNING
                and not self._degraded
            )

    @property
    def is_stopped(self) -> bool:
        """True if the cache bridge has been stopped."""
        with self._lock:
            return self._lifecycle_state == LifecycleState.STOPPED

    def start(self) -> None:
        """Start the background asyncio bridge thread and configure cashews."""
        setup_error: Exception | None = None
        stop_parts: (
            tuple[asyncio.AbstractEventLoop | None, threading.Thread | None] | None
        ) = None
        with self._lock:
            if self._lifecycle_state == LifecycleState.STOPPED:
                raise RuntimeError("ProxyCache cannot be restarted once stopped")
            if self._lifecycle_state in (LifecycleState.STARTING, LifecycleState.RUNNING):
                return
            self._lifecycle_state = LifecycleState.STARTING
            self._stopped = False
            self._stopping = False
            self._degraded = False
            if not self._has_backend_features():
                self._started = True
                self._lifecycle_state = LifecycleState.RUNNING
                return

            startup_queue: queue.Queue[
                tuple[asyncio.AbstractEventLoop | None, BaseException | None]
            ] = queue.Queue(maxsize=1)

            def _run_loop() -> None:
                if self._startup_aborted.is_set():
                    return
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    if self._startup_aborted.is_set():
                        loop.close()
                        return
                    startup_queue.put((loop, None))
                except BaseException as exc:  # noqa: BLE001
                    startup_queue.put((None, exc))
                    return
                try:
                    if self._startup_aborted.is_set():
                        return
                    loop.run_forever()
                finally:
                    loop.close()

            self._thread = threading.Thread(
                target=_run_loop, daemon=True, name="cashews-trio-bridge"
            )
            self._thread.start()

            try:
                loop, startup_exc = startup_queue.get(timeout=self.startup_timeout)
            except queue.Empty:
                self._startup_aborted.set()
                self._stopped = True
                self._started = False
                self._lifecycle_state = LifecycleState.STOPPED
                raise RuntimeError(
                    f"Timed out waiting {self.startup_timeout}s for asyncio bridge thread startup"
                ) from None

            if startup_exc is not None or loop is None:
                self._startup_aborted.set()
                self._stopped = True
                self._started = False
                self._lifecycle_state = LifecycleState.STOPPED
                raise RuntimeError(
                    f"Asyncio bridge thread startup failed: {startup_exc}"
                ) from startup_exc

            self._loop = loop

            try:
                # Backend is configured with suppress=False to propagate all failures.
                # ProxyCache is the sole owner of fail-open vs fail-closed policy.
                self.cache.setup(self.cache_url, suppress=False)
                self._register_decorators()
                fut = asyncio.run_coroutine_threadsafe(self.cache.ping(), self._loop)
                fut.result(timeout=self.startup_timeout)
            except Exception as exc:  # noqa: BLE001
                setup_error = exc
                logger.warning(
                    "Failed to setup cashews cache or startup ping failed with %s: %s",
                    redact_url(self.cache_url),
                    exc,
                )
                if self.cache_failure_policy == "fail-closed":
                    self._stopped = True
                    self._started = False
                    self._degraded = False
                    self._lifecycle_state = LifecycleState.STOPPED
                    stop_parts = self._begin_stop_locked()
                else:
                    self._degraded = True
                    self._last_probe_time = time.monotonic()
                    self._started = True
                    self._lifecycle_state = LifecycleState.RUNNING
            else:
                self._started = True
                self._degraded = False
                self._last_probe_time = time.monotonic()
                self._lifecycle_state = LifecycleState.RUNNING

        if setup_error is not None:
            if stop_parts is not None:
                self._finish_stop(*stop_parts)
            if self.cache_failure_policy == "fail-open":
                self._degraded = True
                self._last_probe_time = time.monotonic()
                with self._lock:
                    self._stopped = False
                    self._started = True
                    self._lifecycle_state = LifecycleState.RUNNING
                return
            raise CacheBackendUnavailable(
                f"Unable to setup cache backend {redact_url(self.cache_url)!r}: {setup_error}"
            ) from setup_error

    def stop(self) -> None:
        """Stop the bridge, draining pending work before closing its loop."""
        with self._lock:
            if self._lifecycle_state == LifecycleState.STOPPED:
                return
            if self._stopping:
                done = self._stop_complete
                parts = None
            elif self._loop is None and self._thread is None:
                self._stopped = True
                self._started = False
                self._lifecycle_state = LifecycleState.STOPPED
                return
            elif not self._started and self._loop is None and self._thread is None:
                self._stopped = True
                self._lifecycle_state = LifecycleState.STOPPED
                return
            else:
                self._stopped = True
                self._started = False
                self._lifecycle_state = LifecycleState.STOPPING
                parts = self._begin_stop_locked()
                done = self._stop_complete

        if parts is None:
            if not done.wait(2.0):
                raise RuntimeError("Timed out stopping ProxyCache bridge")
            return
        self._finish_stop(*parts)

    def _begin_stop_locked(
        self,
    ) -> tuple[asyncio.AbstractEventLoop | None, threading.Thread | None]:
        self._stopping = True
        self._stop_complete.clear()
        return self._loop, self._thread

    def _finish_stop(
        self, loop: asyncio.AbstractEventLoop | None, thread: threading.Thread | None
    ) -> None:
        if loop is not None and not loop.is_closed():

            async def shutdown() -> None:
                current = asyncio.current_task()
                pending = [
                    task for task in asyncio.all_tasks(loop) if task is not current
                ]
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                try:
                    await loop.shutdown_asyncgens()
                except Exception:  # noqa: BLE001, S110
                    pass
                try:
                    await self.cache.close()
                finally:
                    try:
                        if hasattr(loop, "shutdown_default_executor"):
                            await loop.shutdown_default_executor()
                    except Exception:  # noqa: BLE001, S110
                        pass
                    finally:
                        loop.stop()

            try:
                loop.call_soon_threadsafe(lambda: loop.create_task(shutdown()))
            except RuntimeError:
                pass

        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        alive = thread is not None and thread.is_alive()
        open_loop = loop is not None and not loop.is_closed()
        with self._lock:
            self._stopping = False
            self._lifecycle_state = LifecycleState.STOPPED
            if not alive and not open_loop:
                self._stop_complete.set()
                if self._thread is thread:
                    self._thread = None
                if self._loop is loop:
                    self._loop = None
                self._resolve_dns_fn = None
        if alive:
            raise RuntimeError("Timed out stopping ProxyCache bridge")
        if open_loop:
            raise RuntimeError("ProxyCache bridge loop did not close")

    def _ensure_started(self) -> None:
        with self._lock:
            if self._lifecycle_state == LifecycleState.STOPPED or self._stopped:
                raise RuntimeError("ProxyCache has been stopped")
            if not self._started and not self._has_backend_features():
                self._started = True
                self._lifecycle_state = LifecycleState.RUNNING
                return
        self.start()

    def run_coroutine(self, coro: Coroutine[Any, Any, T], timeout: float = 10.0) -> T:
        """Execute a coroutine in the background asyncio event loop synchronously."""
        try:
            self._ensure_started()
            with self._lock:
                if (
                    self._stopped
                    or self._stopping
                    or not self._loop
                    or not self._loop.is_running()
                ):
                    raise RuntimeError("Asyncio bridge event loop is not running")
                fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        except Exception:
            coro.close()
            raise
        try:
            return fut.result(timeout=timeout)
        except Exception:
            fut.cancel()
            raise

    async def run(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> T:
        """Execute an asyncio coroutine function on the bridge from within Trio.

        Cancellation & Timeout Semantics:
        When Trio cancels the caller (e.g. via nursery cancellation) or times out
        via trio.fail_after(timeout), fut.cancel() is invoked on the underlying
        concurrent.futures.Future. This requests cancellation of the asyncio Task
        scheduled on the bridge loop. Callers should note that cancellation is
        asynchronous and cannot guarantee immediate rollback of remote backend side
        effects.
        """
        self._ensure_started()
        with self._lock:
            if (
                self._stopped
                or self._stopping
                or not self._loop
                or not self._loop.is_running()
            ):
                raise RuntimeError("Asyncio bridge event loop is not running")
            coro = coro_fn(*args, **kwargs)
            try:
                fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
            except Exception:
                coro.close()
                raise

        event = trio.Event()
        token = trio.lowlevel.current_trio_token()

        def _done_callback(_: Any) -> None:
            try:
                token.run_sync_soon(event.set)
            except trio.RunFinishedError:
                pass

        fut.add_done_callback(_done_callback)

        try:
            with trio.fail_after(timeout):
                await event.wait()
            return fut.result(timeout=0)
        except (trio.Cancelled, trio.TooSlowError):
            fut.cancel()
            raise

    def _register_decorators(self) -> None:
        c = self.cache

        # DNS Resolution Caching with namespace
        if self.dns_cache_ttl_sec > 0:
            dns_key = f"{self.namespace}:dns:{{host}}"

            @c(ttl=self.dns_cache_ttl, key=dns_key)
            async def _resolve_dns(host: str) -> list[str]:
                if self._loop is None:
                    raise RuntimeError("Event loop is not initialized")
                try:
                    addr_infos = await self._loop.run_in_executor(
                        None,
                        socket.getaddrinfo,
                        host,
                        None,
                        socket.AF_UNSPEC,
                        socket.SOCK_STREAM,
                    )
                except OSError as exc:
                    cast(Any, exc)._trio_http_proxy_dns_error = True
                    raise
                if not addr_infos:
                    exc = OSError(f"Name resolution failed for {host}")
                    cast(Any, exc)._trio_http_proxy_dns_error = True
                    raise exc
                seen: set[str] = set()
                ips: list[str] = []
                for info in addr_infos:
                    ip = str(info[4][0])
                    if ip not in seen:
                        seen.add(ip)
                        ips.append(ip)
                return ips

            self._resolve_dns_fn = _resolve_dns
        else:
            self._resolve_dns_fn = None

    def _backend_error(self, operation: str, exc: Exception) -> CacheBackendUnavailable:
        return CacheBackendUnavailable(
            f"Cache backend failed during {operation}: {exc}"
        )

    def _handle_backend_failure(self, operation: str, exc: Exception) -> None:
        if not isinstance(exc, BACKEND_EXCEPTIONS):
            raise exc
        if self._stopped:
            raise exc
        if self.cache_failure_policy == "fail-open":
            logger.warning(
                "Cache backend failed during %s; operating degraded (bypassing cache): %s",
                operation,
                exc,
            )
            self._degraded = True
            self._last_probe_time = time.monotonic()
            return
        raise self._backend_error(operation, exc) from exc

    async def _maybe_probe_backend(self) -> None:
        """Periodically probe degraded backend to recover health when ping succeeds."""
        if (
            not self._degraded
            or not self._has_backend_features()
            or self._stopped
            or self._stopping
            or self._loop is None
        ):
            return

        now = time.monotonic()
        with self._probe_lock:
            if now - self._last_probe_time < DEFAULT_PROBE_INTERVAL:
                return
            self._last_probe_time = now

        try:
            await self.run(self.cache.ping, timeout=5.0)
        except BACKEND_EXCEPTIONS as exc:
            logger.debug("Cache backend probe ping failed: %s", exc)
        else:
            logger.info("Cache backend recovered; restored normal operations")
            self._degraded = False

    async def resolve_hosts(self, host: str) -> list[str]:
        """Resolve a destination host, bypassing cache on fail-open outages or degraded state."""
        self._ensure_started()
        if self._degraded:
            await self._maybe_probe_backend()
        if not self.enabled or self._degraded or self._resolve_dns_fn is None:
            return [host]

        try:
            ipaddress.ip_address(host.strip("[]"))
            return [host]
        except ValueError:
            pass

        try:
            return await self.run(self._resolve_dns_fn, host)
        except Exception as exc:
            if isinstance(exc, OSError) and (
                isinstance(exc, socket.gaierror)
                or getattr(exc, "_trio_http_proxy_dns_error", False)
            ):
                raise
            self._handle_backend_failure("DNS resolution", exc)
            return [host]

    async def resolve_host(self, host: str) -> str:
        """Resolve a destination host to one IP string."""
        hosts = await self.resolve_hosts(host)
        return hosts[0] if hosts else host

    async def check_circuit(self, host: str, port: int) -> CircuitPermit | None:
        """Verify that the destination circuit is not open and acquire a permit."""
        self._ensure_started()
        if not self.circuit_breaker_enabled:
            return None
        permit = self._circuit_manager.acquire_permit(host, port)
        if permit is None:
            raise CircuitBreakerOpen(f"Circuit breaker open for {host}:{port}")
        return permit

    async def record_connection_result(
        self,
        host: str,
        port: int,
        exc: Exception | None = None,
        permit: CircuitPermit | None = None,
    ) -> None:
        """Record a connection result in the Trio-native circuit breaker."""
        self._ensure_started()
        if not self.circuit_breaker_enabled:
            return
        self._circuit_manager.record_result(host, port, exc, permit=permit)

    async def check_rate_limit(self, key: str) -> None:
        """Verify a key's quota and attach backend expiry on rejection."""
        self._ensure_started()
        if self._degraded:
            await self._maybe_probe_backend()
        if (
            not self.enabled
            or self._degraded
            or not self.rate_limit_enabled
            or self.rate_limit <= 0
        ):
            return

        rate_key = f"{self.namespace}:rate:{key}"
        parsed_period = ttl_to_seconds(self.rate_limit_period)
        period_sec = int(parsed_period) if parsed_period is not None else 60
        parsed_ttl = (
            ttl_to_seconds(self.rate_limit_ttl)
            if self.rate_limit_ttl is not None
            else None
        )
        ttl_sec = int(parsed_ttl) if parsed_ttl is not None else period_sec

        async def _rate_limit_backend() -> int | None:
            count = await self.cache.incr(rate_key, expire=period_sec)
            if count > self.rate_limit:
                if count == self.rate_limit + 1:
                    await self.cache.expire(rate_key, ttl_sec)
                expiry = await self.cache.get_expire(rate_key)
                if expiry is None or expiry <= 0:
                    expiry = ttl_sec
                return max(1, math.ceil(float(expiry)))
            return None

        try:
            retry_after = await self.run(_rate_limit_backend)
        except Exception as exc:  # noqa: BLE001
            self._handle_backend_failure("rate limiting", exc)
            return

        if retry_after is not None:
            raise RateLimitError(retry_after=retry_after)

    async def clear(self) -> None:
        """Clear all entries owned by this proxy cache namespace."""
        self._ensure_started()
        self._circuit_manager.clear()
        if not self.enabled or self._degraded:
            return
        pattern = f"{self.namespace}:*"
        try:
            await self.run(self.cache.delete_match, pattern)
        except Exception as exc:  # noqa: BLE001
            self._handle_backend_failure("cache clear", exc)

    async def dangerous_flush_entire_database(self) -> None:
        """Explicit database-wide flush across all namespaces."""
        self._ensure_started()
        self._circuit_manager.clear()
        if not self.enabled or self._degraded:
            return
        try:
            await self.run(self.cache.clear)
        except Exception as exc:  # noqa: BLE001
            self._handle_backend_failure("cache clear", exc)

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.stop()

    async def __aenter__(self) -> Self:
        await trio.to_thread.run_sync(self.start)
        return self

    async def __aexit__(self, *args: object) -> None:
        await trio.to_thread.run_sync(self.stop)


def get_client_key(stream: Any) -> str | None:
    """Extract a rate-limiting key (typically client IP) from a stream.

    Note: When deployed behind a reverse proxy or TCP load balancer, this
    extracts the upstream load balancer's IP rather than the true client IP.
    For an HTTP CONNECT proxy, client IP spoofing protections or PROXY protocol
    support must be added if rate-limiting individual clients behind a proxy.
    """
    try:
        sock = getattr(stream, "socket", None)
        if sock and hasattr(sock, "getpeername"):
            peer = sock.getpeername()
            if isinstance(peer, tuple) and len(peer) >= 1:
                return str(peer[0])
    except (OSError, AttributeError):
        pass
    return None


DEFAULT_PROXY_CACHE = ProxyCache()

__all__ = [
    "CACHE_FAILURE_POLICY",
    "DEFAULT_CACHE_ENABLED",
    "DEFAULT_CACHE_FAILURE_POLICY",
    "DEFAULT_CACHE_NAMESPACE",
    "DEFAULT_CACHE_URL",
    "DEFAULT_CIRCUIT_BREAKER_ENABLED",
    "DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE",
    "DEFAULT_CIRCUIT_BREAKER_MIN_CALLS",
    "DEFAULT_CIRCUIT_BREAKER_PERIOD",
    "DEFAULT_CIRCUIT_BREAKER_TTL",
    "DEFAULT_DNS_CACHE_TTL",
    "DEFAULT_PROXY_CACHE",
    "DEFAULT_RATE_LIMIT",
    "DEFAULT_RATE_LIMIT_ENABLED",
    "DEFAULT_RATE_LIMIT_PERIOD",
    "DEFAULT_RATE_LIMIT_TTL",
    "CacheBackendUnavailable",
    "CircuitBreakerOpen",
    "CircuitPermit",
    "CircuitState",
    "DestinationCircuitBreaker",
    "ProxyCache",
    "RateLimitError",
    "get_client_key",
]
