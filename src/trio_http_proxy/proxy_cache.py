"""ProxyCache implementation bridging cashews, circuit breaking, and rate limiting."""

from __future__ import annotations

import asyncio
import enum
import inspect
import ipaddress
import logging
import math
import queue
import socket
import threading
import time
import warnings
from collections.abc import Callable, Coroutine
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Protocol, Self, TypeVar, cast, runtime_checkable

import cashews
import trio
from cashews import Cache, CircuitBreakerOpen

from .circuit_breaker import (
    CircuitBreakerManager,
    CircuitPermit,
)
from .dependencies import CacheBackend, HostResolver, ThreadFactory, ThreadHandle
from .errors import CacheBackendUnavailable, RateLimitError
from .util import redact_url

logger = logging.getLogger(__name__)


class LifecycleState(enum.Enum):
    """Lifecycle states of a ProxyCache instance."""

    NEW = "NEW"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


T = TypeVar("T")

from .config import (
    Settings,
    normalize_failure_policy,
    validate_namespace,
)
from .config.builder import ProxyCacheBuilder

BACKEND_EXCEPTIONS: tuple[type[Exception], ...] = (
    cashews.exceptions.CacheError,
    ConnectionError,
    TimeoutError,
    OSError,
    socket.error,
)


@dataclass(frozen=True, slots=True)
class ConnectionRecord:
    """Formalized record of a destination connection result."""

    host: str
    port: int
    error: Exception | None = None
    permit: CircuitPermit | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("host must be specified to build ConnectionRecord")
        if (
            not isinstance(self.port, int)
            or isinstance(self.port, bool)
            or not (1 <= self.port <= 65535)
        ):
            raise ValueError(f"port must be between 1 and 65535, got {self.port}")
        if self.error is not None and self.permit is not None:
            raise ValueError(
                "ConnectionRecord cannot simultaneously hold both an error and a permit"
            )
        if self.host != self.host.strip():
            object.__setattr__(self, "host", self.host.strip())

    @property
    def is_success(self) -> bool:
        """Return True if connection succeeded without error."""
        return self.error is None

    @property
    def has_permit(self) -> bool:
        """Return True if a circuit breaker permit is associated."""
        return self.permit is not None


async def _dispatch_connection_record(
    proxy_cache: Any,
    record: ConnectionRecord,
    *,
    strict_legacy: bool = False,
) -> None:
    """Invoke one recorder, preserving the deprecated builder's strict policy.

    Modern recorders always take precedence. Direct-record callers ignore a
    missing/non-callable legacy recorder; builders historically raise for a
    present but non-callable recorder, so that distinction is intentional.
    """
    if hasattr(proxy_cache, "record_connection_result"):
        res = proxy_cache.record_connection_result(record)
    else:
        if strict_legacy:
            if not hasattr(proxy_cache, "record_result"):
                return
            recorder = proxy_cache.record_result
        else:
            recorder = getattr(proxy_cache, "record_result", None)
            if not callable(recorder):
                return
        res = recorder(record.host, record.port, record.error, permit=record.permit)
    if inspect.isawaitable(res):
        await res


class ConnectionResultBuilder:
    """[Deprecated] Fluent builder for constructing ConnectionRecord instances."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        error: Exception | None = None,
        permit: CircuitPermit | None = None,
    ) -> None:
        warnings.warn(
            "ConnectionResultBuilder is deprecated; construct ConnectionRecord directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._host: str | None = host
        self._port: int | None = port
        self._error: Exception | None = error
        self._permit: CircuitPermit | None = permit

    def with_host(self, host: str) -> Self:
        self._host = host
        return self

    def with_port(self, port: int) -> Self:
        self._port = port
        return self

    def with_target(self, host: str, port: int) -> Self:
        self._host = host
        self._port = port
        return self

    def with_error(self, error: Exception | None) -> Self:
        self._error = error
        return self

    def with_success(self) -> Self:
        self._error = None
        return self

    def with_permit(self, permit: CircuitPermit | None) -> Self:
        self._permit = permit
        return self

    def build(self) -> ConnectionRecord:
        if self._host is None or not self._host.strip():
            raise ValueError("host must be specified to build ConnectionRecord")
        if self._port is None:
            raise ValueError("port must be specified to build ConnectionRecord")
        return ConnectionRecord(
            host=self._host,
            port=self._port,
            error=self._error,
            permit=self._permit,
        )

    async def record_to(self, proxy_cache: Any) -> None:
        """Convenience method to build and dispatch this result to proxy_cache."""
        if proxy_cache is not None:
            await _dispatch_connection_record(
                proxy_cache, self.build(), strict_legacy=True
            )


@runtime_checkable
class ProxyCacheProtocol(Protocol):
    """Protocol defining the cache and resilience contract required by the HTTP proxy."""

    async def record_connection_result(
        self,
        record: ConnectionRecord | ConnectionResultBuilder,
    ) -> None:
        """Record a connection outcome for circuit breaker tracking."""
        ...

    async def check_circuit(self, host: str, port: int) -> CircuitPermit | None:
        """Verify that the destination circuit is not open and acquire a permit."""
        ...

    async def check_rate_limit(self, key: str) -> None:
        """Verify a client's quota and attach backend expiry on rejection."""
        ...

    async def resolve_host(self, host: str) -> str:
        """Resolve a destination host to one IP string."""
        ...

    async def resolve_hosts(self, host: str) -> list[str]:
        """Resolve a destination host, returning all resolved IP strings."""
        ...

    def start(self) -> None:
        """Start the proxy cache bridge."""
        ...

    def stop(self) -> None:
        """Stop the proxy cache bridge."""
        ...


def resolve_addresses(host: str) -> list[str]:
    """Resolve TCP addresses in system order; called off the Trio thread."""
    return [
        str(info[4][0])
        for info in socket.getaddrinfo(host, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    ]


class ProxyCache:
    """Bridges cashews caching, Trio-native circuit breaking, and rate limiting.

    Underlying architecture:
    - DNS caching: Public backend get/set operations with namespace-isolated keys.
    - Circuit breaking: Explicit Trio-native state machine with gating and
      half-open probe discipline (zero Cashews private key dependencies).
    - Rate limiting: Owned protocol using public increment and expiration methods.
    - Failure policy: Backend configured with suppress=False; ProxyCache is sole
      owner of fail-open vs fail-closed policy.
    """

    @classmethod
    def builder(
        cls, settings: Settings | None = None, **kwargs: Any
    ) -> ProxyCacheBuilder:
        """Return a fluent builder for configuring and constructing a ProxyCache."""
        return ProxyCacheBuilder(settings=settings, **kwargs)

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        backend: CacheBackend | None = None,
        resolver: HostResolver | None = None,
        clock: Callable[[], float] | None = None,
        thread_factory: ThreadFactory | None = None,
        **kwargs: Any,
    ) -> None:
        if settings is not None:
            if kwargs:
                self.settings = settings.model_copy(update=kwargs)
            else:
                self.settings = settings
        elif kwargs:
            mapped = dict(kwargs)
            if "namespace" in mapped and "cache_namespace" not in mapped:
                mapped["cache_namespace"] = mapped.pop("namespace")
            if "failure_policy" in mapped and "cache_failure_policy" not in mapped:
                mapped["cache_failure_policy"] = mapped.pop("failure_policy")
            if "enabled" in mapped and "cache_enabled" not in mapped:
                mapped["cache_enabled"] = mapped.pop("enabled")
            self.settings = Settings(**mapped)
        else:
            self.settings = Settings()
        s = self.settings

        self.cache_url: str = s.cache_url
        self.enabled: bool = s.cache_enabled
        self.namespace: str = s.cache_namespace
        self._explicit_dns_cache_ttl: bool = "dns_cache_ttl" in s.model_fields_set
        self.dns_cache_ttl: str = s.dns_cache_ttl
        self.circuit_breaker_enabled: bool = s.circuit_breaker_enabled
        self.circuit_breaker_errors_rate: int = s.circuit_breaker_errors_rate
        self.circuit_breaker_period: str = s.circuit_breaker_period
        self.circuit_breaker_ttl: str = s.circuit_breaker_ttl
        self.circuit_breaker_min_calls: int = s.circuit_breaker_min_calls
        self.circuit_breaker_max_destinations: int = s.circuit_breaker_max_destinations
        self.rate_limit_enabled: bool = s.rate_limit_enabled
        self.rate_limit: int = s.rate_limit
        self.rate_limit_period: str = s.rate_limit_period
        self.rate_limit_ttl: str | None = (
            s.rate_limit_ttl if s.rate_limit_ttl is not None else self.rate_limit_period
        )
        self.rate_limit_period_sec: float = s.rate_limit_period_seconds
        if s.rate_limit_ttl is not None and s.rate_limit_ttl_seconds is not None:
            self.rate_limit_ttl_sec: float = s.rate_limit_ttl_seconds
        else:
            self.rate_limit_ttl_sec: float = s.rate_limit_period_seconds
        self.dns_cache_ttl_sec: float = s.dns_cache_ttl_seconds
        self.cache_failure_policy: str = s.cache_failure_policy
        self.probe_interval: float = s.probe_interval
        self.startup_timeout: float = s.startup_timeout

        self.cache: CacheBackend = (
            backend if backend is not None else Cache(name="trio_http_proxy")
        )
        self._resolver = resolver if resolver is not None else resolve_addresses
        self._clock = clock if clock is not None else time.monotonic
        self._thread_factory: ThreadFactory = (
            thread_factory if thread_factory is not None else threading.Thread
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: ThreadHandle | None = None
        self._lock = threading.Lock()
        self._lifecycle_state: LifecycleState = LifecycleState.NEW
        self._startup_aborted: threading.Event = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutdown_future: Future[None] | None = None
        self._degraded = False
        self._last_probe_time: float = 0.0
        self._probe_lock = threading.Lock()

        self._circuit_manager = CircuitBreakerManager(
            errors_rate=self.circuit_breaker_errors_rate,
            period=s.circuit_breaker_period_seconds,
            ttl=s.circuit_breaker_ttl_seconds,
            min_calls=self.circuit_breaker_min_calls,
            max_destinations=self.circuit_breaker_max_destinations,
        )

        self._resolve_dns_fn: Callable[[str], Coroutine[Any, Any, list[str]]] | None = (
            None
        )

    @property
    def failure_policy(self) -> str:
        return self.cache_failure_policy

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
                self._lifecycle_state == LifecycleState.RUNNING and not self._degraded
            )

    @property
    def is_stopped(self) -> bool:
        """True if the cache bridge has been stopped."""
        with self._lock:
            return self._lifecycle_state == LifecycleState.STOPPED

    def start(self) -> None:
        """Start the background asyncio bridge thread and configure cashews."""
        setup_error: Exception | None = None
        with self._lock:
            if self._lifecycle_state in (
                LifecycleState.STOPPING,
                LifecycleState.STOPPED,
            ):
                raise RuntimeError("ProxyCache cannot be restarted once stopped")
            if self._lifecycle_state in (
                LifecycleState.STARTING,
                LifecycleState.RUNNING,
            ):
                return
            self._lifecycle_state = LifecycleState.STARTING
            self._degraded = False
            if not self._has_backend_features():
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

            try:
                self._thread = self._thread_factory(
                    target=_run_loop, daemon=True, name="cashews-trio-bridge"
                )
                self._thread.start()
            except Exception:
                self._startup_aborted.set()
                self._lifecycle_state = LifecycleState.STOPPED
                raise

            try:
                loop, startup_exc = startup_queue.get(timeout=self.startup_timeout)
            except queue.Empty:
                self._startup_aborted.set()
                # Publication can race Queue.get's deadline. Once aborted, either
                # the worker sees the event or we stop the loop it just published.
                try:
                    late_loop, _ = startup_queue.get_nowait()
                except queue.Empty:
                    pass
                else:
                    if late_loop is not None:
                        try:
                            late_loop.call_soon_threadsafe(late_loop.stop)
                        except RuntimeError:
                            pass  # The worker already closed the aborted loop.
                self._lifecycle_state = LifecycleState.STOPPED
                raise RuntimeError(
                    f"Timed out waiting {self.startup_timeout}s for asyncio bridge thread startup"
                ) from None

            if startup_exc is not None or loop is None:
                self._startup_aborted.set()
                self._lifecycle_state = LifecycleState.STOPPED
                raise RuntimeError(
                    f"Asyncio bridge thread startup failed: {startup_exc}"
                ) from startup_exc

            self._loop = loop

            try:
                # Backend is configured with suppress=False to propagate all failures.
                # ProxyCache is the sole owner of fail-open vs fail-closed policy.
                self.cache.setup(self.cache_url, suppress=False)
                self._register_dns_cache()
            except Exception as exc:  # noqa: BLE001
                setup_error = exc

        # Never hold the state lock while waiting for backend I/O.
        if setup_error is None:
            try:
                self.run_coroutine(self.cache.ping(), timeout=self.startup_timeout)
            except Exception as exc:  # noqa: BLE001
                setup_error = exc

        with self._lock:
            # Only the still-active startup may publish readiness.
            if self._lifecycle_state is not LifecycleState.STARTING:
                return
            if setup_error is not None:
                logger.warning(
                    "Failed to setup cashews cache or startup ping failed with %s: %s",
                    redact_url(self.cache_url),
                    setup_error,
                )
                if self.cache_failure_policy == "fail-closed":
                    self._lifecycle_state = LifecycleState.STOPPING
                else:
                    self._degraded = True
            self._last_probe_time = self._clock()
            if self._lifecycle_state is LifecycleState.STARTING:
                self._lifecycle_state = LifecycleState.RUNNING
                return

        # Fail-closed startup shares the normal shutdown owner and cleanup task.
        self.stop()
        raise CacheBackendUnavailable(
            f"Unable to setup cache backend {redact_url(self.cache_url)!r}: {setup_error}"
        ) from setup_error

    def stop(self) -> None:
        """Reject new work, then serialize bounded, retryable bridge cleanup."""
        if not self._shutdown_lock.acquire(timeout=2.0):
            raise RuntimeError("Timed out stopping ProxyCache bridge")
        try:
            with self._lock:
                if self._lifecycle_state is LifecycleState.STOPPED:
                    return
                self._lifecycle_state = LifecycleState.STOPPING
                loop, thread = self._loop, self._thread
            self._finish_stop(loop, thread)
        finally:
            self._shutdown_lock.release()

    async def _shutdown_bridge(self, loop: asyncio.AbstractEventLoop) -> None:
        """Run exactly once on the bridge, independently of stop() join retries."""
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks(loop) if task is not current]
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
                await loop.shutdown_default_executor()
            except Exception:  # noqa: BLE001, S110
                pass
            finally:
                loop.stop()

    def _finish_stop(
        self, loop: asyncio.AbstractEventLoop | None, thread: ThreadHandle | None
    ) -> None:
        if loop is not None and not loop.is_closed() and self._shutdown_future is None:
            coro = self._shutdown_bridge(loop)
            try:
                self._shutdown_future = asyncio.run_coroutine_threadsafe(coro, loop)
            except RuntimeError:
                coro.close()

        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        alive = thread is not None and thread.is_alive()
        open_loop = loop is not None and not loop.is_closed()
        if alive:
            raise RuntimeError("Timed out stopping ProxyCache bridge")
        if open_loop:
            raise RuntimeError("ProxyCache bridge loop did not close")
        with self._lock:
            self._lifecycle_state = LifecycleState.STOPPED
            self._thread = None
            self._loop = None
            self._shutdown_future = None
            self._resolve_dns_fn = None

    def _ensure_started(self) -> None:
        with self._lock:
            if self._lifecycle_state in (
                LifecycleState.STOPPING,
                LifecycleState.STOPPED,
            ):
                raise RuntimeError("ProxyCache has been stopped")
            if self._lifecycle_state is not LifecycleState.NEW:
                return
        self.start()

    def _submit(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Atomically admit and submit work; never construct rejected coroutines."""
        self._ensure_started()
        with self._lock:
            loop = self._loop
            if (
                self._lifecycle_state
                not in (LifecycleState.STARTING, LifecycleState.RUNNING)
                or loop is None
                or loop.is_closed()
                or (
                    self._lifecycle_state is LifecycleState.RUNNING
                    and not loop.is_running()
                )
            ):
                raise RuntimeError("Asyncio bridge event loop is not running")
            coro = coro_fn(*args, **kwargs)
            try:
                return asyncio.run_coroutine_threadsafe(coro, loop)
            except Exception:
                coro.close()
                raise

    def run_coroutine(self, coro: Coroutine[Any, Any, T], timeout: float = 10.0) -> T:
        """Execute bridge work synchronously; timeout cancels the submitted task."""
        try:
            fut = self._submit(lambda: coro)
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
        fut = self._submit(coro_fn, *args, **kwargs)

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
        except trio.Cancelled, trio.TooSlowError:
            fut.cancel()
            raise

    def _register_dns_cache(self) -> None:
        # Preserve the namespace/key/TTL contract without backend-specific decorators.
        self._resolve_dns_fn = (
            self._resolve_cached_dns if self.dns_cache_ttl_sec > 0 else None
        )

    async def _resolve_cached_dns(self, host: str) -> list[str]:
        key = f"{self.namespace}:dns:{host}"
        missing = object()
        cached = await self.cache.get(key, default=missing)
        if cached is not missing:
            return cached
        if self._loop is None:
            raise RuntimeError("Event loop is not initialized")
        try:
            addresses = await self._loop.run_in_executor(None, self._resolver, host)
            ips = list(dict.fromkeys(addresses))
            if not ips:
                raise OSError(f"Name resolution failed for {host}")
        except OSError as exc:
            cast(Any, exc)._trio_http_proxy_dns_error = True
            raise
        await self.cache.set(key, ips, expire=self.dns_cache_ttl_sec)
        return ips

    def _backend_error(self, operation: str, exc: Exception) -> CacheBackendUnavailable:
        return CacheBackendUnavailable(
            f"Cache backend failed during {operation}: {exc}"
        )

    def _handle_backend_failure(self, operation: str, exc: Exception) -> None:
        if not isinstance(exc, BACKEND_EXCEPTIONS):
            raise exc
        if self.lifecycle_state in (LifecycleState.STOPPING, LifecycleState.STOPPED):
            raise exc
        if self.cache_failure_policy == "fail-open":
            logger.warning(
                "Cache backend failed during %s; operating degraded (bypassing cache): %s",
                operation,
                exc,
            )
            with self._lock:
                self._degraded = True
                self._last_probe_time = self._clock()
            return
        raise self._backend_error(operation, exc) from exc

    async def _maybe_probe_backend(self) -> None:
        """Periodically probe degraded backend to recover health when ping succeeds."""
        if (
            not self._degraded
            or not self._has_backend_features()
            or self.lifecycle_state is not LifecycleState.RUNNING
            or self._loop is None
        ):
            return

        now = self._clock()
        with self._probe_lock:
            if now - self._last_probe_time < self.probe_interval:
                return
            self._last_probe_time = now

        try:
            await self.run(self.cache.ping, timeout=self.startup_timeout)
        except BACKEND_EXCEPTIONS as exc:
            logger.debug("Cache backend probe ping failed: %s", exc)
        else:
            logger.info("Cache backend recovered; restored normal operations")
            with self._lock:
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

    @classmethod
    def result_builder(
        cls,
        host: str | None = None,
        port: int | None = None,
    ) -> ConnectionResultBuilder:
        """[Deprecated] Return a new ConnectionResultBuilder initialized with optional target."""
        warnings.warn(
            "ProxyCache.result_builder is deprecated; construct ConnectionRecord directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ConnectionResultBuilder(host=host, port=port)

    async def aclose(self) -> None:
        """Asynchronously stop and release background cache resources."""
        await trio.to_thread.run_sync(self.stop)

    async def record_connection_result(
        self,
        record: ConnectionRecord | ConnectionResultBuilder,
    ) -> None:
        """Record a connection result in the Trio-native circuit breaker."""
        self._ensure_started()
        if not self.circuit_breaker_enabled:
            return
        if isinstance(record, ConnectionResultBuilder):
            rec = record.build()
        else:
            rec = record
        self._circuit_manager.record_result(
            rec.host, rec.port, rec.error, permit=rec.permit
        )

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
        period_sec = int(self.rate_limit_period_sec)
        ttl_sec = int(self.rate_limit_ttl_sec)

        async def _rate_limit_backend() -> int | None:
            count = await self.cache.incr(rate_key, expire=period_sec)
            if count > self.rate_limit:
                expiry = await self.cache.get_expire(rate_key)
                if expiry is None or expiry <= 0:
                    # No ban TTL attached yet (first violation, or a
                    # concurrent burst raced past it without ever seeing
                    # count == rate_limit + 1) -- attach it now. Checking
                    # get_expire() first, instead of unconditionally calling
                    # expire() on every over-limit request, keeps the ban a
                    # fixed RATE_LIMIT_TTL window instead of one that resets
                    # on every retry from a client that keeps hammering it.
                    await self.cache.expire(rate_key, ttl_sec)
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
    except OSError, AttributeError:
        pass
    return None


_DEFAULT_PROXY_CACHE_INSTANCE: ProxyCache | None = None
_DEFAULT_PROXY_CACHE_LOCK = threading.Lock()


def get_default_proxy_cache() -> ProxyCache:
    """Lazily construct the shared default ProxyCache (no import-time env reads)."""
    global _DEFAULT_PROXY_CACHE_INSTANCE
    with _DEFAULT_PROXY_CACHE_LOCK:
        if _DEFAULT_PROXY_CACHE_INSTANCE is None:
            _DEFAULT_PROXY_CACHE_INSTANCE = ProxyCacheBuilder().build()
        assert _DEFAULT_PROXY_CACHE_INSTANCE is not None
        return _DEFAULT_PROXY_CACHE_INSTANCE


__all__ = [
    "BACKEND_EXCEPTIONS",
    "ConnectionRecord",
    "ConnectionResultBuilder",
    "LifecycleState",
    "ProxyCache",
    "ProxyCacheBuilder",
    "ProxyCacheProtocol",
    "get_client_key",
    "get_default_proxy_cache",
    "normalize_failure_policy",
    "validate_namespace",
]
