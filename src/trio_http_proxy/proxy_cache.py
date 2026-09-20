"""ProxyCache implementation bridging cashews, circuit breaking, and rate limiting."""

from __future__ import annotations

import asyncio
import enum
import ipaddress
import logging
import math
import queue
import socket
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Protocol, Self, TypeVar, cast, runtime_checkable

import cashews
import trio
from cashews import Cache, CircuitBreakerOpen

from .circuit_breaker import (
    CircuitBreakerManager,
    CircuitPermit,
)
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
    NAMESPACE_PATTERN,
    Settings,
    normalize_failure_policy,
    parse_duration_seconds,
    validate_namespace,
)
from .config.builder import ProxyCacheBuilder


def normalize_optional_duration_local(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped or stripped == "0s":
        return None
    parse_duration_seconds(stripped, field="rate_limit_ttl")
    return stripped


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

    @property
    def exc(self) -> Exception | None:
        """Backward-compatible alias for error."""
        return self.error

    @property
    def is_success(self) -> bool:
        """Return True if connection succeeded without error."""
        return self.error is None

    @property
    def has_permit(self) -> bool:
        """Return True if a circuit breaker permit is associated."""
        return self.permit is not None


class ConnectionResultBuilder:
    """Fluent builder for constructing ConnectionRecord instances."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        error: Exception | None = None,
        permit: CircuitPermit | None = None,
        *,
        exc: Exception | None = None,
    ) -> None:
        self._host: str | None = host
        self._port: int | None = port
        self._error: Exception | None = exc if exc is not None else error
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

    def with_exc(self, exc: Exception | None) -> Self:
        self._error = exc
        return self

    def with_success(self) -> Self:
        self._error = None
        return self

    def with_permit(self, permit: CircuitPermit | None) -> Self:
        self._permit = permit
        return self

    def build(self) -> ConnectionRecord:
        if self._host is None:
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
        if proxy_cache is None:
            return
        record = self.build()
        if hasattr(proxy_cache, "record_connection_result"):
            try:
                await proxy_cache.record_connection_result(record)
            except TypeError as err:
                if err.__traceback__ is not None and err.__traceback__.tb_next is None:
                    await proxy_cache.record_connection_result(
                        record.host, record.port, record.error
                    )
                else:
                    raise


@runtime_checkable
class ProxyCacheProtocol(Protocol):
    """Protocol defining the cache and resilience contract required by the HTTP proxy."""

    async def record_connection_result(
        self,
        result: ConnectionRecord | ConnectionResultBuilder | str,
        port: int | None = None,
        exc: Exception | None = None,
        permit: CircuitPermit | None = None,
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

    @classmethod
    def builder(
        cls, settings: Settings | None = None, **kwargs: Any
    ) -> ProxyCacheBuilder:
        """Return a fluent builder for configuring and constructing a ProxyCache."""
        return ProxyCacheBuilder(settings=settings, **kwargs)

    def __init__(
        self,
        settings: Settings | ProxyCacheBuilder | str | None = None,
        *,
        builder: ProxyCacheBuilder | None = None,
        **kwargs: Any,
    ) -> None:
        if "url" in kwargs and "cache_url" not in kwargs:
            kwargs["cache_url"] = kwargs.pop("url")

        if isinstance(settings, str):
            kwargs["cache_url"] = settings
            settings = None

        if isinstance(settings, ProxyCacheBuilder):
            builder = settings
            settings = None

        if builder is not None:
            if settings is not None or kwargs:
                raise ValueError(
                    "Cannot specify both builder and settings/kwargs to ProxyCache"
                )
            resolved_settings = builder.build_settings()
        elif settings is not None:
            if kwargs:
                raise ValueError(
                    "Cannot pass both settings and keyword arguments to ProxyCache"
                )
            resolved_settings = settings
        elif kwargs:
            resolved_settings = ProxyCacheBuilder(**kwargs).build_settings()
        else:
            resolved_settings = ProxyCacheBuilder().build_settings()

        self.settings: Settings = resolved_settings
        s = resolved_settings

        # Semantic boundary validation in constructor
        _stripped_namespace = validate_namespace(s.cache_namespace)
        if not 1 <= s.circuit_breaker_errors_rate <= 99:
            raise ValueError(
                f"circuit_breaker_errors_rate must be between 1 and 99, got {s.circuit_breaker_errors_rate}"
            )
        if s.circuit_breaker_min_calls < 1:
            raise ValueError(
                f"circuit_breaker_min_calls must be >= 1, got {s.circuit_breaker_min_calls}"
            )
        if s.rate_limit < 0:
            raise ValueError(f"rate_limit must be >= 0, got {s.rate_limit}")
        if s.startup_timeout <= 0:
            raise ValueError(f"startup_timeout must be > 0, got {s.startup_timeout}")

        self.cache_url: str = s.cache_url
        self.enabled: bool = s.cache_enabled
        self.namespace: str = _stripped_namespace
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
        _rl_ttl = normalize_optional_duration_local(s.rate_limit_ttl)
        self.rate_limit_ttl: str | None = (
            _rl_ttl if _rl_ttl is not None else self.rate_limit_period
        )
        self.rate_limit_period_sec: float = s.rate_limit_period_seconds
        if _rl_ttl is not None and s.rate_limit_ttl_seconds is not None:
            self.rate_limit_ttl_sec: float = s.rate_limit_ttl_seconds
        else:
            self.rate_limit_ttl_sec: float = s.rate_limit_period_seconds
        self.dns_cache_ttl_sec: float = s.dns_cache_ttl_seconds
        self.failure_policy: str = s.cache_failure_policy
        self.cache_failure_policy: str = self.failure_policy
        self.probe_interval: float = s.probe_interval
        self.startup_timeout: float = s.startup_timeout

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
        stop_parts: (
            tuple[asyncio.AbstractEventLoop | None, threading.Thread | None] | None
        ) = None
        with self._lock:
            if self._lifecycle_state == LifecycleState.STOPPED:
                raise RuntimeError("ProxyCache cannot be restarted once stopped")
            if self._lifecycle_state in (
                LifecycleState.STARTING,
                LifecycleState.RUNNING,
            ):
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
            # Only advance to STOPPED once the thread/loop are actually gone;
            # otherwise leave the lifecycle in STOPPING so a subsequent stop()
            # call retries cleanup instead of silently no-opping on a leaked
            # thread/loop (stop() short-circuits on LifecycleState.STOPPED).
            if not alive and not open_loop:
                self._lifecycle_state = LifecycleState.STOPPED
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
            if self._lifecycle_state in (
                LifecycleState.RUNNING,
                LifecycleState.STARTING,
            ):
                return
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
        except trio.Cancelled, trio.TooSlowError:
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
            if now - self._last_probe_time < self.probe_interval:
                return
            self._last_probe_time = now

        try:
            await self.run(self.cache.ping, timeout=self.startup_timeout)
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

    @classmethod
    def result_builder(
        cls,
        host: str | None = None,
        port: int | None = None,
    ) -> ConnectionResultBuilder:
        """Return a new ConnectionResultBuilder initialized with optional target."""
        return ConnectionResultBuilder(host=host, port=port)

    async def record_connection_result(
        self,
        result: ConnectionRecord | ConnectionResultBuilder | str,
        port: int | None = None,
        exc: Exception | None = None,
        permit: CircuitPermit | None = None,
    ) -> None:
        """Record a connection result in the Trio-native circuit breaker."""
        self._ensure_started()
        if not self.circuit_breaker_enabled:
            return
        if isinstance(result, ConnectionRecord):
            record = result
        elif isinstance(result, ConnectionResultBuilder):
            record = result.build()
        else:
            record = (
                ConnectionResultBuilder()
                .with_host(result)
                .with_port(port)  # type: ignore[arg-type]
                .with_error(exc)
                .with_permit(permit)
                .build()
            )
        self._circuit_manager.record_result(
            record.host, record.port, record.error, permit=record.permit
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
        parsed_period = parse_duration_seconds(
            self.rate_limit_period, field="rate_limit_period"
        )
        period_sec = int(parsed_period)
        parsed_ttl = (
            parse_duration_seconds(self.rate_limit_ttl, field="rate_limit_ttl")
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
    "NAMESPACE_PATTERN",
    "ConnectionRecord",
    "ConnectionResultBuilder",
    "LifecycleState",
    "ProxyCache",
    "ProxyCacheBuilder",
    "ProxyCacheProtocol",
    "get_client_key",
    "get_default_proxy_cache",
    "normalize_failure_policy",
    "normalize_optional_duration_local",
    "validate_namespace",
]
