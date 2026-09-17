"""ProxyCache implementation bridging cashews, circuit breaking, and rate limiting."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import math
import queue
import socket
import threading
import time
from collections.abc import Callable, Coroutine
from typing import Any, Self, TypeVar, cast

import cashews
import trio
from cashews import Cache, CircuitBreakerOpen

from .cache.lifecycle_state import LifecycleState
from .circuit_breaker import (
    CircuitBreakerManager,
    CircuitPermit,
)
from .errors import CacheBackendUnavailable, RateLimitError
from .util import redact_url

logger = logging.getLogger(__name__)

T = TypeVar("T")

from .config import (
    NAMESPACE_PATTERN,
    CacheSettings,
    Settings,
    get_settings,
    normalize_failure_policy,
    parse_duration_seconds,
    validate_namespace,
)


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
        cache_url: str | None = None,
        enabled: bool | None = None,
        namespace: str | None = None,
        dns_cache_ttl: str | None = None,
        circuit_breaker_enabled: bool | None = None,
        circuit_breaker_errors_rate: int | None = None,
        circuit_breaker_period: str | None = None,
        circuit_breaker_ttl: str | None = None,
        circuit_breaker_min_calls: int | None = None,
        circuit_breaker_max_destinations: int | None = None,
        rate_limit_enabled: bool | None = None,
        rate_limit: int | None = None,
        rate_limit_period: str | None = None,
        rate_limit_ttl: str | None = None,
        failure_policy: str | None = None,
        cache_failure_policy: str | None = None,
        startup_timeout: float | None = None,
        settings: Settings | CacheSettings | None = None,
    ) -> None:
        _explicit_dns_cache_ttl = dns_cache_ttl is not None
        base = settings if isinstance(settings, Settings) else get_settings()
        cache_group = settings if isinstance(settings, CacheSettings) else base.cache
        cb_group = base.circuit_breaker
        rl_group = base.rate_limit

        cache_url = cache_url if cache_url is not None else cache_group.cache_url
        enabled = enabled if enabled is not None else cache_group.cache_enabled
        namespace = namespace if namespace is not None else cache_group.cache_namespace
        dns_cache_ttl = (
            dns_cache_ttl if dns_cache_ttl is not None else cache_group.dns_cache_ttl
        )
        circuit_breaker_enabled = (
            circuit_breaker_enabled
            if circuit_breaker_enabled is not None
            else cb_group.circuit_breaker_enabled
        )
        circuit_breaker_errors_rate = (
            circuit_breaker_errors_rate
            if circuit_breaker_errors_rate is not None
            else cb_group.circuit_breaker_errors_rate
        )
        circuit_breaker_period = (
            circuit_breaker_period
            if circuit_breaker_period is not None
            else cb_group.circuit_breaker_period
        )
        circuit_breaker_ttl = (
            circuit_breaker_ttl
            if circuit_breaker_ttl is not None
            else cb_group.circuit_breaker_ttl
        )
        circuit_breaker_min_calls = (
            circuit_breaker_min_calls
            if circuit_breaker_min_calls is not None
            else cb_group.circuit_breaker_min_calls
        )
        circuit_breaker_max_destinations = (
            circuit_breaker_max_destinations
            if circuit_breaker_max_destinations is not None
            else cb_group.circuit_breaker_max_destinations
        )
        rate_limit_enabled = (
            rate_limit_enabled
            if rate_limit_enabled is not None
            else rl_group.rate_limit_enabled
        )
        rate_limit = rate_limit if rate_limit is not None else rl_group.rate_limit
        rate_limit_period = (
            rate_limit_period
            if rate_limit_period is not None
            else rl_group.rate_limit_period
        )
        rate_limit_ttl = (
            rate_limit_ttl if rate_limit_ttl is not None else rl_group.rate_limit_ttl
        )
        if cache_failure_policy is not None:
            failure_policy = cache_failure_policy
        if failure_policy is None:
            failure_policy = cache_group.cache_failure_policy
        startup_timeout = (
            startup_timeout
            if startup_timeout is not None
            else cache_group.startup_timeout
        )

        self.settings: Settings | CacheSettings | None = settings
        # Semantic boundary validation in constructor
        _stripped_namespace = validate_namespace(namespace)
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
        self.namespace = _stripped_namespace
        self._explicit_dns_cache_ttl = _explicit_dns_cache_ttl
        self.dns_cache_ttl: str = dns_cache_ttl
        self.circuit_breaker_enabled = circuit_breaker_enabled
        self.circuit_breaker_errors_rate = circuit_breaker_errors_rate
        self.circuit_breaker_period: str = circuit_breaker_period
        self.circuit_breaker_ttl: str = circuit_breaker_ttl
        self.circuit_breaker_min_calls = circuit_breaker_min_calls
        self.circuit_breaker_max_destinations = circuit_breaker_max_destinations
        self.rate_limit_enabled = rate_limit_enabled
        self.rate_limit = rate_limit
        self.rate_limit_period: str = rate_limit_period
        _rl_ttl = normalize_optional_duration_local(rate_limit_ttl)
        self.rate_limit_ttl: str | None = (
            _rl_ttl if _rl_ttl is not None else self.rate_limit_period
        )
        self.rate_limit_period_sec = parse_duration_seconds(
            self.rate_limit_period, field="rate_limit_period"
        )
        self.rate_limit_ttl_sec = parse_duration_seconds(
            self.rate_limit_ttl, field="rate_limit_ttl"
        )
        self.dns_cache_ttl_sec = parse_duration_seconds(
            self.dns_cache_ttl, field="dns_cache_ttl", allow_zero=True
        )
        self.failure_policy = normalize_failure_policy(failure_policy)
        self.cache_failure_policy = self.failure_policy
        self.probe_interval: float = cache_group.probe_interval
        self.startup_timeout: float = startup_timeout
        if cache_failure_policy is not None:
            failure_policy = cache_failure_policy
        if failure_policy is None:
            failure_policy = cache_group.cache_failure_policy
        startup_timeout = (
            startup_timeout
            if startup_timeout is not None
            else cache_group.startup_timeout
        )

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

        self._circuit_manager = CircuitBreakerManager(
            errors_rate=self.circuit_breaker_errors_rate,
            period=parse_duration_seconds(
                self.circuit_breaker_period, field="circuit_breaker_period"
            ),
            ttl=parse_duration_seconds(
                self.circuit_breaker_ttl, field="circuit_breaker_ttl"
            ),
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
            _DEFAULT_PROXY_CACHE_INSTANCE = ProxyCache()
        return _DEFAULT_PROXY_CACHE_INSTANCE


__all__ = [
    "BACKEND_EXCEPTIONS",
    "NAMESPACE_PATTERN",
    "LifecycleState",
    "ProxyCache",
    "get_client_key",
    "get_default_proxy_cache",
    "normalize_failure_policy",
    "normalize_optional_duration_local",
    "validate_namespace",
]
