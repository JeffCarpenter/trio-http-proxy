import asyncio
import ipaddress
import logging
import math
import socket
import threading
from collections.abc import Callable, Coroutine
from os import getenv
from typing import Any, Self, TypeVar, cast

import cashews
import trio
from cashews import Cache, CircuitBreakerOpen

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
    return val.strip().lower() in ("1", "true", "yes", "on")


DEFAULT_CACHE_URL = getenv("CACHE_URL", "mem://")
DEFAULT_CACHE_ENABLED = _bool_env("CACHE_ENABLED", True)
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
DEFAULT_RATE_LIMIT_TTL = getenv("RATE_LIMIT_TTL", "60s")


def _failure_policy(value: str) -> str:
    value = value.strip().lower()
    if value not in {"fail-open", "fail-closed"}:
        raise ValueError(
            "CACHE_FAILURE_POLICY must be either 'fail-open' or 'fail-closed'"
        )
    return value


CACHE_FAILURE_POLICY = _failure_policy(getenv("CACHE_FAILURE_POLICY", "fail-open"))
DEFAULT_CACHE_FAILURE_POLICY = CACHE_FAILURE_POLICY


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
    def retry_after_seconds(self) -> int | None:
        return self.retry_after


class ProxyCache:
    """Bridges cashews caching, circuit breaking, and rate limiting to Trio.

    Because cashews is hardcoded to standard asyncio primitives
    (asyncio.create_task, asyncio.sleep, asyncio.Lock), it cannot run directly
    on Trio's event loop. ProxyCache manages an asyncio event loop running on a
    dedicated background thread and coordinates operations safely via
    cross-thread completion signaling with trio.from_thread.run_sync and
    asyncio.run_coroutine_threadsafe.
    """

    def __init__(
        self,
        cache_url: str = DEFAULT_CACHE_URL,
        enabled: bool = DEFAULT_CACHE_ENABLED,
        dns_cache_ttl: str = DEFAULT_DNS_CACHE_TTL,
        circuit_breaker_enabled: bool = DEFAULT_CIRCUIT_BREAKER_ENABLED,
        circuit_breaker_errors_rate: int = DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE,
        circuit_breaker_period: str = DEFAULT_CIRCUIT_BREAKER_PERIOD,
        circuit_breaker_ttl: str = DEFAULT_CIRCUIT_BREAKER_TTL,
        circuit_breaker_min_calls: int = DEFAULT_CIRCUIT_BREAKER_MIN_CALLS,
        rate_limit_enabled: bool = DEFAULT_RATE_LIMIT_ENABLED,
        rate_limit: int = DEFAULT_RATE_LIMIT,
        rate_limit_period: str = DEFAULT_RATE_LIMIT_PERIOD,
        rate_limit_ttl: str = DEFAULT_RATE_LIMIT_TTL,
        failure_policy: str = CACHE_FAILURE_POLICY,
        cache_failure_policy: str | None = None,
    ) -> None:
        self.cache_url = cache_url
        self.enabled = enabled
        self.dns_cache_ttl = dns_cache_ttl
        self.circuit_breaker_enabled = circuit_breaker_enabled
        self.circuit_breaker_errors_rate = circuit_breaker_errors_rate
        self.circuit_breaker_period = circuit_breaker_period
        self.circuit_breaker_ttl = circuit_breaker_ttl
        self.circuit_breaker_min_calls = circuit_breaker_min_calls
        self.rate_limit_enabled = rate_limit_enabled
        self.rate_limit = rate_limit
        self.rate_limit_period = rate_limit_period
        self.rate_limit_ttl = rate_limit_ttl
        if cache_failure_policy is not None:
            failure_policy = cache_failure_policy
        self.failure_policy = _failure_policy(failure_policy)
        self.cache_failure_policy = self.failure_policy

        self.cache = Cache(name="trio_http_proxy")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._started = False
        self._stopped = False
        self._stopping = False
        self._stop_complete = threading.Event()

        self._resolve_dns_fn: Callable[[str], Coroutine[Any, Any, list[str]]] | None = (
            None
        )
        self._circuit_probe_fn: (
            Callable[[str, int, Exception | None], Coroutine[Any, Any, bool]] | None
        ) = None
        self._rate_limiter_fn: Callable[[str], Coroutine[Any, Any, bool]] | None = None

    def start(self) -> None:
        """Start the background asyncio bridge thread and configure cashews."""
        setup_error: Exception | None = None
        stop_parts: (
            tuple[asyncio.AbstractEventLoop | None, threading.Thread | None] | None
        ) = None
        with self._lock:
            if self._started:
                return
            self._stopped = False
            self._stopping = False
            if not self.enabled or not self.cache_url:
                self._started = True
                return

            self._ready.clear()

            def _run_loop() -> None:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self._loop = loop
                self._ready.set()
                try:
                    loop.run_forever()
                finally:
                    loop.close()

            self._thread = threading.Thread(
                target=_run_loop, daemon=True, name="cashews-trio-bridge"
            )
            self._thread.start()
            self._ready.wait()

            try:
                # setup is deliberately performed here: tests and callers can
                # distinguish backend setup failure from lazy backend failures.
                self.cache.setup(self.cache_url)
                self._register_decorators()
            except Exception as exc:
                setup_error = exc
                logger.exception(
                    "Failed to setup cashews cache with %s", self.cache_url
                )
                self._stopped = True
                self._started = False
                stop_parts = self._begin_stop_locked()
            else:
                self._started = True

        if setup_error is not None:
            assert stop_parts is not None
            self._finish_stop(*stop_parts)
            if self.cache_failure_policy == "fail-open":
                self._disable_cache_features()
                with self._lock:
                    self._stopped = False
                    self._started = True
                return
            raise CacheBackendUnavailable(
                f"Unable to setup cache backend {self.cache_url!r}"
            ) from setup_error

    def stop(self) -> None:
        """Stop the bridge, draining pending work before closing its loop."""
        with self._lock:
            if self._stopping:
                done = self._stop_complete
                parts = None
            elif not self._started and self._loop is None and self._thread is None:
                self._stopped = True
                return
            else:
                self._stopped = True
                self._started = False
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
                    await self.cache.close()
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
            if not alive and not open_loop:
                self._stop_complete.set()
                if self._thread is thread:
                    self._thread = None
                if self._loop is loop:
                    self._loop = None
                self._resolve_dns_fn = None
                self._circuit_probe_fn = None
                self._rate_limiter_fn = None
        if alive:
            # Keep references intact so a later stop can retry; daemon status
            # does not prove that the bridge terminated.
            raise RuntimeError("Timed out stopping ProxyCache bridge")
        if open_loop:
            raise RuntimeError("ProxyCache bridge loop did not close")

    def _disable_cache_features(self) -> None:
        self.enabled = False
        self._resolve_dns_fn = None
        self._circuit_probe_fn = None
        self._rate_limiter_fn = None

    def _ensure_started(self) -> None:
        if self._stopped:
            raise RuntimeError("ProxyCache has been stopped")
        if not self._started:
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
        """Execute an asyncio coroutine function on the bridge from within Trio."""
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

        # 1. DNS Resolution Caching
        if self.dns_cache_ttl and self.dns_cache_ttl not in ("0", "0s"):

            @c(ttl=self.dns_cache_ttl, key="dns:{host}")
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
                    # Keep destination lookup failures distinguishable from an
                    # OSError raised by a cache backend.
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

        # 2. Destination Circuit Breaker
        if self.circuit_breaker_enabled:

            @c.circuit_breaker(
                key="cb:{host}:{port}",
                errors_rate=self.circuit_breaker_errors_rate,
                period=self.circuit_breaker_period,
                ttl=self.circuit_breaker_ttl,
                min_calls=self.circuit_breaker_min_calls,
            )
            async def _circuit_probe(
                host: str, port: int, exc: Exception | None = None
            ) -> bool:
                if exc is not None:
                    raise exc
                return True

            self._circuit_probe_fn = _circuit_probe
        else:
            self._circuit_probe_fn = None

        # 3. Rate Limiter
        if self.rate_limit_enabled and self.rate_limit > 0:

            @c.rate_limit(
                limit=self.rate_limit,
                period=self.rate_limit_period,
                ttl=self.rate_limit_ttl,
                key="rl:{key}",
            )
            async def _rate_limiter(key: str) -> bool:
                return True

            self._rate_limiter_fn = _rate_limiter
        else:
            self._rate_limiter_fn = None

    def _backend_error(self, operation: str, exc: Exception) -> CacheBackendUnavailable:
        return CacheBackendUnavailable(
            f"Cache backend failed during {operation}: {exc}"
        )

    def _handle_backend_failure(self, operation: str, exc: Exception) -> None:
        if self._stopped:
            raise exc
        if self.cache_failure_policy == "fail-open":
            self._disable_cache_features()
            return
        raise self._backend_error(operation, exc) from exc

    async def resolve_hosts(self, host: str) -> list[str]:
        """Resolve a destination host, bypassing cache on fail-open outages."""
        self._ensure_started()
        if not self.enabled or self._resolve_dns_fn is None:
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

    async def _is_circuit_open(self, host: str, port: int) -> bool:
        cache_key = f"circuit_breaker:cb:{host}:{port}"
        return bool(
            await self.cache.is_locked(f"{cache_key}:open")
            or await self.cache.exists(f"{cache_key}:halfopen")
        )

    async def check_circuit(self, host: str, port: int) -> None:
        """Verify that the destination circuit is not open."""
        self._ensure_started()
        if not self.enabled or not self.circuit_breaker_enabled:
            return
        try:
            is_open = await self.run(self._is_circuit_open, host, port)
        except Exception as exc:  # noqa: BLE001
            self._handle_backend_failure("circuit breaker", exc)
            return
        if is_open:
            raise CircuitBreakerOpen()

    async def record_connection_result(
        self, host: str, port: int, exc: Exception | None = None
    ) -> None:
        """Record a connection result without masking destination errors."""
        self._ensure_started()
        if not self.enabled or self._circuit_probe_fn is None:
            return
        try:
            await self.run(self._circuit_probe_fn, host, port, exc)
        except CircuitBreakerOpen:
            pass
        except Exception as backend_exc:  # noqa: BLE001
            # The exception supplied by the caller is the expected circuit
            # recording result, not a cache backend outage.
            if exc is not None and backend_exc is exc:
                return
            self._handle_backend_failure("circuit breaker", backend_exc)

    async def check_rate_limit(self, key: str) -> None:
        """Verify a key's quota and attach its exact backend expiry on rejection."""
        self._ensure_started()
        if not self.enabled or self._rate_limiter_fn is None:
            return
        try:
            await self.run(self._rate_limiter_fn, key)
        except RateLimitError:
            raise
        except cashews.RateLimitError as exc:
            expiry_key = f"rate_limit:rl:{key}"
            try:
                expiry = await self.run(self.cache.get_expire, expiry_key)
            except Exception as backend_exc:  # noqa: BLE001
                self._handle_backend_failure("rate-limit expiry", backend_exc)
                return
            try:
                retry_after = max(1, math.ceil(float(expiry)))
            except TypeError, ValueError:
                retry_after = 1
            raise RateLimitError(retry_after=retry_after) from exc
        except Exception as exc:  # noqa: BLE001
            self._handle_backend_failure("rate limiting", exc)

    async def clear(self) -> None:
        """Clear all entries in the cache."""
        self._ensure_started()
        if not self.enabled:
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
        self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        self.stop()


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


DEFAULT_PROXY_CACHE = ProxyCache()

__all__ = [
    "CACHE_FAILURE_POLICY",
    "DEFAULT_CACHE_ENABLED",
    "DEFAULT_CACHE_FAILURE_POLICY",
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
    "ProxyCache",
    "RateLimitError",
    "get_client_key",
]
