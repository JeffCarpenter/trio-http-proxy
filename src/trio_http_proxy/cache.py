import asyncio
import ipaddress
import logging
import socket
import threading
from collections.abc import Callable, Coroutine
from os import getenv
from typing import Any, Self, TypeVar

import trio
from cashews import Cache, CircuitBreakerOpen, RateLimitError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _int_env(
    name: str, default: int, _getenv: Callable[..., str | None] = getenv
) -> int:
    val = _getenv(name)
    if val is None or not val.strip():
        return default
    try:
        return int(val)
    except ValueError:
        return default


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
DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE = _int_env("CIRCUIT_BREAKER_ERRORS_RATE", 50)
DEFAULT_CIRCUIT_BREAKER_PERIOD = getenv("CIRCUIT_BREAKER_PERIOD", "60s")
DEFAULT_CIRCUIT_BREAKER_TTL = getenv("CIRCUIT_BREAKER_TTL", "30s")
DEFAULT_CIRCUIT_BREAKER_MIN_CALLS = _int_env("CIRCUIT_BREAKER_MIN_CALLS", 3)
DEFAULT_RATE_LIMIT_ENABLED = _bool_env("RATE_LIMIT_ENABLED", False)
DEFAULT_RATE_LIMIT = _int_env("RATE_LIMIT", 0)
DEFAULT_RATE_LIMIT_PERIOD = getenv("RATE_LIMIT_PERIOD", "60s")
DEFAULT_RATE_LIMIT_TTL = getenv("RATE_LIMIT_TTL", "60s")


class ProxyCache:
    """Bridges cashews caching, circuit breaking, and rate limiting to Trio.

    Because cashews is hardcoded to standard asyncio primitives
    (asyncio.create_task, asyncio.sleep, asyncio.Lock), it cannot run directly
    on Trio's event loop. ProxyCache manages an asyncio event loop running on a
    dedicated background thread and coordinates operations safely via
    trio.to_thread.run_sync and asyncio.run_coroutine_threadsafe.
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

        self.cache = Cache(name="trio_http_proxy")
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._lock = threading.Lock()
        self._started = False

        self._resolve_dns_fn: Callable[[str], Coroutine[Any, Any, str]] | None = None
        self._circuit_probe_fn: (
            Callable[[str, int, Exception | None], Coroutine[Any, Any, bool]] | None
        ) = None
        self._rate_limiter_fn: Callable[[str], Coroutine[Any, Any, bool]] | None = None

    def start(self) -> None:
        """Start the background asyncio bridge thread and configure cashews."""
        with self._lock:
            if self._started:
                return
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
                self.cache.setup(self.cache_url)
                self._register_decorators()
            except Exception:
                logger.exception(
                    "Failed to setup cashews cache with %s", self.cache_url
                )
                self._teardown_thread()
                raise

            self._started = True

    def stop(self) -> None:
        """Gracefully stop cashews and the background asyncio bridge thread."""
        with self._lock:
            if not self._started:
                return
            self._teardown_thread()
            self._started = False

    def _teardown_thread(self) -> None:
        if self._loop and self._loop.is_running():
            try:
                fut = asyncio.run_coroutine_threadsafe(self.cache.close(), self._loop)
                fut.result(timeout=2.0)
            except Exception:
                logger.debug("Exception closing cashews cache", exc_info=True)
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread:
                self._thread.join(timeout=2.0)
        self._loop = None
        self._thread = None
        self._resolve_dns_fn = None
        self._circuit_probe_fn = None
        self._rate_limiter_fn = None

    def _is_mock_clock(self) -> bool:
        try:
            clock = trio.lowlevel.current_clock()
            return getattr(clock, "autojump_threshold", None) is not None
        except RuntimeError:
            return False

    def _ensure_started(self) -> None:
        if not self._started:
            self.start()

    def run_coroutine(self, coro: Coroutine[Any, Any, T], timeout: float = 10.0) -> T:
        """Execute a coroutine in the background asyncio event loop synchronously."""
        self._ensure_started()
        if not self._loop or not self._loop.is_running():
            raise RuntimeError("Asyncio bridge event loop is not running")
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    async def run(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        timeout: float = 10.0,
        **kwargs: Any,
    ) -> T:
        """Execute an asyncio coroutine function on the bridge from within Trio."""
        self._ensure_started()
        return await trio.to_thread.run_sync(
            lambda: self.run_coroutine(coro_fn(*args, **kwargs), timeout=timeout)
        )

    def _register_decorators(self) -> None:
        c = self.cache

        # 1. DNS Resolution Caching
        if self.dns_cache_ttl and self.dns_cache_ttl not in ("0", "0s"):

            @c(ttl=self.dns_cache_ttl, key="dns:{host}")
            async def _resolve_dns(host: str) -> str:
                assert self._loop is not None
                addr_infos = await self._loop.run_in_executor(
                    None,
                    socket.getaddrinfo,
                    host,
                    None,
                    socket.AF_UNSPEC,
                    socket.SOCK_STREAM,
                )
                if not addr_infos:
                    raise OSError(f"Name resolution failed for {host}")
                ip: str = str(addr_infos[0][4][0])
                return ip

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

    async def resolve_host(self, host: str) -> str:
        """Resolve a destination host to an IP, with cashews caching.

        If the host is already an IP address or caching is disabled, returns host
        unchanged.
        """
        self._ensure_started()
        if not self.enabled or self._resolve_dns_fn is None or self._is_mock_clock():
            return host

        # Check if already an IP literal
        try:
            ipaddress.ip_address(host.strip("[]"))
            return host
        except ValueError:
            pass

        return await self.run(self._resolve_dns_fn, host)

    async def check_circuit(self, host: str, port: int) -> None:
        """Verify the circuit for (host, port) is not open.

        Raises CircuitBreakerOpen if the destination has failed repeatedly.
        """
        self._ensure_started()
        if not self.enabled or self._circuit_probe_fn is None or self._is_mock_clock():
            return
        await self.run(self._circuit_probe_fn, host, port, None)

    async def record_connection_result(
        self, host: str, port: int, exc: Exception | None = None
    ) -> None:
        """Record success or failure of a connection attempt in the circuit breaker."""
        self._ensure_started()
        if not self.enabled or self._circuit_probe_fn is None or self._is_mock_clock():
            return
        try:
            await self.run(self._circuit_probe_fn, host, port, exc)
        except CircuitBreakerOpen, OSError, RuntimeError:
            pass

    async def check_rate_limit(self, key: str) -> None:
        """Verify the given key has not exceeded the configured rate limit.

        Raises RateLimitError if exceeded.
        """
        self._ensure_started()
        if not self.enabled or self._rate_limiter_fn is None or self._is_mock_clock():
            return
        await self.run(self._rate_limiter_fn, key)

    async def clear(self) -> None:
        """Clear all entries in the cache."""
        self._ensure_started()
        if self.enabled:
            await self.run(self.cache.clear)

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
    """Extract a rate-limiting key (typically client IP) from a stream."""
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
    "DEFAULT_CACHE_ENABLED",
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
    "CircuitBreakerOpen",
    "ProxyCache",
    "RateLimitError",
    "get_client_key",
]
