from functools import partial
from typing import Any, Self

import pytest
import trio
import trio.testing as tt
from cashews.ttl import ttl_to_seconds

from trio_http_proxy import http
from trio_http_proxy.cache import (
    DEFAULT_PROXY_CACHE,
    CircuitBreakerOpen,
    ProxyCache,
    RateLimitError,
    get_client_key,
)
from trio_http_proxy.proxy import http_proxy, tunnel


def connect_request(target: bytes = b"example.com:443", extra: bytes = b"") -> bytes:
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n" + extra


class MockSocket:
    def __init__(self, ip: str = "192.168.1.50") -> None:
        self._ip = ip

    def getpeername(self) -> tuple[str, int]:
        return (self._ip, 12345)


class StreamWithSocket:
    def __init__(self, inner: Any, ip: str = "192.168.1.50") -> None:
        self._inner: Any = inner
        self.socket = MockSocket(ip)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if hasattr(self._inner, "aclose"):
            await self._inner.aclose()


@tt.trio_test
async def test_proxy_cache_basic_get_set():
    async with ProxyCache(cache_url="mem://") as pc:
        key = f"{pc.namespace}:test-key"
        await pc.run(pc.cache.set, key, "test-val", expire="10s")
        val = await pc.run(pc.cache.get, key)
        assert val == "test-val"
        await pc.run(pc.cache.set, "unrelated:key", "keep-me")
        await pc.clear()
        val_after_clear = await pc.run(pc.cache.get, key)
        assert val_after_clear is None
        assert await pc.run(pc.cache.get, "unrelated:key") == "keep-me"


@tt.trio_test
async def test_proxy_cache_sync_context_manager():
    with ProxyCache(cache_url="mem://") as pc:
        await pc.run(pc.cache.set, "k", 123)
        res = await pc.run(pc.cache.get, "k")
        assert res == 123


@tt.trio_test
async def test_proxy_cache_circuit_breaker_trips():
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_errors_rate=50,
        circuit_breaker_period="10s",
        circuit_breaker_ttl="5s",
        circuit_breaker_min_calls=2,
    ) as pc:
        # Initially circuit should be closed (healthy)
        await pc.check_circuit("broken.host", 443)

        # Record 2 consecutive failures to cross min_calls & error_rate
        await pc.record_connection_result(
            "broken.host", 443, OSError("connection refused")
        )
        await pc.record_connection_result(
            "broken.host", 443, OSError("connection refused")
        )

        # 3rd check should now raise CircuitBreakerOpen
        with pytest.raises(CircuitBreakerOpen):
            await pc.check_circuit("broken.host", 443)

        # A different destination host should still be healthy
        await pc.check_circuit("other.host", 443)


@tt.trio_test
async def test_proxy_cache_rate_limiter():
    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=2,
        rate_limit_period="10s",
    ) as pc:
        await pc.check_rate_limit("client-1")
        await pc.check_rate_limit("client-1")

        # 3rd call exceeds limit of 2
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-1")

        # Another client key is unaffected
        await pc.check_rate_limit("client-2")


@tt.trio_test
async def test_proxy_cache_resolve_host_ip_literal():
    async with ProxyCache(cache_url="mem://", dns_cache_ttl="60s") as pc:
        # IPv4 literal bypasses DNS lookup
        ip4 = await pc.resolve_host("127.0.0.1")
        assert ip4 == "127.0.0.1"

        # IPv6 literal bypasses DNS lookup
        ip6 = await pc.resolve_host("[::1]")
        assert ip6 == "[::1]"


@tt.trio_test
async def test_proxy_cache_resolve_host_cached():
    async with ProxyCache(cache_url="mem://", dns_cache_ttl="60s") as pc:
        # Resolve localhost (resolves to 127.0.0.1 or ::1)
        res1 = await pc.resolve_host("localhost")
        assert res1 in ("127.0.0.1", "::1")

        # Second lookup should hit cache
        res2 = await pc.resolve_host("localhost")
        assert res1 == res2


@tt.trio_test
async def test_proxy_cache_disabled():
    pc = ProxyCache(enabled=False)
    # When disabled, resolve_host returns host unchanged
    assert await pc.resolve_host("unresolved.example.test") == "unresolved.example.test"
    # check_circuit and check_rate_limit do nothing
    await pc.check_circuit("any.host", 80)
    await pc.check_rate_limit("any_client")


@tt.trio_test
async def test_tunnel_with_circuit_breaker_fast_fail():
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_min_calls=1,
        circuit_breaker_errors_rate=1,
    ) as pc:
        proxy_client, test_client = tt.memory_stream_pair()

        # Manually trip circuit breaker for failfast.test:443
        await pc.record_connection_result(
            "failfast.test", 443, OSError("connection refused")
        )

        connect_called = False

        async def fake_open(host, port):
            nonlocal connect_called
            connect_called = True
            raise AssertionError("Should not connect when circuit is open")

        await test_client.send_all(connect_request(target=b"failfast.test:443"))

        with pytest.raises(CircuitBreakerOpen):
            await tunnel(proxy_client, _connect=fake_open, proxy_cache=pc)

        assert connect_called is False
        reply = await test_client.receive_some(4096)
        assert reply == http.http_502_bad_gateway


@tt.trio_test
async def test_http_proxy_with_rate_limit_exceeded():
    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=1,
        rate_limit_period="10s",
    ) as pc:
        # Pre-consume the client's rate limit quota
        await pc.check_rate_limit("192.168.1.50")

        proxy_client, test_client = tt.memory_stream_pair()
        stream_with_sock = StreamWithSocket(proxy_client, "192.168.1.50")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(partial(http_proxy, stream_with_sock, proxy_cache=pc))
            reply = await test_client.receive_some(4096)
            assert reply == http.http_429_too_many_requests % ttl_to_seconds(
                pc.rate_limit_period
            )
            remainder = await test_client.receive_some(4096)
            assert remainder == b""


def test_get_client_key():
    class DummyStreamWithSocket:
        class Socket:
            def getpeername(self):
                return ("10.0.0.1", 54321)

        socket = Socket()

    assert get_client_key(DummyStreamWithSocket()) == "10.0.0.1"
    assert get_client_key(object()) is None


def test_default_proxy_cache_singleton():
    assert isinstance(DEFAULT_PROXY_CACHE, ProxyCache)
    assert DEFAULT_PROXY_CACHE.cache is not None
