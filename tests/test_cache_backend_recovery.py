import inspect
import socket
import time
from typing import Any
from unittest.mock import AsyncMock

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.cache import ProxyCache
from trio_http_proxy.errors import RateLimitError


class MockClock:
    """Deterministic monotonic clock fixture for controlling probe intervals."""

    def __init__(self, initial: float = 1000.0) -> None:
        self.current = initial

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


async def _trigger_backend_probe(pc: ProxyCache) -> None:
    """Trigger the backend health probe via explicit method or event loop yield."""
    probe_fn = getattr(pc, "_maybe_probe_backend", None)
    if probe_fn is not None:
        res = probe_fn()
        if inspect.isawaitable(res):
            await res
    with trio.move_on_after(0.05):
        await trio.sleep(0.01)


@tt.trio_test
async def test_sticky_degradation_recovers_after_temporary_backend_blip(monkeypatch):
    """Sticky degradation (§3.2 in findings.md) causes rate limiting and caching to stay

    permanently bypassed after a temporary backend blip until restart.

    Contract & Acceptance Criterion:
    - In fail-open mode, degraded backend bypasses operations to protect traffic.
    - Periodic probe checks backend health via cache.cache.ping().
    - While backend remains down, is_degraded stays True and is_running stays False.
    - When backend recovers (ping succeeds), probe detects health:
      - is_degraded resets to False
      - is_running transitions back to True
      - Subsequent backend operations immediately resume enforcing quota limits.
    """
    clock = MockClock(1000.0)
    monkeypatch.setattr(time, "monotonic", clock)

    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        # Step 1: Verify initial healthy state and quota enforcement
        assert pc.is_running is True
        assert pc.is_degraded is False

        # First request allowed (1 <= 1)
        await pc.check_rate_limit("client-a")

        # Second request rejected (quota exceeded)
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-a")

        # Step 2: Backend enters degraded mode (e.g. transient blip / failure)
        pc._degraded = True
        assert pc.is_degraded is True
        assert pc.is_running is False

        # In degraded mode, backend operations bypass backend so traffic is not disrupted
        await pc.check_rate_limit("client-a")  # Does not raise RateLimitError

        # Step 3: While backend remains down, probe detects failure and keeps degraded
        async def failing_ping() -> Any:
            raise ConnectionError("Backend unavailable")

        monkeypatch.setattr(pc.cache, "ping", failing_ping)

        # Advance clock past probe interval (30s per §3.2)
        clock.advance(35.0)
        await _trigger_backend_probe(pc)

        assert pc.is_degraded is True
        assert pc.is_running is False
        # Traffic continues to bypass without disruption
        await pc.check_rate_limit("client-a")

        # Step 4: Backend recovers (simulated by ping returning successfully)
        async def healthy_ping() -> bytes:
            return b"PONG"

        monkeypatch.setattr(pc.cache, "ping", healthy_ping)

        # Advance clock past next probe interval
        clock.advance(35.0)
        await _trigger_backend_probe(pc)

        # Acceptance Criterion: Degraded backend recovers upon successful ping
        assert pc.is_degraded is False
        assert pc.is_running is True

        # Subsequent backend operations immediately resume using backend and enforcing limits
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-a")


@tt.trio_test
async def test_runtime_backend_failure_fail_open_degrades_and_recovers_on_ping(
    monkeypatch,
):
    """Runtime backend failure in fail-open mode must degrade cleanly and recover upon successful ping.

    Catches: Operational failure causing crash instead of graceful fail-open degradation,
    or failing to restore service after backend health is restored.
    """
    clock = MockClock(1000.0)
    monkeypatch.setattr(time, "monotonic", clock)

    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        # 1st call succeeds under healthy backend
        await pc.check_rate_limit("client-b")

        # Simulate runtime backend failure during operation
        original_incr = pc.cache.incr

        async def failing_incr(*args: Any, **kwargs: Any) -> Any:
            raise ConnectionError("Redis server connection dropped")

        monkeypatch.setattr(pc.cache, "incr", failing_incr)

        # Runtime failure under fail-open must not raise to the caller
        await pc.check_rate_limit("client-b")

        # Must have transitioned to degraded mode
        assert pc.is_degraded is True
        assert pc.is_running is False

        # In degraded mode, subsequent operations bypass backend
        await pc.check_rate_limit("client-b")

        # Restore backend operational method and configure successful ping
        monkeypatch.setattr(pc.cache, "incr", original_incr)

        async def healthy_ping() -> bytes:
            return b"PONG"

        monkeypatch.setattr(pc.cache, "ping", healthy_ping)

        # Advance past probe interval and run probe
        clock.advance(35.0)
        await _trigger_backend_probe(pc)

        # Backend must recover
        assert pc.is_degraded is False
        assert pc.is_running is True

        # Quota enforcement resumes (quota was already exceeded: 1 request limit)
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-b")


@tt.trio_test
async def test_probe_throttles_pings_to_avoid_stampede_while_degraded(monkeypatch):
    """Health check probe must throttle ping frequency to prevent hammering a failing backend.

    Catches: Unbounded probe ping execution on every degraded operation request.
    """
    clock = MockClock(1000.0)
    monkeypatch.setattr(time, "monotonic", clock)

    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=5,
        cache_failure_policy="fail-open",
    ) as pc:
        ping_spy = AsyncMock(side_effect=ConnectionError("Backend unreachable"))
        monkeypatch.setattr(pc.cache, "ping", ping_spy)

        # Enter degraded mode
        pc._degraded = True
        assert pc.is_degraded is True

        # Advance clock to permit the first probe
        clock.advance(35.0)
        await _trigger_backend_probe(pc)
        initial_ping_count = ping_spy.call_count

        # Fire 10 rapid operations within a 1-second window (< 30s probe interval)
        for _ in range(10):
            clock.advance(0.1)
            await pc.check_rate_limit("rapid-client")
            probe_fn = getattr(pc, "_maybe_probe_backend", None)
            if probe_fn is not None:
                res = probe_fn()
                if inspect.isawaitable(res):
                    await res

        # Ping count must not have surged 10 times; probe must be throttled
        assert ping_spy.call_count == initial_ping_count

        # Advance clock past probe interval (30s)
        clock.advance(35.0)
        await _trigger_backend_probe(pc)
        # Now a new probe should have executed
        assert ping_spy.call_count > initial_ping_count


@tt.trio_test
async def test_failing_probe_keeps_backend_degraded_and_traffic_uninterrupted(
    monkeypatch,
):
    """Failed health probes must not raise exceptions to callers and must maintain degraded status.

    Catches: Unhandled exception escaping during probe or premature de-flagging of degraded status.
    """
    clock = MockClock(1000.0)
    monkeypatch.setattr(time, "monotonic", clock)

    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        # Exhaust quota while healthy
        await pc.check_rate_limit("client-fail-probe")
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-fail-probe")

        # Degrade backend
        pc._degraded = True

        # Configure ping to raise TimeoutError
        async def timeout_ping() -> Any:
            raise TimeoutError("Ping timeout")

        monkeypatch.setattr(pc.cache, "ping", timeout_ping)

        clock.advance(35.0)
        # Probe should execute without raising
        await _trigger_backend_probe(pc)

        # Must remain degraded
        assert pc.is_degraded is True
        assert pc.is_running is False

        # Operation continues to bypass cleanly (fail-open)
        await pc.check_rate_limit("client-fail-probe")


@tt.trio_test
async def test_probe_recovery_restores_dns_caching_after_backend_outage(monkeypatch):
    """Backend recovery upon successful ping must restore DNS resolution caching.

    Catches: Degradation recovery restoring rate limiting but failing to restore backend DNS caching.
    """
    clock = MockClock(1000.0)
    monkeypatch.setattr(time, "monotonic", clock)

    lookup_calls = 0

    def fake_getaddrinfo(host: str, *args: Any, **kwargs: Any) -> list[Any]:
        nonlocal lookup_calls
        lookup_calls += 1
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                socket.IPPROTO_TCP,
                "",
                ("93.184.216.34", 80),
            )
        ]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)

    async with ProxyCache(
        cache_url="mem://",
        dns_cache_ttl="60s",
        cache_failure_policy="fail-open",
    ) as pc:
        # 1st lookup calls getaddrinfo and caches result
        ips1 = await pc.resolve_hosts("example.com")
        assert ips1 == ["93.184.216.34"]
        assert lookup_calls == 1

        # 2nd lookup hits cache
        ips2 = await pc.resolve_hosts("example.com")
        assert ips2 == ["93.184.216.34"]
        assert lookup_calls == 1

        # Enter degraded mode
        pc._degraded = True
        assert pc.is_degraded is True

        # In degraded mode, DNS resolution bypasses cache (returns raw host directly)
        bypassed_ips = await pc.resolve_hosts("example.com")
        assert bypassed_ips == ["example.com"]

        # Backend recovers upon successful ping
        async def healthy_ping() -> bytes:
            return b"PONG"

        monkeypatch.setattr(pc.cache, "ping", healthy_ping)

        clock.advance(35.0)
        await _trigger_backend_probe(pc)

        # Verify state recovered
        assert pc.is_degraded is False
        assert pc.is_running is True

        # DNS resolution resumes using cache: cached entry for example.com is returned
        recovered_ips = await pc.resolve_hosts("example.com")
        assert recovered_ips == ["93.184.216.34"]
