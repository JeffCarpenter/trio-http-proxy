"""Recover backend service through public operations and a controlled transport."""

import pytest
import trio.testing as tt
from cache_fakes import ControlledBackend

from trio_http_proxy.cache import ProxyCache
from trio_http_proxy.errors import RateLimitError


class ManualClock:
    """Monotonic time source advanced explicitly, independent of wall-clock sleeps."""

    def __init__(self, initial: float = 1000.0) -> None:
        self.current = initial

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


async def unavailable(*args: object, **kwargs: object) -> None:
    raise ConnectionError("Backend unreachable")


async def interrupt_rate_limiting(
    pc: ProxyCache, backend: ControlledBackend, key: str
) -> None:
    """Enter fail-open degradation through a real failed backend operation."""
    backend.effects["incr"] = unavailable
    try:
        await pc.check_rate_limit(key)
    finally:
        backend.effects.pop("incr")
    assert pc.is_degraded
    assert not pc.is_running


@tt.trio_test
async def test_sticky_degradation_recovers_after_temporary_backend_blip(backend):
    clock = ManualClock()
    async with ProxyCache(
        backend=backend,
        clock=clock,
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        assert pc.is_running
        assert not pc.is_degraded
        await pc.check_rate_limit("client-a")
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-a")

        await interrupt_rate_limiting(pc, backend, "client-a")
        await pc.check_rate_limit("client-a")  # bypass exhausted quota
        backend.effects["ping"] = unavailable
        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert pc.is_degraded
        assert not pc.is_running
        await pc.check_rate_limit("client-a")

        backend.effects.pop("ping")  # probes now reach the real memory backend
        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert not pc.is_degraded
        assert pc.is_running
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-a")


@tt.trio_test
async def test_runtime_backend_failure_fail_open_degrades_and_recovers_on_ping(backend):
    clock = ManualClock()
    async with ProxyCache(
        backend=backend,
        clock=clock,
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        await pc.check_rate_limit("client-b")
        await interrupt_rate_limiting(pc, backend, "client-b")
        await pc.check_rate_limit("client-b")

        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert not pc.is_degraded
        assert pc.is_running
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-b")


@tt.trio_test
async def test_probe_throttles_pings_to_avoid_stampede_while_degraded(backend):
    clock = ManualClock()
    async with ProxyCache(
        backend=backend,
        clock=clock,
        rate_limit_enabled=True,
        rate_limit=5,
        cache_failure_policy="fail-open",
    ) as pc:
        backend.effects["ping"] = unavailable
        await interrupt_rate_limiting(pc, backend, "rapid-client")
        startup_pings = backend.operations.count("ping")
        assert startup_pings == 1

        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert backend.operations.count("ping") == startup_pings + 1

        for _ in range(10):
            clock.advance(0.1)
            await pc.check_rate_limit("rapid-client")
            await pc._maybe_probe_backend()
        assert backend.operations.count("ping") == startup_pings + 1

        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert backend.operations.count("ping") == startup_pings + 2
        assert pc.is_degraded


@tt.trio_test
async def test_failing_probe_keeps_backend_degraded_and_traffic_uninterrupted(backend):
    clock = ManualClock()
    async with ProxyCache(
        backend=backend,
        clock=clock,
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as pc:
        await pc.check_rate_limit("client-fail-probe")
        with pytest.raises(RateLimitError):
            await pc.check_rate_limit("client-fail-probe")
        await interrupt_rate_limiting(pc, backend, "client-fail-probe")

        async def timeout_ping() -> bytes:
            raise TimeoutError("Ping timeout")

        backend.effects["ping"] = timeout_ping
        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert pc.is_degraded
        assert not pc.is_running
        await pc.check_rate_limit("client-fail-probe")


@tt.trio_test
async def test_probe_recovery_restores_dns_caching_after_backend_outage(backend):
    clock = ManualClock()
    lookups = []

    def resolve(host: str) -> list[str]:
        lookups.append(host)
        return ["93.184.216.34"]

    async with ProxyCache(
        backend=backend,
        clock=clock,
        resolver=resolve,
        dns_cache_ttl="60s",
        cache_failure_policy="fail-open",
    ) as pc:
        assert await pc.resolve_hosts("example.com") == ["93.184.216.34"]
        assert await pc.resolve_hosts("example.com") == ["93.184.216.34"]
        assert lookups == ["example.com"]

        backend.effects["get"] = unavailable
        assert await pc.resolve_hosts("example.com") == ["example.com"]
        backend.effects.pop("get")
        assert pc.is_degraded
        assert await pc.resolve_hosts("example.com") == ["example.com"]

        clock.advance(35.0)
        await pc._maybe_probe_backend()
        assert not pc.is_degraded
        assert pc.is_running
        assert await pc.resolve_hosts("example.com") == ["93.184.216.34"]
        assert lookups == ["example.com"]  # recovery reuses the original cached entry
