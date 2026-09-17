import pytest
import trio.testing as tt

from trio_http_proxy.cache import CircuitBreakerOpen, ProxyCache


@tt.trio_test
async def test_thread_leak_eliminated_when_cache_disabled_explicitly() -> None:
    """A background thread leak occurs if an event loop or OS thread is spawned when cache is disabled."""
    cache = ProxyCache(enabled=False)
    cache.start()

    try:
        assert cache._thread is None, "Background OS thread was leaked when cache was explicitly disabled"
        assert cache._loop is None, "Asyncio event loop was leaked when cache was explicitly disabled"
        assert cache.is_running is True, "ProxyCache should report is_running=True once started even when cache is disabled"

        # Verify in-memory circuit breaker functions normally without thread or loop
        permit = await cache.check_circuit("example.com", 443)
        assert permit is not None
        await cache.record_connection_result("example.com", 443, exc=None, permit=permit)

        # Verify circuit breaker can trip on repeated failures even when cache is explicitly disabled
        host = "disabled-cache-circuit.test"
        port = 8080
        for _ in range(3):
            p = await cache.check_circuit(host, port)
            assert p is not None
            await cache.record_connection_result(host, port, exc=OSError("connection failed"), permit=p)

        with pytest.raises(CircuitBreakerOpen):
            await cache.check_circuit(host, port)
    finally:
        cache.stop()

    assert cache.is_stopped is True, "ProxyCache should report is_stopped=True after stop()"
    assert cache.is_running is False, "ProxyCache should report is_running=False after stop()"
    assert cache._thread is None
    assert cache._loop is None


@tt.trio_test
async def test_thread_leak_eliminated_when_cache_url_empty() -> None:
    """A background thread leak occurs if an event loop or OS thread is spawned when cache_url is empty."""
    cache = ProxyCache(cache_url="")
    cache.start()

    try:
        assert cache._thread is None, "Background OS thread was leaked when cache_url was empty"
        assert cache._loop is None, "Asyncio event loop was leaked when cache_url was empty"
        assert cache.is_running is True, "ProxyCache should report is_running=True once started when cache_url is empty"

        permit = await cache.check_circuit("example.com", 80)
        assert permit is not None
        await cache.record_connection_result("example.com", 80, exc=None, permit=permit)

        # In-memory circuit breaker operations must function normally and trip when cache_url is empty
        host = "empty-url-circuit.test"
        port = 8080
        for _ in range(3):
            p = await cache.check_circuit(host, port)
            assert p is not None
            await cache.record_connection_result(host, port, exc=OSError("connection failed"), permit=p)

        with pytest.raises(CircuitBreakerOpen):
            await cache.check_circuit(host, port)
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


@tt.trio_test
async def test_thread_leak_eliminated_when_no_backend_features_enabled_sync_start() -> None:
    """A background thread leak occurs if an event loop or OS thread is spawned when no backend features are enabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="0s",
        rate_limit_enabled=False,
        circuit_breaker_min_calls=3,
        circuit_breaker_errors_rate=50,
    )
    cache.start()

    try:
        assert cache._thread is None, "Background OS thread was leaked when no backend features were enabled"
        assert cache._loop is None, "Asyncio event loop was leaked when no backend features were enabled"
        assert cache.is_running is True, "ProxyCache should report is_running=True once started"

        # In-memory circuit breaker operations must function normally and be able to trip
        host = "tripping-service.test"
        port = 8080
        for _ in range(3):
            permit = await cache.check_circuit(host, port)
            assert permit is not None
            await cache.record_connection_result(host, port, exc=OSError("connection failed"), permit=permit)

        with pytest.raises(CircuitBreakerOpen):
            await cache.check_circuit(host, port)

        assert cache._thread is None, "Thread was unexpectedly spawned during circuit breaker operations"
        assert cache._loop is None, "Loop was unexpectedly spawned during circuit breaker operations"
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


@tt.trio_test
async def test_thread_leak_eliminated_when_no_backend_features_enabled_ensure_started() -> None:
    """A background thread leak occurs if _ensure_started() spawns an event loop or OS thread when features are disabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="0s",
        rate_limit_enabled=False,
    )
    # Trigger lazy startup via _ensure_started()
    cache._ensure_started()

    try:
        assert cache._thread is None, "Background OS thread was leaked by _ensure_started() when no backend features enabled"
        assert cache._loop is None, "Asyncio event loop was leaked by _ensure_started() when no backend features enabled"
        assert cache.is_running is True

        permit = await cache.check_circuit("example.com", 443)
        assert permit is not None
        await cache.record_connection_result("example.com", 443, exc=None, permit=permit)
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


@tt.trio_test
async def test_thread_leak_eliminated_in_async_context_manager_when_backend_features_disabled() -> None:
    """A background thread leak occurs if async context manager spawns an event loop or thread when features are disabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="0s",
        rate_limit_enabled=False,
    )
    async with cache:
        assert cache._thread is None, "Background OS thread was leaked inside async context manager"
        assert cache._loop is None, "Asyncio event loop was leaked inside async context manager"
        assert cache.is_running is True

        permit = await cache.check_circuit("10.0.0.1", 80)
        assert permit is not None
        await cache.record_connection_result("10.0.0.1", 80, exc=None, permit=permit)

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


def test_bridge_thread_spawned_when_dns_cache_feature_enabled_control() -> None:
    """Positive control: bridge thread MUST be spawned when DNS caching feature is enabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="10s",
        rate_limit_enabled=False,
    )
    cache.start()

    try:
        assert cache._thread is not None, "Expected bridge thread to be spawned when dns_cache_ttl > 0s"
        assert cache._thread.is_alive() is True, "Bridge thread should be active and running"
        assert cache._loop is not None, "Expected asyncio event loop to be initialized"
        assert cache.is_running is True
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


def test_bridge_thread_spawned_when_rate_limit_feature_enabled_control() -> None:
    """Positive control: bridge thread MUST be spawned when rate limiting feature is enabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="0s",
        rate_limit_enabled=True,
        rate_limit=10,
    )
    cache.start()

    try:
        assert cache._thread is not None, "Expected bridge thread to be spawned when rate_limit_enabled is True"
        assert cache._thread.is_alive() is True, "Bridge thread should be active and running"
        assert cache._loop is not None, "Expected asyncio event loop to be initialized"
        assert cache.is_running is True
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None
