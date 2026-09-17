import asyncio
import socket
import threading

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.cache import (
    CACHE_FAILURE_POLICY,
    CacheBackendUnavailable,
    CircuitState,
    DestinationCircuitBreaker,
    ProxyCache,
    RateLimitError,
    _bool_env,
)


def test_failure_policy_default_and_validation():
    assert CACHE_FAILURE_POLICY == "fail-open"
    assert (
        ProxyCache(cache_failure_policy="fail-open").cache_failure_policy == "fail-open"
    )
    with pytest.raises(ValueError):
        ProxyCache(cache_failure_policy="sometimes")


def test_setup_failure_fail_open_and_fail_closed(monkeypatch):
    def fail_setup(*args, **kwargs):
        raise RuntimeError("backend unavailable")

    open_cache = ProxyCache(cache_url="mem://", cache_failure_policy="fail-open")
    monkeypatch.setattr(open_cache.cache, "setup", fail_setup)
    open_cache.start()
    assert open_cache.enabled is True
    assert open_cache.is_degraded is True
    assert open_cache.is_running is False
    open_cache.stop()

    closed_cache = ProxyCache(cache_url="mem://", cache_failure_policy="fail-closed")
    monkeypatch.setattr(closed_cache.cache, "setup", fail_setup)
    with pytest.raises(CacheBackendUnavailable):
        closed_cache.start()
    closed_cache.stop()


@tt.trio_test
async def test_dns_oserror_is_not_backend_failure(monkeypatch):
    def fail_lookup(*args, **kwargs):
        raise socket.gaierror("destination lookup failed")

    monkeypatch.setattr(socket, "getaddrinfo", fail_lookup)
    async with ProxyCache(cache_url="mem://", dns_cache_ttl="60s") as cache:
        with pytest.raises(socket.gaierror, match="destination lookup failed"):
            await cache.resolve_hosts("destination.invalid")


@tt.trio_test
async def test_lazy_backend_failure_fail_open_bypasses_dns_circuit_and_rate(
    monkeypatch,
):
    async with ProxyCache(
        cache_url="mem://",
        dns_cache_ttl="60s",
        circuit_breaker_enabled=True,
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-open",
    ) as cache:

        async def unavailable(*args, **kwargs):
            raise RuntimeError("backend unavailable")

        monkeypatch.setattr(cache.cache, "get", unavailable)
        assert await cache.resolve_host("backend.invalid") == "backend.invalid"
        await cache.check_circuit("backend.invalid", 443)
        await cache.check_rate_limit("client")
        assert cache.enabled is True
        assert cache.is_degraded is True
        assert cache.is_running is False


@tt.trio_test
async def test_lazy_backend_failure_fail_closed_raises(monkeypatch):
    async with ProxyCache(
        cache_url="mem://",
        dns_cache_ttl="60s",
        circuit_breaker_enabled=True,
        rate_limit_enabled=True,
        rate_limit=1,
        cache_failure_policy="fail-closed",
    ) as cache:

        async def unavailable(*args, **kwargs):
            raise RuntimeError("backend unavailable")

        monkeypatch.setattr(cache.cache, "get", unavailable)
        with pytest.raises(CacheBackendUnavailable):
            await cache.resolve_host("backend.invalid")

        monkeypatch.setattr(cache.cache, "incr", unavailable)
        with pytest.raises(CacheBackendUnavailable):
            await cache.check_rate_limit("client")


@tt.trio_test
async def test_rate_limit_retry_after_uses_exact_key_and_ttl():
    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=1,
        rate_limit_period="7s",
        rate_limit_ttl="13s",
    ) as cache:
        await cache.check_rate_limit("client")
        with pytest.raises(RateLimitError) as raised:
            await cache.check_rate_limit("client")
        retry_after = getattr(raised.value, "retry_after", None)
        assert isinstance(retry_after, int)
        assert retry_after >= 1
        assert retry_after <= 13


@tt.trio_test
async def test_repeated_fast_bridge_runs_do_not_deadlock():
    async with ProxyCache(cache_url="mem://") as cache:
        for index in range(100):
            assert await cache.run(cache.cache.get, f"missing-{index}") is None


@tt.trio_test
async def test_stop_drains_pending_bridge_future_and_closes_captured_loop():
    cache = ProxyCache(cache_url="mem://")
    cache.start()
    loop = cache._loop
    thread = cache._thread
    assert loop is not None and thread is not None

    async def pending():
        await asyncio.Event().wait()

    future = asyncio.run_coroutine_threadsafe(pending(), loop)
    await trio.to_thread.run_sync(cache.stop)
    assert future.done()
    assert not thread.is_alive()
    assert loop.is_closed()
    assert cache._loop is None
    assert cache._thread is None


@tt.trio_test
async def test_stop_waits_for_blocked_default_executor_job():
    started = threading.Event()
    release = threading.Event()
    captured_worker: list[threading.Thread] = []

    def blocked_getaddrinfo(*args, **kwargs):
        captured_worker.append(threading.current_thread())
        started.set()
        if not release.wait(5):
            raise TimeoutError("test DNS lookup was not released")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0))]

    cache = ProxyCache(cache_url="mem://", dns_cache_ttl="60s")
    cache.start()
    loop = cache._loop
    bridge_thread = cache._thread
    resolve_dns = cache._resolve_dns_fn
    assert loop is not None and bridge_thread is not None and resolve_dns is not None

    stop_done = threading.Event()
    stop_error: list[BaseException] = []

    def stop() -> None:
        try:
            cache.stop()
        except BaseException as exc:  # noqa: BLE001
            stop_error.append(exc)
        finally:
            stop_done.set()

    stop_thread = threading.Thread(target=stop, daemon=True)
    stop_started = False
    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setattr(socket, "getaddrinfo", blocked_getaddrinfo)
            future = asyncio.run_coroutine_threadsafe(resolve_dns("blocked.test"), loop)
            assert await trio.to_thread.run_sync(started.wait, 2)
            stop_started = True
            stop_thread.start()
            await trio.sleep(0.1)
            assert not stop_done.is_set()
    finally:
        release.set()
        if stop_started:
            stop_thread.join(timeout=3)
        else:
            cache.stop()

    assert stop_done.is_set()
    assert not stop_error
    assert future.done()
    assert captured_worker
    worker = captured_worker[0]
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert not bridge_thread.is_alive()
    assert loop.is_closed()
    assert cache._thread is None
    assert cache._loop is None


@tt.trio_test
async def test_stop_rejects_racing_submissions():
    cache = ProxyCache(cache_url="mem://")
    cache.start()
    started = threading.Event()

    async def pending():
        started.set()
        await asyncio.Event().wait()

    def submit() -> None:
        try:
            cache.run_coroutine(pending())
        except BaseException:  # noqa: BLE001
            return

    async with trio.open_nursery() as nursery:
        nursery.start_soon(trio.to_thread.run_sync, submit)
        assert await trio.to_thread.run_sync(started.wait, 2)
        await trio.to_thread.run_sync(cache.stop)
        nursery.cancel_scope.cancel()
    assert cache._stopped


def test_bool_env_strict_parsing():
    assert _bool_env("TEST_VAR", False, _getenv=lambda k: "1") is True
    assert _bool_env("TEST_VAR", False, _getenv=lambda k: "true") is True
    assert _bool_env("TEST_VAR", False, _getenv=lambda k: "YES") is True
    assert _bool_env("TEST_VAR", False, _getenv=lambda k: "on") is True

    assert _bool_env("TEST_VAR", True, _getenv=lambda k: "0") is False
    assert _bool_env("TEST_VAR", True, _getenv=lambda k: "false") is False
    assert _bool_env("TEST_VAR", True, _getenv=lambda k: "NO") is False
    assert _bool_env("TEST_VAR", True, _getenv=lambda k: "off") is False

    assert _bool_env("TEST_VAR", True, _getenv=lambda k: None) is True
    assert _bool_env("TEST_VAR", False, _getenv=lambda k: "  ") is False

    for typo in ("ture", "flase", "enabled", "disabled", "maybe", "2"):
        with pytest.raises(ValueError, match="Invalid boolean value for"):
            _bool_env("TEST_VAR", True, _getenv=lambda k, t=typo: t)


def test_semantic_boundary_validation_in_init():
    with pytest.raises(
        ValueError, match="circuit_breaker_errors_rate must be between 1 and 99"
    ):
        ProxyCache(circuit_breaker_errors_rate=0)
    with pytest.raises(
        ValueError, match="circuit_breaker_errors_rate must be between 1 and 99"
    ):
        ProxyCache(circuit_breaker_errors_rate=100)

    with pytest.raises(ValueError, match="circuit_breaker_min_calls must be >= 1"):
        ProxyCache(circuit_breaker_min_calls=0)

    with pytest.raises(ValueError, match="rate_limit must be >= 0"):
        ProxyCache(rate_limit=-1)

    with pytest.raises(ValueError, match="startup_timeout must be > 0"):
        ProxyCache(startup_timeout=0)

    with pytest.raises(ValueError, match="Cache namespace must be a non-empty string"):
        ProxyCache(namespace="   ")

    with pytest.raises(ValueError, match="CACHE_FAILURE_POLICY must be either"):
        ProxyCache(failure_policy="unsupported")


def test_destination_circuit_breaker_state_machine():
    cb = DestinationCircuitBreaker(
        errors_rate=50,
        period=10.0,
        ttl=5.0,
        min_calls=2,
    )
    t0 = 1000.0
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute(now=t0) is True

    # Call 1: failure
    cb.record_result(OSError("fail 1"), now=t0 + 1.0)
    assert cb.state == CircuitState.CLOSED  # min_calls not reached

    # Call 2: failure -> trips breaker!
    cb.record_result(OSError("fail 2"), now=t0 + 2.0)
    assert cb.state == CircuitState.OPEN
    assert cb.opened_at == t0 + 2.0

    # In OPEN state before TTL: rejected
    assert cb.can_execute(now=t0 + 4.0) is False

    # After TTL: transitions to HALF_OPEN
    # Probe discipline: first caller gets True (probe in flight)
    assert cb.can_execute(now=t0 + 7.1) is True
    assert cb.state == CircuitState.HALF_OPEN

    # Concurrent second caller while probe is in flight is gated/rejected!
    assert cb.can_execute(now=t0 + 7.2) is False

    # If probe fails: transitions back to OPEN
    cb.record_result(OSError("probe failed"), now=t0 + 7.3)
    assert cb.state == CircuitState.OPEN
    assert cb.opened_at == t0 + 7.3

    # Wait another TTL: transitions to HALF_OPEN again
    assert cb.can_execute(now=t0 + 13.0) is True
    assert cb.state == CircuitState.HALF_OPEN

    # Probe succeeds! Transitions back to CLOSED
    cb.record_result(None, now=t0 + 13.1)
    assert cb.state == CircuitState.CLOSED
    assert cb.can_execute(now=t0 + 13.2) is True


@tt.trio_test
async def test_safe_namespaced_clear_and_dangerous_flush():
    async with ProxyCache(cache_url="mem://", namespace="my_app") as pc:
        # Set keys in owned namespace
        await pc.run(pc.cache.set, "my_app:key1", "val1")
        await pc.run(pc.cache.set, "my_app:key2", "val2")
        # Set keys in unrelated namespace
        await pc.run(pc.cache.set, "other_app:key3", "val3")

        # clear() should delete only my_app:*
        await pc.clear()
        assert await pc.run(pc.cache.get, "my_app:key1") is None
        assert await pc.run(pc.cache.get, "my_app:key2") is None
        assert await pc.run(pc.cache.get, "other_app:key3") == "val3"

        # dangerous_flush_entire_database() deletes everything
        await pc.dangerous_flush_entire_database()
        assert await pc.run(pc.cache.get, "other_app:key3") is None


def test_proxy_cache_lifecycle_states():
    # 1. Configured disabled
    disabled_cache = ProxyCache(enabled=False)
    assert disabled_cache.configured_disabled is True
    assert disabled_cache.is_running is False
    assert disabled_cache.is_degraded is False
    assert disabled_cache.is_stopped is False

    # 2. Running cache
    running_cache = ProxyCache(cache_url="mem://")
    running_cache.start()
    assert running_cache.configured_disabled is False
    assert running_cache.is_running is True
    assert running_cache.is_degraded is False
    assert running_cache.is_stopped is False

    # 3. Stopped cache
    running_cache.stop()
    assert running_cache.is_stopped is True
    assert running_cache.is_running is False


@tt.trio_test
async def test_async_context_manager_non_blocking():
    # Verifies __aenter__ and __aexit__ execute asynchronously via trio thread worker
    async with ProxyCache(cache_url="mem://") as pc:
        assert pc.is_running is True
        assert await pc.resolve_host("127.0.0.1") == "127.0.0.1"
    assert pc.is_stopped is True
