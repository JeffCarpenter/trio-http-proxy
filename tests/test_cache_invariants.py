import asyncio
import socket
import threading

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.cache import (
    CacheBackendUnavailable,
    CircuitBreakerManager,
    CircuitBreakerOpen,
    CircuitState,
    DestinationCircuitBreaker,
    ProxyCache,
    RateLimitError,
    canonicalize_destination,
)


def test_failure_policy_default_and_validation(cache_settings):
    assert cache_settings.cache_failure_policy == "fail-open"
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
            raise ConnectionError("backend unavailable")

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
            raise ConnectionError("backend unavailable")

        monkeypatch.setattr(cache.cache, "get", unavailable)
        with pytest.raises(CacheBackendUnavailable):
            await cache.resolve_host("backend.invalid")

        async def broken_get(*args, **kwargs):
            raise TypeError("programming error")

        monkeypatch.setattr(cache.cache, "get", broken_get)
        with pytest.raises(TypeError, match="programming error"):
            await cache.resolve_host("backend.invalid")

        monkeypatch.setattr(cache.cache, "incr", unavailable)
        with pytest.raises(CacheBackendUnavailable):
            await cache.check_rate_limit("client")


def test_invariant_cross_clear_epoch_isolation():
    manager = CircuitBreakerManager(errors_rate=50, period=10.0, ttl=5.0, min_calls=2)
    b1 = manager.get_breaker("dest.example.com", 80)
    p1 = b1.acquire_permit(now=1000.0)
    assert p1 is not None

    manager.clear()
    b2 = manager.get_breaker("dest.example.com", 80)
    assert b1 is not b2
    assert b1.epoch != b2.epoch

    # Attempting to record p1 against b2 is rejected because permit.breaker is not b2
    b2.record_result(OSError("old error"), now=1001.0, permit=p1)
    assert len(b2._calls) == 0


def test_invariant_half_open_unleased_caller_rejected():
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("trip"), now=1000.0)
    assert cb.state == CircuitState.OPEN

    # Advance beyond TTL -> state moves to HALF_OPEN when probe is acquired
    probe = cb.acquire_permit(now=1006.0)
    assert probe is not None
    assert cb.state == CircuitState.HALF_OPEN

    # An unleased caller (permit=None) attempts to report success
    cb.record_result(None, now=1006.1, permit=None)
    # State MUST remain HALF_OPEN, not transitioned to CLOSED
    assert cb.state == CircuitState.HALF_OPEN

    # Legitimate probe reports success -> transitions to CLOSED
    cb.record_result(None, now=1006.2, permit=probe)
    assert cb.state == CircuitState.CLOSED


def test_invariant_aborted_probe_cannot_resuscitate_or_race():
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("trip"), now=1000.0)
    assert cb.state == CircuitState.OPEN

    probe_a = cb.acquire_permit(now=1006.0)
    assert probe_a is not None
    assert cb.state == CircuitState.HALF_OPEN

    # Probe A aborts (e.g. cancelled in Trio)
    probe_a.abort()
    assert probe_a.was_aborted is True

    # Probe B acquires lease
    probe_b = cb.acquire_permit(now=1006.5)
    assert probe_b is not None

    # Probe A finishes late in background and attempts to report success
    cb.record_result(None, now=1007.0, permit=probe_a)
    assert cb.state == CircuitState.HALF_OPEN

    # Probe B succeeds and closes circuit
    cb.record_result(None, now=1007.1, permit=probe_b)
    assert cb.state == CircuitState.CLOSED


def test_invariant_wedged_half_open_dropped_probe_timeout_recovery():
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("trip"), now=1000.0)
    assert cb.state == CircuitState.OPEN

    # Probe 1 starts at 1006.0 but crashes/drops without abort
    p1 = cb.acquire_permit(now=1006.0)
    assert p1 is not None

    # Probe in-flight before TTL expires -> rejected
    assert cb.acquire_permit(now=1008.0) is None

    # Once probe exceeds TTL (now - probe_started_at >= ttl), circuit recovers from wedge
    p2 = cb.acquire_permit(now=1012.0)
    assert p2 is not None
    assert p2.permit_id != p1.permit_id


def test_invariant_bracketed_ipv6_canonicalization():
    assert canonicalize_destination("[::1]", 8080) == ("::1", 8080)
    assert canonicalize_destination("[2001:db8::1]", 443) == ("2001:db8::1", 443)
    assert canonicalize_destination("::1", 8080) == ("::1", 8080)
    assert canonicalize_destination("EXAMPLE.COM.", 80) == ("example.com", 80)


@tt.trio_test
async def test_invariant_semantic_dns_ttl_parsing():
    async with ProxyCache(cache_url="mem://", dns_cache_ttl="0m") as c_zero:
        assert c_zero.dns_cache_ttl_sec == 0.0
        assert c_zero._resolve_dns_fn is None

    async with ProxyCache(cache_url="mem://", dns_cache_ttl="0s") as c_zero_s:
        assert c_zero_s.dns_cache_ttl_sec == 0.0
        assert c_zero_s._resolve_dns_fn is None

    async with ProxyCache(cache_url="mem://", dns_cache_ttl="15s") as c_pos:
        assert c_pos.dns_cache_ttl_sec == 15.0
        assert c_pos._resolve_dns_fn is not None


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


def test_cache_enabled_boolean_parsing(monkeypatch):
    from trio_http_proxy.config import CacheSettings

    for val in ("1", "true", "yes", "on"):
        monkeypatch.setenv("CACHE_ENABLED", val)
        assert CacheSettings().cache_enabled is True

    for val in ("0", "false", "no", "off"):
        monkeypatch.setenv("CACHE_ENABLED", val)
        assert CacheSettings().cache_enabled is False


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
    assert cb.acquire_permit(now=t0) is not None

    # Call 1: failure
    cb.record_result(OSError("fail 1"), now=t0 + 1.0)
    assert cb.state == CircuitState.CLOSED  # min_calls not reached

    # Call 2: failure -> trips breaker!
    cb.record_result(OSError("fail 2"), now=t0 + 2.0)
    assert cb.state == CircuitState.OPEN
    assert cb.opened_at == t0 + 2.0

    # In OPEN state before TTL: rejected
    assert cb.acquire_permit(now=t0 + 4.0) is None

    # After TTL: transitions to HALF_OPEN
    # Probe discipline: first caller gets True (probe in flight)
    permit = cb.acquire_permit(now=t0 + 7.1)
    assert permit is not None
    assert cb.state == CircuitState.HALF_OPEN

    # Concurrent second caller while probe is in flight is gated/rejected!
    assert cb.acquire_permit(now=t0 + 7.2) is None

    # If probe fails: transitions back to OPEN
    cb.record_result(OSError("probe failed"), now=t0 + 7.3, permit=permit)
    assert cb.state == CircuitState.OPEN
    assert cb.opened_at == t0 + 7.3

    # After another TTL:
    permit2 = cb.acquire_permit(now=t0 + 13.0)
    assert permit2 is not None
    assert cb.state == CircuitState.HALF_OPEN
    assert cb.acquire_permit(now=t0 + 13.1) is None
    cb.record_result(None, now=t0 + 13.2, permit=permit2)
    assert cb.state == CircuitState.CLOSED
    assert cb.opened_at is None
    assert cb.acquire_permit(now=t0 + 13.2) is not None


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


def test_rate_limit_zero_duration_rejection():
    with pytest.raises(ValueError, match="rate_limit_period must evaluate to > 0"):
        ProxyCache(rate_limit_period="0")
    with pytest.raises(ValueError, match="rate_limit_period must evaluate to > 0"):
        ProxyCache(rate_limit_period="0s")
    with pytest.raises(ValueError, match="rate_limit_ttl must evaluate to > 0"):
        ProxyCache(rate_limit_ttl="0")
    # "0s"/"" now uniformly mean disabled: rate-limit TTL falls back to period
    assert ProxyCache(rate_limit_ttl="0s").rate_limit_ttl_sec > 0


@tt.trio_test
async def test_rate_limit_cashews_ban_ttl_protocol(monkeypatch):
    async with ProxyCache(
        cache_url="mem://",
        rate_limit_enabled=True,
        rate_limit=2,
        rate_limit_period="10s",
        rate_limit_ttl="30s",
    ) as cache:
        key = "client-test-ban"

        # 1. First call: count = 1 <= 2 (permitted)
        await cache.check_rate_limit(key)

        # 2. Second call: count = 2 <= 2 (permitted)
        await cache.check_rate_limit(key)

        # 3. Third call: count = 3 == rate_limit + 1 (threshold crossed, ban applied)
        with pytest.raises(RateLimitError) as exc_info:
            await cache.check_rate_limit(key)
        assert exc_info.value.retry_after is not None and exc_info.value.retry_after > 0

        # 4. Fourth call: count = 4 > rate_limit + 1 (ban remains in effect)
        with pytest.raises(RateLimitError) as exc_info:
            await cache.check_rate_limit(key)
        assert exc_info.value.retry_after is not None and exc_info.value.retry_after > 0


@tt.trio_test
async def test_backend_error_boundary_reraises_programming_errors(monkeypatch):
    async with ProxyCache(
        cache_url="mem://",
        dns_cache_ttl="60s",
        cache_failure_policy="fail-open",
    ) as cache:

        async def broken_get(*args, **kwargs):
            raise TypeError("internal bug: wrong type passed")

        monkeypatch.setattr(cache.cache, "get", broken_get)

        # TypeError is an internal bug, not in BACKEND_EXCEPTIONS; must re-raise
        with pytest.raises(TypeError, match="internal bug"):
            await cache.resolve_host("buggy.invalid")

        # Cache must NOT be marked degraded because of a programming bug
        assert not cache.is_degraded


@tt.trio_test
async def test_proxy_cache_check_circuit_and_record_connection_result_protocol():
    """Verify that ProxyCache.check_circuit mints tokenized permits and record_connection_result closes HALF_OPEN."""
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_errors_rate=50,
        circuit_breaker_period="10s",
        circuit_breaker_ttl="2s",
        circuit_breaker_min_calls=2,
    ) as pc:
        # 1. Healthy CLOSED state returns permit with is_probe=False
        permit1 = await pc.check_circuit("api.service.local", 443)
        assert permit1 is not None
        assert not permit1.is_probe
        await pc.record_connection_result(
            "api.service.local", 443, exc=OSError("down 1"), permit=permit1
        )

        permit2 = await pc.check_circuit("api.service.local", 443)
        assert permit2 is not None
        await pc.record_connection_result(
            "api.service.local", 443, exc=OSError("down 2"), permit=permit2
        )

        # 2. Breaker has tripped to OPEN
        with pytest.raises(CircuitBreakerOpen):
            await pc.check_circuit("api.service.local", 443)

        # 3. Fast-forward past TTL cooldown
        breaker = pc._circuit_manager.get_breaker("api.service.local", 443)
        assert breaker.state == CircuitState.OPEN
        assert breaker.opened_at is not None
        breaker.opened_at -= 3.0

        # 4. First caller acquires probe permit in HALF_OPEN
        probe_permit = await pc.check_circuit("api.service.local", 443)
        assert probe_permit is not None
        assert probe_permit.is_probe
        assert breaker.state == CircuitState.HALF_OPEN

        # Concurrent unleased caller is rejected in HALF_OPEN
        with pytest.raises(CircuitBreakerOpen):
            await pc.check_circuit("api.service.local", 443)

        # 5. Successful probe connection records result and closes breaker
        await pc.record_connection_result(
            "api.service.local", 443, exc=None, permit=probe_permit
        )
        assert breaker.state == CircuitState.CLOSED

        # 6. Normal traffic resumes
        permit_normal = await pc.check_circuit("api.service.local", 443)
        assert permit_normal is not None
        assert not permit_normal.is_probe


def test_circuit_permit_context_manager_auto_abort():
    """Verify that uncompleted with-statement exit automatically aborts permit."""
    breaker = DestinationCircuitBreaker(
        errors_rate=50, period=10.0, ttl=5.0, min_calls=1
    )
    breaker.record_result(OSError("trip"), now=1000.0)
    assert breaker.state == CircuitState.OPEN

    # Transition to HALF_OPEN by acquiring probe permit
    probe = breaker.acquire_permit(now=1006.0)
    assert probe is not None
    assert probe.is_probe
    assert breaker._half_open_in_flight

    # Exiting context manager without completing aborts the probe
    with probe:
        pass  # Did not call mark_completed() or record_result()

    assert probe.was_aborted
    assert not breaker._half_open_in_flight

    # New probe can now be acquired since previous probe was aborted
    probe2 = breaker.acquire_permit(now=1007.0)
    assert probe2 is not None
    assert breaker._half_open_in_flight


@tt.trio_test
async def test_circuit_breaker_functions_during_backend_outage():
    """Verify that circuit breaker admission and recording function during backend outage (degraded)."""
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_min_calls=2,
        circuit_breaker_errors_rate=50,
        cache_failure_policy="fail-open",
    ) as cache:
        # Simulate backend degradation (e.g. remote Redis outage in fail-open mode)
        cache._degraded = True
        assert cache.is_degraded is True

        # Circuit breaker must not bypass: recording failures must still trip the breaker
        permit1 = await cache.check_circuit("upstream.example.com", 80)
        await cache.record_connection_result(
            "upstream.example.com",
            80,
            exc=ConnectionRefusedError("fail 1"),
            permit=permit1,
        )

        permit2 = await cache.check_circuit("upstream.example.com", 80)
        await cache.record_connection_result(
            "upstream.example.com",
            80,
            exc=ConnectionRefusedError("fail 2"),
            permit=permit2,
        )

        # Circuit breaker must be OPEN and reject traffic even when backend is degraded
        with pytest.raises(CircuitBreakerOpen):
            await cache.check_circuit("upstream.example.com", 80)
