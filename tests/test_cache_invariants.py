import asyncio
import socket
import threading

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.cache import (
    CACHE_FAILURE_POLICY,
    CacheBackendUnavailable,
    ProxyCache,
    RateLimitError,
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
    assert open_cache.enabled is False
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
        assert cache.enabled is False


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

        monkeypatch.setattr(cache.cache, "is_locked", unavailable)
        with pytest.raises(CacheBackendUnavailable):
            await cache.check_circuit("backend.invalid", 443)

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
