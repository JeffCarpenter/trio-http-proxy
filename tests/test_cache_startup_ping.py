"""Tests for ProxyCache startup backend health ping behavior.

Behavior contract:
When backend caching is enabled (enabled=True, cache_url non-empty):
- During start(), a health ping is executed against the backend (cache.cache.ping)
  within startup_timeout.
- Under fail-closed policy: transport failure or timeout during startup ping must
  fail fast by raising CacheBackendUnavailable, closing any started asyncio loop
  and joining the bridge thread (is_running=False, thread dead/None, loop closed).
- Under fail-open policy: transport failure during startup ping must not raise,
  transitioning into degraded mode (is_degraded=True, is_running=False) while
  keeping the loop/thread alive for periodic recovery probes.
- When startup ping succeeds: cache transitions to healthy (is_running=True,
  is_degraded=False).
"""

import pytest

from trio_http_proxy.cache import ProxyCache
from trio_http_proxy.errors import CacheBackendUnavailable


def test_startup_ping_fail_closed_connection_error_fails_fast_and_cleans_up(
    backend,
):
    """Break caught: Under fail-closed, an unreachable backend during startup ping
    must fail fast by raising CacheBackendUnavailable and cleanly shutting down
    the bridge thread and loop, rather than reporting is_running=True.
    """

    async def fail_ping(*args, **kwargs):
        raise ConnectionError("connection refused")

    cache = ProxyCache(
        backend=backend,
        cache_url="mem://",
        cache_failure_policy="fail-closed",
        startup_timeout=2.0,
    )
    backend.effects["ping"] = fail_ping

    with pytest.raises(CacheBackendUnavailable):
        cache.start()

    assert cache.is_running is False
    assert cache.is_degraded is False
    assert cache.is_stopped is True
    if cache._loop is not None:
        assert cache._loop.is_closed()
    if cache._thread is not None:
        assert not cache._thread.is_alive()


def test_startup_ping_fail_closed_timeout_fails_fast_and_cleans_up(backend):
    """Break caught: Under fail-closed, a hung or timed-out startup ping must fail
    fast by raising CacheBackendUnavailable and closing the bridge thread/loop.
    """

    async def timeout_ping(*args, **kwargs):
        raise TimeoutError("backend ping timed out")

    cache = ProxyCache(
        backend=backend,
        cache_url="mem://",
        cache_failure_policy="fail-closed",
        startup_timeout=0.5,
    )
    backend.effects["ping"] = timeout_ping

    with pytest.raises(CacheBackendUnavailable):
        cache.start()

    assert cache.is_running is False
    assert cache.is_degraded is False
    assert cache.is_stopped is True
    if cache._loop is not None:
        assert cache._loop.is_closed()
    if cache._thread is not None:
        assert not cache._thread.is_alive()


def test_startup_ping_fail_open_enters_degraded_and_keeps_loop_alive(backend):
    """Break caught: Under fail-open, a failed startup ping must not raise and must
    transition the cache into degraded mode (is_degraded=True, is_running=False),
    keeping the loop and thread running so recovery probes can run.
    """

    async def fail_ping(*args, **kwargs):
        raise ConnectionError("connection refused")

    cache = ProxyCache(
        backend=backend,
        cache_url="mem://",
        cache_failure_policy="fail-open",
        startup_timeout=2.0,
    )
    backend.effects["ping"] = fail_ping

    try:
        cache.start()
        assert cache.is_degraded is True
        assert cache.is_running is False
        assert cache.is_stopped is False
        assert cache._loop is not None
        assert not cache._loop.is_closed()
        assert cache._thread is not None
        assert cache._thread.is_alive()
    finally:
        cache.stop()
        assert cache.is_stopped is True


def test_startup_ping_success_sets_running_and_healthy(backend):
    """Break caught: Successful startup ping must mark cache as running and healthy
    (is_running=True, is_degraded=False) with active bridge thread and loop.
    """

    async def success_ping(*args, **kwargs):
        return b"PONG"

    cache = ProxyCache(
        backend=backend,
        cache_url="mem://",
        cache_failure_policy="fail-closed",
        startup_timeout=2.0,
    )
    backend.effects["ping"] = success_ping

    try:
        cache.start()
        assert cache.is_running is True
        assert cache.is_degraded is False
        assert cache.is_stopped is False
        assert cache._loop is not None
        assert not cache._loop.is_closed()
        assert cache._thread is not None
        assert cache._thread.is_alive()
    finally:
        cache.stop()
        assert cache.is_stopped is True


def test_startup_ping_skipped_when_backend_disabled(backend):
    """Break caught: When cache.enabled is False, start() must not attempt a ping
    or spawn a bridge thread.
    """

    async def unexpected_ping(*args, **kwargs):
        raise AssertionError("ping should not be executed when cache is disabled")

    cache = ProxyCache(
        backend=backend,
        cache_url="mem://",
        enabled=False,
        cache_failure_policy="fail-closed",
    )
    backend.effects["ping"] = unexpected_ping

    cache.start()
    assert cache._thread is None
    assert cache._loop is None
    cache.stop()


def test_startup_ping_skipped_when_cache_url_empty(backend):
    """Break caught: When cache_url is empty, start() must not attempt a ping
    or spawn a bridge thread.
    """

    async def unexpected_ping(*args, **kwargs):
        raise AssertionError("ping should not be executed when cache_url is empty")

    cache = ProxyCache(
        backend=backend,
        cache_url="",
        enabled=True,
        cache_failure_policy="fail-closed",
    )
    backend.effects["ping"] = unexpected_ping

    cache.start()
    assert cache._thread is None
    assert cache._loop is None
    cache.stop()
