"""Dependency contracts, isolation, and generated cache/recovery invariants."""

import threading
from collections.abc import Callable

import pytest
import trio
from cache_fakes import ControlledBackend
from hypothesis import given
from hypothesis import settings as hypothesis_settings
from hypothesis import strategies as st

from trio_http_proxy import CacheBackend, LifecycleState, ProxyCache, ProxyCacheBuilder


@hypothesis_settings(max_examples=40, deadline=None)
@given(
    addresses=st.lists(
        st.sampled_from(["127.0.0.1", "192.0.2.1", "192.0.2.2", "::1", "2001:db8::1"]),
        min_size=1,
        max_size=20,
    ),
    ttl=st.integers(min_value=1, max_value=3600),
)
def test_dns_cache_preserves_order_deduplicates_and_clears(addresses, ttl):
    async def scenario():
        calls = []
        bridge_workers = []
        backend: CacheBackend = ControlledBackend()
        caller = threading.current_thread()

        def resolve(host):
            calls.append(host)
            bridge_workers.append(threading.current_thread())
            return tuple(addresses)

        async with ProxyCache(
            backend=backend, resolver=resolve, dns_cache_ttl=f"{ttl}s"
        ) as cache:
            expected = sorted(set(addresses), key=addresses.index)
            assert await cache.resolve_hosts("property.test") == expected
            assert await cache.resolve_hosts("property.test") == expected
            assert calls == ["property.test"]
            key = f"{cache.namespace}:dns:property.test"
            assert await cache.run(backend.get, key) == expected
            remaining = await cache.run(backend.get_expire, key)
            assert 0 <= remaining <= ttl
            await cache.run(backend.expire, key, -1)
            assert await cache.resolve_hosts("property.test") == expected
            assert calls == ["property.test", "property.test"]
            await cache.clear()
            assert await cache.resolve_hosts("property.test") == expected
            assert calls == ["property.test", "property.test", "property.test"]
            assert all(worker is not caller for worker in bridge_workers)

    trio.run(scenario)


@hypothesis_settings(max_examples=40, deadline=None)
@given(
    interval=st.integers(min_value=1, max_value=60),
    advances=st.lists(st.integers(min_value=0, max_value=70), max_size=30),
)
def test_recovery_probes_follow_only_the_injected_clock(interval, advances):
    async def scenario():
        now = 1000.0
        pings = 0
        backend = ControlledBackend()

        async def failed_operation(*args, **kwargs):
            raise ConnectionError("unavailable")

        async def failed_ping():
            nonlocal pings
            pings += 1
            raise ConnectionError("still unavailable")

        async with ProxyCache(
            backend=backend,
            clock=lambda: now,
            probe_interval=interval,
            rate_limit_enabled=True,
            rate_limit=1000,
        ) as cache:
            backend.effects["incr"] = failed_operation
            await cache.check_rate_limit("client")
            assert cache.is_degraded
            backend.effects["ping"] = failed_ping
            last_probe = now
            expected_probes = 0
            for advance in advances:
                now += advance
                if now - last_probe >= interval:
                    expected_probes += 1
                    last_probe = now
                await cache.check_rate_limit("client")
                assert pings == expected_probes
                assert cache.is_degraded

            # Recover precisely at the interval boundary, including empty histories.
            now += interval
            backend.effects.clear()
            await cache.check_rate_limit("client")
            assert not cache.is_degraded
            assert cache.is_running

    trio.run(scenario)


@pytest.mark.parametrize("via_builder", [False, True])
async def test_dependencies_are_forwarded_and_isolated_between_instances(via_builder):
    backend = ControlledBackend()
    lookups = []
    threads = []
    clock_reads = []

    def clock():
        clock_reads.append(True)
        return 1234.0

    def resolve(host):
        lookups.append(host)
        return ["192.0.2.10"]

    def thread_factory(*, target, daemon, name):
        thread = threading.Thread(target=target, daemon=daemon, name=name)
        threads.append(thread)
        return thread

    constructor = ProxyCacheBuilder if via_builder else ProxyCache
    owner = constructor(
        backend=backend,
        resolver=resolve,
        clock=clock,
        thread_factory=thread_factory,
        dns_cache_ttl="60s",
        cache_namespace="isolated",
    )
    # Builder context managers must pass dependencies through build(), too.
    async with owner as injected, ProxyCache(dns_cache_ttl="60s") as independent:
        assert injected.cache is backend
        assert independent.cache is not backend
        assert await injected.resolve_host("injected.test") == "192.0.2.10"
        key = "isolated:dns:injected.test"
        assert await injected.run(backend.get, key) == ["192.0.2.10"]
        assert await independent.run(independent.cache.get, key) is None
        assert lookups == ["injected.test"]
        assert len(threads) == 1
        assert clock_reads == [True]
        assert "backend" not in injected.settings.model_dump()
    assert not threads[0].is_alive()


@pytest.mark.parametrize("outcome", [[], OSError("lookup failed"), TypeError("bug")])
async def test_resolver_failures_are_not_backend_outages(outcome):
    def resolve(host):
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async with ProxyCache(resolver=resolve, dns_cache_ttl="60s") as cache:
        error_type = type(outcome) if isinstance(outcome, Exception) else OSError
        with pytest.raises(error_type):
            await cache.resolve_hosts("invalid.test")
        assert not cache.is_degraded
        assert cache.is_running


@pytest.mark.parametrize("host", ["127.0.0.1", "::1", "[::1]"])
async def test_ip_literals_do_not_invoke_resolver(host):
    def unexpected(host):
        pytest.fail("literal IP must bypass DNS")

    async with ProxyCache(resolver=unexpected, dns_cache_ttl="60s") as cache:
        assert await cache.resolve_hosts(host) == [host]


@pytest.mark.parametrize("disabled", [{"enabled": False}, {"cache_url": ""}])
async def test_disabled_backend_does_not_touch_injected_dependencies(disabled):
    backend = ControlledBackend()

    def unexpected(*args, **kwargs):
        pytest.fail("disabled caching must not use external effects")

    backend.effects["setup"] = unexpected
    async with ProxyCache(
        backend=backend,
        resolver=unexpected,
        clock=unexpected,
        thread_factory=unexpected,
        **disabled,
    ) as cache:
        assert await cache.resolve_hosts("disabled.test") == ["disabled.test"]
        await cache.check_rate_limit("client")


@pytest.mark.parametrize("fail_on_start", [False, True])
def test_thread_creation_failures_do_not_leave_starting_state(fail_on_start):
    class FailingThread:
        def start(self):
            raise RuntimeError("thread admission failed")

        def is_alive(self):
            return False

        def join(self, timeout=None):
            pass

    def factory(*, target: Callable[[], None], daemon: bool, name: str):
        if fail_on_start:
            return FailingThread()
        raise RuntimeError("thread creation failed")

    cache = ProxyCache(thread_factory=factory, rate_limit_enabled=True)
    with pytest.raises(RuntimeError, match="thread (admission|creation) failed"):
        cache.start()
    assert cache.lifecycle_state == LifecycleState.STOPPED
    assert cache.is_stopped
    cache.stop()
    with pytest.raises(RuntimeError, match="cannot be restarted"):
        cache.start()
