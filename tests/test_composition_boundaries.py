"""Composition selects effects locally without replacing application methods."""

import queue
from functools import partial

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import (
    LifecycleState,
    ProxyCache,
    ProxyCacheBuilder,
    StartupChannel,
    StartupResult,
    http,
    http_proxy,
)


@pytest.mark.parametrize("use_builder", [False, True])
def test_startup_channel_factory_is_forwarded_and_used_once(use_builder):
    channels: list[queue.Queue[StartupResult]] = []

    def channel_factory() -> StartupChannel:
        channel: queue.Queue[StartupResult] = queue.Queue(maxsize=1)
        channels.append(channel)
        return channel

    if use_builder:
        cache = ProxyCacheBuilder(
            rate_limit_enabled=True, startup_channel_factory=channel_factory
        ).build()
    else:
        cache = ProxyCache(
            rate_limit_enabled=True, startup_channel_factory=channel_factory
        )

    assert channels == []  # construction has no thread/handoff side effects
    with cache:
        cache.start()
        assert cache.is_running
        assert len(channels) == 1
        assert channels[0].empty()  # startup consumed the bridge's publication
    assert cache.is_stopped


@pytest.mark.parametrize("use_builder", [False, True])
def test_startup_channel_creation_failure_stops_only_its_cache(use_builder):
    def unavailable_channel() -> StartupChannel:
        raise RuntimeError("handoff unavailable")

    if use_builder:
        cache = ProxyCacheBuilder(
            rate_limit_enabled=True, startup_channel_factory=unavailable_channel
        ).build()
    else:
        cache = ProxyCache(
            rate_limit_enabled=True, startup_channel_factory=unavailable_channel
        )

    with pytest.raises(RuntimeError, match="handoff unavailable"):
        cache.start()
    assert cache.lifecycle_state is LifecycleState.STOPPED
    assert cache._thread is None
    cache.stop()
    with pytest.raises(RuntimeError, match="cannot be restarted"):
        cache.start()

    # No global queue patch leaks into independently composed instances.
    with ProxyCache(rate_limit_enabled=True) as healthy_cache:
        assert healthy_cache.is_running


def test_circuit_only_cache_needs_no_startup_channel():
    channels = []

    def channel_factory() -> StartupChannel:
        channels.append(True)
        return queue.Queue(maxsize=1)

    with ProxyCache(
        dns_cache_ttl="0s",
        rate_limit_enabled=False,
        startup_channel_factory=channel_factory,
    ) as cache:
        assert cache.is_running
    assert channels == []


@pytest.mark.parametrize("client_key", ["limited-client", None])
@tt.trio_test
async def test_client_identity_resolver_controls_rate_limit_admission(client_key):
    client, peer = tt.memory_stream_pair()
    resolved = []

    def identify(stream: trio.abc.Stream) -> str | None:
        resolved.append(stream)
        return client_key

    async with ProxyCache(rate_limit_enabled=True, rate_limit=1) as cache:
        await cache.check_rate_limit("limited-client")
        async with peer:
            await peer.send_all(b"INVALID REQUEST\r\n\r\n")
            await http_proxy(
                client,
                proxy_cache=cache,
                client_key_resolver=identify,
                limiter=None,
            )
            response = await peer.receive_some(4096)
    assert resolved == [client]
    if client_key is None:
        assert response == http.http_400_bad_request
    else:
        assert response.startswith(b"HTTP/1.1 429 Too Many Requests\r\n")


@tt.trio_test
async def test_disabled_cache_does_not_resolve_client_identity():
    client, peer = tt.memory_stream_pair()
    resolved = []

    def identify(stream: trio.abc.Stream) -> str:
        resolved.append(stream)
        return "client"

    async with peer:
        await peer.send_all(b"INVALID REQUEST\r\n\r\n")
        await http_proxy(
            client, proxy_cache=None, client_key_resolver=identify, limiter=None
        )
        assert await peer.receive_some(4096) == http.http_400_bad_request
    assert resolved == []


@tt.trio_test
async def test_default_socket_identity_enforces_quota_over_real_tcp():
    async with ProxyCache(rate_limit_enabled=True, rate_limit=1) as cache:
        await cache.check_rate_limit("127.0.0.1")
        handler = partial(http_proxy, proxy_cache=cache, limiter=None)
        with trio.fail_after(5):
            async with trio.open_nursery() as nursery:
                listeners = await nursery.start(
                    partial(trio.serve_tcp, handler, 0, host="127.0.0.1")
                )
                port = listeners[0].socket.getsockname()[1]
                async with await trio.open_tcp_stream("127.0.0.1", port) as stream:
                    # Admission happens before reading CONNECT bytes.
                    response = bytearray()
                    while data := await stream.receive_some(4096):
                        response.extend(data)
                    assert response.startswith(b"HTTP/1.1 429 Too Many Requests\r\n")
                nursery.cancel_scope.cancel()
