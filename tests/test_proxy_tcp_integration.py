"""Real TCP composition and cleanup, without TLS or external network services."""

import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from functools import partial

import pytest
import trio
import trio.testing

from trio_http_proxy import ProxyCache, Settings, http_proxy


@dataclass
class Connections:
    """Observe server-owned resources without replacing application behavior."""

    streams: list[trio.SocketStream] = field(default_factory=list)
    finished: int = 0
    changed: trio.Event = field(default_factory=trio.Event)

    def notify(self) -> None:
        self.changed.set()
        self.changed = trio.Event()

    async def run(
        self,
        handler: Callable[[trio.SocketStream], Awaitable[None]],
        stream: trio.SocketStream,
    ) -> None:
        self.streams.append(stream)
        self.notify()
        try:
            await handler(stream)
        finally:
            self.finished += 1
            self.notify()

    async def wait_finished(self, count: int) -> None:
        while self.finished < count:
            await self.changed.wait()

    async def wait_started(self, count: int) -> None:
        while len(self.streams) < count:
            await self.changed.wait()

    def assert_closed(self) -> None:
        assert self.finished == len(self.streams)
        assert all(stream.socket.fileno() == -1 for stream in self.streams)


async def echo(stream: trio.SocketStream) -> None:
    async with stream:
        try:
            while data := await stream.receive_some(65536):
                await stream.send_all(data)
            await stream.send_eof()
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass  # Disconnects are expected during churn and cancellation.


@dataclass(frozen=True)
class LoopbackServers:
    proxy_port: int
    origin_port: int
    proxy: Connections
    origin: Connections


@asynccontextmanager
async def loopback_servers(
    cache: ProxyCache, limiter: trio.CapacityLimiter
) -> AsyncIterator[LoopbackServers]:
    proxy, origin = Connections(), Connections()
    async with trio.open_nursery() as nursery:
        origin_listeners = await nursery.start(
            partial(trio.serve_tcp, partial(origin.run, echo), 0, host="127.0.0.1")
        )
        handler = partial(
            http_proxy, proxy_cache=cache, limiter=limiter, settings=Settings()
        )
        proxy_listeners = await nursery.start(
            partial(trio.serve_tcp, partial(proxy.run, handler), 0, host="127.0.0.1")
        )
        try:
            yield LoopbackServers(
                proxy_listeners[0].socket.getsockname()[1],
                origin_listeners[0].socket.getsockname()[1],
                proxy,
                origin,
            )
        finally:
            nursery.cancel_scope.cancel()
    proxy.assert_closed()
    origin.assert_closed()
    assert limiter.borrowed_tokens == 0
    assert all(
        listener.socket.fileno() == -1
        for listener in proxy_listeners + origin_listeners
    )


async def read_headers(stream: trio.SocketStream) -> bytes:
    headers = bytearray()
    while not headers.endswith(b"\r\n\r\n"):
        chunk = await stream.receive_some(1)
        assert chunk, f"EOF before complete response: {headers!r}"
        headers.extend(chunk)
        assert len(headers) <= 4096
    return bytes(headers)


async def connect(stream: trio.SocketStream, origin_port: int) -> None:
    target = f"upstream.test:{origin_port}"
    await stream.send_all(
        f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode()
    )
    assert (await read_headers(stream)).startswith(b"HTTP/1.1 200 ")


async def round_trip(stream: trio.SocketStream, payload: bytes) -> None:
    await stream.send_all(payload)
    received = bytearray()
    while len(received) < len(payload):
        chunk = await stream.receive_some(len(payload) - len(received))
        assert chunk, "EOF before complete echo"
        received.extend(chunk)
    assert received == payload


async def test_real_tcp_connect_dns_quota_and_shutdown():
    lookups, resolver_workers = [], []

    def resolve(host):
        lookups.append(host)
        resolver_workers.append(threading.current_thread())
        return ["127.0.0.1"]

    cache = ProxyCache(
        resolver=resolve,
        dns_cache_ttl="60s",
        rate_limit_enabled=True,
        rate_limit=2,
        rate_limit_period="60s",
    )
    limiter = trio.CapacityLimiter(4)
    async with cache:
        worker, loop = cache._thread, cache._loop
        assert worker is not None and worker.is_alive()
        assert loop is not None and loop.is_running()
        with trio.fail_after(15):
            async with loopback_servers(cache, limiter) as servers:
                for repetitions in (1, 2):
                    async with await trio.open_tcp_stream(
                        "127.0.0.1", servers.proxy_port
                    ) as client:
                        await connect(client, servers.origin_port)
                        await round_trip(client, bytes(range(256)) * repetitions)
                        await client.send_eof()
                        assert await client.receive_some(1) == b""
                    await servers.proxy.wait_finished(repetitions)
                    await servers.origin.wait_finished(repetitions)

                # Limiting precedes parsing. Do not send unread CONNECT bytes,
                # which can turn a graceful rejection into a TCP reset on close.
                async with await trio.open_tcp_stream(
                    "127.0.0.1", servers.proxy_port
                ) as client:
                    response = await read_headers(client)
                    assert response.startswith(b"HTTP/1.1 429 ")
                    assert b"Retry-After:" in response
                    # Drain any response body and observe the peer's close.
                    while await client.receive_some(4096):
                        pass
                await servers.proxy.wait_finished(3)
                assert len(servers.origin.streams) == 2
                assert lookups == ["upstream.test"]
    assert cache.is_stopped
    assert cache._thread is None
    assert not worker.is_alive()
    assert loop.is_closed()
    assert resolver_workers
    assert all(not thread.is_alive() for thread in resolver_workers)


@pytest.mark.parametrize("cycle", range(3))
async def test_tcp_churn_and_shutdown_with_live_handshakes_and_tunnels(cycle):
    resolver_workers = []

    def resolve(host):
        resolver_workers.append(threading.current_thread())
        return ["127.0.0.1"]

    cache = ProxyCache(
        resolver=resolve,
        dns_cache_ttl="60s",
        rate_limit_enabled=False,
    )
    limiter = trio.CapacityLimiter(12)
    async with cache:
        worker, loop = cache._thread, cache._loop
        assert worker is not None and loop is not None
        with trio.fail_after(20):
            async with AsyncExitStack() as clients:
                async with loopback_servers(cache, limiter) as servers:

                    async def short_connection(index):
                        async with await trio.open_tcp_stream(
                            "127.0.0.1", servers.proxy_port
                        ) as client:
                            if index % 3 == 1:
                                await client.send_all(b"CONNECT upstream.test:")
                            elif index % 3 == 2:
                                await connect(client, servers.origin_port)
                                await round_trip(
                                    client, bytes(range(256)) * (cycle + 1)
                                )
                            # No headers, partial headers, or abrupt tunnel EOF.

                    for wave in range(3):
                        async with trio.open_nursery() as nursery:
                            for index in range(24):
                                nursery.start_soon(short_connection, index)
                        await servers.proxy.wait_finished((wave + 1) * 24)
                        await servers.origin.wait_finished((wave + 1) * 8)
                        assert limiter.borrowed_tokens == 0
                        servers.proxy.assert_closed()
                        servers.origin.assert_closed()

                    held = []
                    for index in range(12):
                        client = await clients.enter_async_context(
                            await trio.open_tcp_stream("127.0.0.1", servers.proxy_port)
                        )
                        held.append(client)
                        if index % 2:
                            await connect(client, servers.origin_port)
                            await round_trip(client, b"still active")
                        else:
                            await client.send_all(b"CONNECT upstream.test:")
                    await servers.proxy.wait_started(84)
                    await trio.testing.wait_all_tasks_blocked()
                    assert servers.proxy.finished == 72
                    assert limiter.borrowed_tokens == 12
                    # Leaving the server context cancels six incomplete
                    # handshakes and six active bidirectional pipe nurseries.
                for client in held:
                    assert await client.receive_some(1) == b""
    assert cache.is_stopped
    assert not worker.is_alive()
    assert loop.is_closed()
    assert resolver_workers
    assert all(not thread.is_alive() for thread in resolver_workers)
