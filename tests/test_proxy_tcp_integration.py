"""Live loopback TCP tests for the nano CONNECT server."""

from __future__ import annotations

import socket
from contextlib import AsyncExitStack
from functools import partial

import trio

from trio_http_proxy import http, start_server


def connect_request(port: int) -> bytes:
    return (
        f"CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
    ).encode()


async def test_live_tcp_round_trip(
    nursery: trio.Nursery, dest_server_port: int
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    assert listeners[0].socket.getsockname()[0] == "127.0.0.1"
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        await client.send_all(connect_request(dest_server_port))
        assert await client.receive_some(4096) == http.HTTP_200_CONNECTION_ESTABLISHED
        await client.send_all(b"GET /echo HTTP/1.1\r\nHost: localhost\r\n\r\n")
        response = bytearray()
        while chunk := await client.receive_some(4096):
            response.extend(chunk)
        assert response.startswith(b"HTTP/1.1 200 OK\r\n")
        assert b"GET /echo HTTP/1.1" in response


async def test_live_tcp_concurrent_tunnels(
    nursery: trio.Nursery, dest_server_port: int
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    proxy_port = listeners[0].socket.getsockname()[1]
    async with (
        await trio.open_tcp_stream("127.0.0.1", proxy_port) as first,
        await trio.open_tcp_stream("127.0.0.1", proxy_port) as second,
    ):
        # Both handshakes complete before either connection sends its payload.
        for client in (first, second):
            await client.send_all(connect_request(dest_server_port))
            assert (
                await client.receive_some(4096) == http.HTTP_200_CONNECTION_ESTABLISHED
            )
        for index, client in enumerate((first, second)):
            await client.send_all(
                f"GET /{index} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
            )
            response = bytearray()
            while chunk := await client.receive_some(4096):
                response.extend(chunk)
            assert f"GET /{index} HTTP/1.1".encode() in response


async def test_live_tcp_destination_unreachable_returns_502(
    nursery: trio.Nursery,
) -> None:
    # Keep the port reserved without listening so nothing else can claim it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unused:
        unused.bind(("127.0.0.1", 0))
        listeners = await nursery.start(partial(start_server, 0))
        proxy_port = listeners[0].socket.getsockname()[1]
        async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
            await client.send_all(connect_request(unused.getsockname()[1]))
            assert await client.receive_some(4096) == http.HTTP_502_BAD_GATEWAY
            assert await client.receive_some(4096) == b""


async def test_live_tcp_invalid_request_returns_400(nursery: trio.Nursery) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    proxy_port = listeners[0].socket.getsockname()[1]
    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert await client.receive_some(4096) == http.HTTP_400_BAD_REQUEST
        assert await client.receive_some(4096) == b""


async def test_server_custom_host_and_cancellation_close_active_tunnel(
    nursery: trio.Nursery,
) -> None:
    upstream_closed = trio.Event()

    async def origin(stream: trio.SocketStream) -> None:
        async with stream:
            assert await stream.receive_some(4096) == b""
        upstream_closed.set()

    with trio.fail_after(5):
        origins = await nursery.start(
            partial(trio.serve_tcp, origin, 0, host="127.0.0.1")
        )
        origin_port = origins[0].socket.getsockname()[1]
        async with AsyncExitStack() as resources:
            async with trio.open_nursery() as server_nursery:
                # A distinct loopback address detects an ignored host argument.
                listeners = await server_nursery.start(
                    partial(start_server, 0, host="127.0.0.2")
                )
                host, proxy_port = listeners[0].socket.getsockname()
                assert host == "127.0.0.2"
                assert proxy_port != 0
                client = await resources.enter_async_context(
                    await trio.open_tcp_stream(host, proxy_port)
                )
                await client.send_all(connect_request(origin_port))
                response = bytearray()
                while not response.endswith(b"\r\n\r\n"):
                    chunk = await client.receive_some(4096)
                    assert chunk, "proxy closed before completing the handshake"
                    response.extend(chunk)
                assert response == http.HTTP_200_CONNECTION_ESTABLISHED
                server_nursery.cancel_scope.cancel()
            # Observe server cleanup before the client/fixture contexts exit.
            assert all(listener.socket.fileno() == -1 for listener in listeners)
            assert await client.receive_some(4096) == b""
            await upstream_closed.wait()


async def test_live_tcp_invalid_dns_label_returns_400_and_keeps_server_alive(
    nursery: trio.Nursery, dest_server_port: int
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as bad_client:
        await bad_client.send_all(
            b"CONNECT .example.com:443 HTTP/1.1\r\nHost: .example.com:443\r\n\r\n"
        )
        assert await bad_client.receive_some(4096) == http.HTTP_400_BAD_REQUEST
        assert await bad_client.receive_some(4096) == b""

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as good_client:
        await good_client.send_all(connect_request(dest_server_port))
        assert (
            await good_client.receive_some(4096) == http.HTTP_200_CONNECTION_ESTABLISHED
        )
