"""Live loopback TCP tests for the nano CONNECT server."""

from __future__ import annotations

import socket
from contextlib import AsyncExitStack
from functools import partial

import pytest
import trio
import trio.testing as tt

from tests.conftest import _dest_http_server
from tests.streams import receive_headers, receive_until_eof
from trio_http_proxy import http, start_server


@pytest.mark.parametrize("prefix", [b"G", b"GET /echo HTTP/1.1\r\nHost: localhost\r\n"])
async def test_origin_waits_for_complete_request_headers(
    nursery: trio.Nursery, prefix: bytes
) -> None:
    request = b"GET /echo HTTP/1.1\r\nHost: localhost\r\n\r\n"
    client, server = tt.memory_stream_pair()
    response = bytearray()
    replied = trio.Event()

    async def serve() -> None:
        async with server:
            await _dest_http_server(server)

    async def observe_reply() -> None:
        while chunk := await client.receive_some(4096):
            response.extend(chunk)
        replied.set()

    with trio.fail_after(5):
        async with client:
            nursery.start_soon(serve)
            nursery.start_soon(observe_reply)
            await client.send_all(prefix)
            await tt.wait_all_tasks_blocked()
            assert not replied.is_set(), "origin replied before the headers ended"
            assert not response
            await client.send_all(request[len(prefix) :])
            await replied.wait()
            assert response == (
                b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n"
                b"Connection: close\r\n\r\nGET /echo HTTP/1.1"
            )


@pytest.mark.parametrize("prefix", [b"", b"G", b"GET /echo HTTP/1.1\r\n"])
async def test_origin_does_not_reply_to_incomplete_headers_at_eof(
    nursery: trio.Nursery, prefix: bytes
) -> None:
    client, server = tt.memory_stream_pair()

    async def serve() -> None:
        async with server:
            await _dest_http_server(server)

    with trio.fail_after(5):
        async with client:
            nursery.start_soon(serve)
            if prefix:
                await client.send_all(prefix)
            await client.send_eof()
            assert await client.receive_some(4096) == b""


def connect_request(port: int) -> bytes:
    return (
        f"CONNECT 127.0.0.1:{port} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
    ).encode()


@pytest.mark.parametrize("receive_size", [1, 4096])
@pytest.mark.parametrize("early_request", [False, True])
async def test_live_tcp_round_trip(
    nursery: trio.Nursery,
    dest_server_port: int,
    receive_size: int,
    early_request: bool,
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    assert listeners[0].socket.getsockname()[0] == "127.0.0.1"
    proxy_port = listeners[0].socket.getsockname()[1]

    with trio.fail_after(5):
        async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
            payload = b"GET /echo HTTP/1.1\r\nHost: localhost\r\n\r\n"
            await client.send_all(
                connect_request(dest_server_port) + (payload if early_request else b"")
            )
            if early_request:
                await tt.wait_all_tasks_blocked()
            headers, trailing = await receive_headers(client, max_bytes=receive_size)
            assert headers == http.HTTP_200_CONNECTION_ESTABLISHED
            if not early_request:
                await client.send_all(payload)
            response = await receive_until_eof(
                client, initial=trailing, max_bytes=receive_size
            )
            assert response == (
                b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n"
                b"Connection: close\r\n\r\nGET /echo HTTP/1.1"
            )


@pytest.mark.parametrize("receive_size", [1, 4096])
async def test_live_tcp_concurrent_tunnels(
    nursery: trio.Nursery, dest_server_port: int, receive_size: int
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
            headers, trailing = await receive_headers(client, max_bytes=receive_size)
            assert headers == http.HTTP_200_CONNECTION_ESTABLISHED
            assert trailing == b""
        for index, client in enumerate((first, second)):
            await client.send_all(
                f"GET /{index} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()
            )
            response = await receive_until_eof(client, max_bytes=receive_size)
            assert f"GET /{index} HTTP/1.1".encode() in response


@pytest.mark.parametrize("receive_size", [1, 4096])
async def test_live_tcp_destination_unreachable_returns_502(
    nursery: trio.Nursery, receive_size: int
) -> None:
    # Keep the port reserved without listening so nothing else can claim it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unused:
        unused.bind(("127.0.0.1", 0))
        listeners = await nursery.start(partial(start_server, 0))
        proxy_port = listeners[0].socket.getsockname()[1]
        async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
            await client.send_all(connect_request(unused.getsockname()[1]))
            assert (
                await receive_until_eof(client, max_bytes=receive_size)
                == http.HTTP_502_BAD_GATEWAY
            )


@pytest.mark.parametrize("receive_size", [1, 4096])
async def test_live_tcp_invalid_request_returns_400(
    nursery: trio.Nursery, receive_size: int
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    proxy_port = listeners[0].socket.getsockname()[1]
    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert (
            await receive_until_eof(client, max_bytes=receive_size)
            == http.HTTP_400_BAD_REQUEST
        )


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
                headers, trailing = await receive_headers(client)
                assert headers == http.HTTP_200_CONNECTION_ESTABLISHED
                assert trailing == b""
                server_nursery.cancel_scope.cancel()
            # Observe server cleanup before the client/fixture contexts exit.
            assert all(listener.socket.fileno() == -1 for listener in listeners)
            assert await client.receive_some(4096) == b""
            await upstream_closed.wait()


@pytest.mark.parametrize("receive_size", [1, 4096])
async def test_live_tcp_invalid_dns_label_returns_400_and_keeps_server_alive(
    nursery: trio.Nursery, dest_server_port: int, receive_size: int
) -> None:
    listeners = await nursery.start(partial(start_server, 0))
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as bad_client:
        await bad_client.send_all(
            b"CONNECT .example.com:443 HTTP/1.1\r\nHost: .example.com:443\r\n\r\n"
        )
        assert (
            await receive_until_eof(bad_client, max_bytes=receive_size)
            == http.HTTP_400_BAD_REQUEST
        )

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as good_client:
        await good_client.send_all(connect_request(dest_server_port))
        headers, trailing = await receive_headers(good_client, max_bytes=receive_size)
        assert headers == http.HTTP_200_CONNECTION_ESTABLISHED
        assert trailing == b""
