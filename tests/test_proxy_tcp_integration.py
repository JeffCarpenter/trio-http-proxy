"""Live loopback TCP integration tests for minimal Trio HTTP proxy."""

from __future__ import annotations

import socket
from functools import partial

import pytest
import trio
import trio.testing

from trio_http_proxy import (
    ProxyConfig,
    get_conn_limiter,
    http,
    http_proxy,
    reset_conn_limiter,
)


def _get_unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.trio
async def test_live_tcp_round_trip(
    nursery: trio.Nursery,
    dest_server_port: int,
) -> None:
    """Verify live TCP CONNECT round-trip through proxy to mock origin."""
    cfg = ProxyConfig(connect_read_timeout=5.0, dest_connect_timeout=5.0)
    listeners = await nursery.start(
        partial(trio.serve_tcp, partial(http_proxy, config=cfg), 0)
    )
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        connect_req = (
            f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)

        resp = await client.receive_some(4096)
        assert resp == http.HTTP_200_CONNECTION_ESTABLISHED

        # Send request payload through established tunnel
        req_line = b"GET /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        await client.send_all(req_line)

        # Receive echo response from dest_server_port
        echo_resp = await client.receive_some(4096)
        assert echo_resp.startswith(b"HTTP/1.1 200 OK\r\n")
        assert b"GET /echo HTTP/1.1" in echo_resp


@pytest.mark.trio
async def test_concurrency_limiting_backpressure(
    nursery: trio.Nursery,
    dest_server_port: int,
) -> None:
    """With maxconns=1, verify second connection is queued by CapacityLimiter until first completes."""
    reset_conn_limiter()
    cfg = ProxyConfig(maxconns=1)
    limiter = get_conn_limiter(cfg.maxconns)

    listeners = await nursery.start(
        partial(trio.serve_tcp, partial(http_proxy, config=cfg, limiter=limiter), 0)
    )
    proxy_port = listeners[0].socket.getsockname()[1]

    # Connection 1 connects and establishes tunnel
    client1 = await trio.open_tcp_stream("127.0.0.1", proxy_port)
    connect_req1 = (
        f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
    ).encode()
    await client1.send_all(connect_req1)
    resp1 = await client1.receive_some(4096)
    assert resp1 == http.HTTP_200_CONNECTION_ESTABLISHED

    # Connection 1 is actively holding the single CapacityLimiter token
    assert limiter.statistics().borrowed_tokens == 1
    assert limiter.statistics().tasks_waiting == 0

    # Connection 2 connects concurrently to the proxy
    client2 = await trio.open_tcp_stream("127.0.0.1", proxy_port)
    connect_req2 = (
        f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
    ).encode()
    await client2.send_all(connect_req2)

    # Allow all tasks to run until blocked; client2 must queue on CapacityLimiter
    await trio.testing.wait_all_tasks_blocked()
    assert limiter.statistics().borrowed_tokens == 1
    assert limiter.statistics().tasks_waiting == 1

    # Verify client2 receives no response while queued
    with trio.move_on_after(0.05):
        data = await client2.receive_some(4096)
        assert False, f"Client 2 received unexpected data while queued: {data!r}"

    # Connection 1 sends its payload, reads response, and closes
    await client1.send_all(b"GET /client1 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    echo1 = await client1.receive_some(4096)
    assert b"GET /client1 HTTP/1.1" in echo1
    await client1.aclose()

    # Now Connection 2 is unblocked and completes handshake
    resp2 = await client2.receive_some(4096)
    assert resp2 == http.HTTP_200_CONNECTION_ESTABLISHED
    assert limiter.statistics().borrowed_tokens == 1
    assert limiter.statistics().tasks_waiting == 0

    # Connection 2 sends its payload and completes
    await client2.send_all(b"GET /client2 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    echo2 = await client2.receive_some(4096)
    assert b"GET /client2 HTTP/1.1" in echo2
    await client2.aclose()

    await trio.testing.wait_all_tasks_blocked()
    assert limiter.statistics().borrowed_tokens == 0
    assert limiter.statistics().tasks_waiting == 0
    reset_conn_limiter()


@pytest.mark.trio
async def test_live_tcp_destination_unreachable_returns_502(
    nursery: trio.Nursery,
) -> None:
    """Connecting to an unreachable origin port returns 502 Bad Gateway."""
    unused_port = _get_unused_port()
    cfg = ProxyConfig(dest_connect_timeout=2.0)
    listeners = await nursery.start(
        partial(trio.serve_tcp, partial(http_proxy, config=cfg), 0)
    )
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        connect_req = (
            f"CONNECT 127.0.0.1:{unused_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{unused_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_502_BAD_GATEWAY


@pytest.mark.trio
async def test_live_tcp_invalid_request_returns_400(
    nursery: trio.Nursery,
) -> None:
    """Sending non-CONNECT HTTP request returns 400 Bad Request."""
    cfg = ProxyConfig()
    listeners = await nursery.start(
        partial(trio.serve_tcp, partial(http_proxy, config=cfg), 0)
    )
    proxy_port = listeners[0].socket.getsockname()[1]

    async with await trio.open_tcp_stream("127.0.0.1", proxy_port) as client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_400_BAD_REQUEST
