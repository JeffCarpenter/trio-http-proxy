"""Comprehensive unit tests for tunnel and http_proxy over memory streams."""

from __future__ import annotations

import random
from functools import partial

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import (
    ProxyProtocolError,
    RequestTooLargeError,
    http,
    http_proxy,
    tunnel,
)

from .streams import RecordingStream, receive_headers


@pytest.fixture
def mock_clock() -> tt.MockClock:
    """Fixture providing an autojump mock clock for time-dependent tests."""
    return tt.MockClock(autojump_threshold=0)


def connect_request(target: bytes = b"example.com:443", extra: bytes = b"") -> bytes:
    """Generate a valid HTTP CONNECT request wire-format byte string."""
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com:443\r\n\r\n" + extra


# ---------------------------------------------------------------------------
# tunnel unit tests
# ---------------------------------------------------------------------------


async def test_tunnel_success_connects_and_replies_200():
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _dest_test_side = tt.memory_stream_pair()
    calls: list[tuple[str, int]] = []

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        calls.append((host, port))
        return dest_proxy_side

    await test_client.send_all(connect_request(target=b"example.com:443"))
    dest_stream = await tunnel(proxy_client, _connect=fake_open_tcp_stream)

    assert calls == [("example.com", 443)]
    assert dest_stream is dest_proxy_side
    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_200_CONNECTION_ESTABLISHED


async def test_tunnel_bad_request_sends_400_and_raises():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        nonlocal called
        called = True
        raise AssertionError("should not connect on bad request")

    await test_client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    with pytest.raises(ProxyProtocolError):
        await tunnel(proxy_client, _connect=fake_open_tcp_stream)

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_400_BAD_REQUEST


async def test_tunnel_ipvfuture_sends_400_without_connecting() -> None:
    proxy_client, test_client = tt.memory_stream_pair()
    upstream, peer = tt.memory_stream_pair()
    calls: list[tuple[str, int]] = []

    async def connect(host: str, port: int) -> trio.abc.Stream:
        calls.append((host, port))
        return upstream

    with trio.fail_after(5):
        async with proxy_client, test_client, upstream, peer:
            await test_client.send_all(connect_request(target=b"[v1.localhost]:443"))
            with pytest.raises(ProxyProtocolError):
                await tunnel(proxy_client, _connect=connect)
            headers, trailing = await receive_headers(test_client, max_bytes=1)
            assert headers == http.HTTP_400_BAD_REQUEST
            assert trailing == b""
            assert calls == []


async def test_tunnel_times_out_reading_connect_sends_408():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        nonlocal called
        called = True
        raise AssertionError("should not connect on timeout")

    with pytest.raises(trio.TooSlowError):
        await tunnel(proxy_client, read_timeout=0.01, _connect=fake_open_tcp_stream)

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_408_REQUEST_TIMEOUT


async def test_tunnel_oversized_request_sends_414():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        nonlocal called
        called = True
        raise AssertionError("should not connect on oversized request")

    oversized = (
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n"
        + b"X-Large-Header: "
        + b"v" * 150
        + b"\r\n\r\n"
    )
    await test_client.send_all(oversized)

    with pytest.raises(RequestTooLargeError):
        await tunnel(proxy_client, maxreqsz=100, _connect=fake_open_tcp_stream)

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_414_URI_TOO_LONG


async def test_tunnel_connect_failure_sends_502_and_raises():
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        raise OSError("connection refused")

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with pytest.raises(OSError):
        await tunnel(proxy_client, _connect=fake_open_tcp_stream)

    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_502_BAD_GATEWAY


async def test_tunnel_times_out_connecting_sends_504():
    proxy_client, test_client = tt.memory_stream_pair()

    async def hanging_connect(host: str, port: int) -> trio.abc.Stream:
        await trio.sleep_forever()

    await test_client.send_all(connect_request(target=b"example.com:443"))

    with pytest.raises(trio.TooSlowError):
        await tunnel(proxy_client, connect_timeout=0.01, _connect=hanging_connect)

    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_504_GATEWAY_TIMEOUT


async def test_tunnel_forwards_early_trailing_data_to_dest():
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy_side

    early_data = b"EARLY_CLIENT_DATA_TLS_CLIENT_HELLO"
    await test_client.send_all(
        connect_request(target=b"example.com:443", extra=early_data)
    )
    dest_stream = await tunnel(proxy_client, _connect=fake_open_tcp_stream)

    assert dest_stream is dest_proxy_side
    reply = await test_client.receive_some(4096)
    assert reply == http.HTTP_200_CONNECTION_ESTABLISHED

    forwarded_trailing = await dest_test_side.receive_some(4096)
    assert forwarded_trailing == early_data


# ---------------------------------------------------------------------------
# http_proxy unit tests
# ---------------------------------------------------------------------------


async def test_http_proxy_full_round_trip():
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy_side

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )

        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == http.HTTP_200_CONNECTION_ESTABLISHED

        await test_client.send_all(b"client->dest payload")
        forwarded = await dest_test_side.receive_some(4096)
        assert forwarded == b"client->dest payload"

        await dest_test_side.send_all(b"dest->client payload")
        forwarded_back = await test_client.receive_some(4096)
        assert forwarded_back == b"dest->client payload"

        await test_client.aclose()
        await dest_test_side.aclose()


async def test_http_proxy_bad_request_sends_400_and_closes():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        nonlocal called
        called = True
        raise AssertionError("should not connect")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(b"not a valid request\r\n\r\n")
        reply = await test_client.receive_some(4096)
        assert reply == http.HTTP_400_BAD_REQUEST

    assert called is False
    await proxy_client.aclose()
    remainder = await test_client.receive_some(4096)
    assert remainder == b""
    await test_client.aclose()


async def test_http_proxy_dest_connect_failure_sends_502_and_closes():
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        raise OSError("no route to host")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == http.HTTP_502_BAD_GATEWAY

    await proxy_client.aclose()
    remainder = await test_client.receive_some(4096)
    assert remainder == b""
    await test_client.aclose()


async def test_http_proxy_forwards_large_payload_across_chunk_boundaries():
    raw_client, test_client = tt.memory_stream_pair()
    raw_upstream, dest_test_side = tt.memory_stream_pair()
    proxy_client = RecordingStream(raw_client)
    dest_proxy_side = RecordingStream(raw_upstream)

    async def fake_open_tcp_stream(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy_side

    payload_size = 8192 * 3 + 777
    client_payload = random.Random(1).randbytes(payload_size)
    dest_payload = random.Random(2).randbytes(payload_size)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )

        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == http.HTTP_200_CONNECTION_ESTABLISHED

        client_reads_before_payload = len(proxy_client.read_sizes)

        await test_client.send_all(client_payload)
        await dest_test_side.send_all(dest_payload)

        received_by_dest = b""
        while len(received_by_dest) < payload_size:
            chunk = await dest_test_side.receive_some(65536)
            if not chunk:
                break
            received_by_dest += chunk

        received_by_client = b""
        while len(received_by_client) < payload_size:
            chunk = await test_client.receive_some(65536)
            if not chunk:
                break
            received_by_client += chunk

        assert received_by_dest == client_payload
        assert received_by_client == dest_payload

        assert len(proxy_client.read_sizes) - client_reads_before_payload >= 4
        assert len(dest_proxy_side.read_sizes) >= 4

        await test_client.aclose()
        await dest_test_side.aclose()


async def test_http_proxy_handles_concurrent_connections_without_cross_talk():
    conns = []
    for i in range(5):
        proxy_client, test_client = tt.memory_stream_pair()
        dest_proxy_side, dest_test_side = tt.memory_stream_pair()
        conns.append((i, proxy_client, test_client, dest_proxy_side, dest_test_side))

    def fake_open_tcp_stream_for(dest_proxy_side: trio.abc.Stream):
        async def _connect(host: str, port: int) -> trio.abc.Stream:
            return dest_proxy_side

        return _connect

    async with trio.open_nursery() as nursery:
        for i, proxy_client, _test_client, dest_proxy_side, _dest_test_side in conns:
            connect = fake_open_tcp_stream_for(dest_proxy_side)
            nursery.start_soon(partial(http_proxy, proxy_client, _connect=connect))

        for i, _proxy_client, test_client, _dest_proxy_side, _dest_test_side in conns:
            await test_client.send_all(
                connect_request(target=f"dest{i}.example.com:443".encode())
            )
            reply = await test_client.receive_some(4096)
            assert reply == http.HTTP_200_CONNECTION_ESTABLISHED

        for i, _proxy_client, test_client, _dest_proxy_side, _dest_test_side in conns:
            await test_client.send_all(f"payload-for-conn-{i}".encode())

        for i, _proxy_client, _test_client, _dest_proxy_side, dest_test_side in conns:
            received = await dest_test_side.receive_some(4096)
            assert received == f"payload-for-conn-{i}".encode()

        for i, _proxy_client, _test_client, _dest_proxy_side, dest_test_side in conns:
            await dest_test_side.send_all(f"reply-for-conn-{i}".encode())

        for i, _proxy_client, test_client, _dest_proxy_side, _dest_test_side in conns:
            received = await test_client.receive_some(4096)
            assert received == f"reply-for-conn-{i}".encode()

        for _i, _proxy_client, test_client, _dest_proxy_side, dest_test_side in conns:
            await test_client.aclose()
            await dest_test_side.aclose()
