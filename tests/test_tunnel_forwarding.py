"""http_proxy must forward its handshake limits to tunnel, and tunnel must honor them.

Every limit is set to a value that differs from its default and from the other
limits, so a dropped or crossed argument changes the observed deadline or reply.
"""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import http, http_proxy, tunnel

from .streams import RecordingStream

READ_TIMEOUT = 2.0
CONNECT_TIMEOUT = 3.0
SEND_TIMEOUT = 4.0
MAXREQSZ = 100

LIMITS = {
    "read_timeout": READ_TIMEOUT,
    "connect_timeout": CONNECT_TIMEOUT,
    "send_timeout": SEND_TIMEOUT,
    "maxreqsz": MAXREQSZ,
}

CONNECT = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"


class FakeUpstream(trio.abc.Stream):
    """Upstream stream that can stall its writes and records being closed."""

    def __init__(self, *, stall_send: bool) -> None:
        self.stall_send = stall_send
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self.stall_send:
            await trio.sleep_forever()
        await trio.lowlevel.checkpoint()

    async def wait_send_all_might_not_block(self) -> None:
        await trio.lowlevel.checkpoint()

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        await trio.sleep_forever()
        raise AssertionError("unreachable")

    async def aclose(self) -> None:
        self.closed = True
        await trio.lowlevel.checkpoint()


class SlowUpstream(FakeUpstream):
    """Upstream whose writes complete, but only after a fixed delay."""

    def __init__(self, send_delay: float) -> None:
        super().__init__(stall_send=False)
        self.send_delay = send_delay

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await trio.sleep(self.send_delay)


async def read_until_eof(stream: trio.abc.ReceiveStream) -> bytes:
    data = b""
    while chunk := await stream.receive_some(4096):
        data += chunk
    return data


async def test_http_proxy_forwards_read_timeout(autojump_clock: tt.MockClock) -> None:
    client, proxy_client = tt.memory_stream_pair()
    connected = False

    async def connect(host: str, port: int) -> trio.abc.Stream:
        nonlocal connected
        connected = True
        raise AssertionError("must not connect before the request is complete")

    async with client, proxy_client:
        await client.send_all(CONNECT[:-4])  # no terminating blank line
        started = trio.current_time()
        await http_proxy(proxy_client, _connect=connect, **LIMITS)
        assert trio.current_time() - started == READ_TIMEOUT
        assert await read_until_eof(client) == http.HTTP_408_REQUEST_TIMEOUT
    assert not connected


async def test_http_proxy_forwards_maxreqsz() -> None:
    client, proxy_client = tt.memory_stream_pair()
    connected = False

    async def connect(host: str, port: int) -> trio.abc.Stream:
        nonlocal connected
        connected = True
        raise OSError("must not connect for an oversized request")

    oversized = CONNECT[:-2] + b"X-Large-Header: " + b"v" * 150 + b"\r\n\r\n"
    assert len(oversized) > MAXREQSZ
    async with client, proxy_client:
        await client.send_all(oversized)
        await http_proxy(proxy_client, _connect=connect, **LIMITS)
        assert await read_until_eof(client) == http.HTTP_414_URI_TOO_LONG
    assert not connected


async def test_http_proxy_forwards_connect_timeout(
    autojump_clock: tt.MockClock,
) -> None:
    client, proxy_client = tt.memory_stream_pair()

    async def connect(host: str, port: int) -> trio.abc.Stream:
        await trio.sleep_forever()
        raise AssertionError("unreachable")

    async with client, proxy_client:
        await client.send_all(CONNECT)
        started = trio.current_time()
        await http_proxy(proxy_client, _connect=connect, **LIMITS)
        assert trio.current_time() - started == CONNECT_TIMEOUT
        assert await read_until_eof(client) == http.HTTP_504_GATEWAY_TIMEOUT


@pytest.mark.parametrize("stage", ["initial_data", "response"])
@pytest.mark.parametrize("entrypoint", [tunnel, http_proxy])
async def test_handshake_send_timeout_closes_upstream(
    stage: str,
    entrypoint,
    autojump_clock: tt.MockClock,
) -> None:
    client, raw_client = tt.memory_stream_pair()
    upstream = FakeUpstream(stall_send=stage == "initial_data")

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async def stall(data: bytes | bytearray | memoryview) -> None:
        await trio.sleep_forever()

    proxy_client = RecordingStream(
        raw_client, before_send=stall if stage == "response" else None
    )
    async with client, raw_client:
        await client.send_all(CONNECT + b"early bytes")
        started = trio.current_time()
        if entrypoint is tunnel:
            with pytest.raises(trio.TooSlowError):
                await tunnel(proxy_client, _connect=connect, **LIMITS)
        else:
            await http_proxy(proxy_client, _connect=connect, **LIMITS)
        assert trio.current_time() - started == SEND_TIMEOUT
        assert upstream.closed


async def test_handshake_sends_share_one_send_timeout(
    autojump_clock: tt.MockClock,
) -> None:
    """Early bytes upstream and the 200 reply spend one send_timeout together.

    docs/configuration.md promises one shared deadline: 3 s on the early bytes
    leave 1 s of the 4 s budget for the reply. Separate deadlines would allow 7 s.
    """
    client, raw_client = tt.memory_stream_pair()
    upstream = SlowUpstream(send_delay=3.0)

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async def stall(data: bytes | bytearray | memoryview) -> None:
        await trio.sleep_forever()

    proxy_client = RecordingStream(raw_client, before_send=stall)
    async with client, raw_client:
        await client.send_all(CONNECT + b"early bytes")
        started = trio.current_time()
        with pytest.raises(trio.TooSlowError):
            await tunnel(proxy_client, _connect=connect, **LIMITS)
        assert trio.current_time() - started == SEND_TIMEOUT
        assert upstream.closed


async def test_http_proxy_forwards_idle_timeout_to_pipes(
    autojump_clock: tt.MockClock,
) -> None:
    client, proxy_client = tt.memory_stream_pair()
    upstream = FakeUpstream(stall_send=False)

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async with client, proxy_client:
        await client.send_all(CONNECT)
        started = trio.current_time()
        await http_proxy(
            proxy_client, idle_timeout=5.0, send_timeout=SEND_TIMEOUT, _connect=connect
        )
        assert trio.current_time() - started == 5.0


async def test_http_proxy_forwards_send_timeout_to_pipes(
    autojump_clock: tt.MockClock,
) -> None:
    client, proxy_client = tt.memory_stream_pair()
    upstream = FakeUpstream(stall_send=False)

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async def send_late_bytes() -> None:
        await trio.sleep(1.0)
        upstream.stall_send = True
        await client.send_all(b"x")

    async with client, proxy_client, trio.open_nursery() as nursery:
        await client.send_all(CONNECT)
        nursery.start_soon(send_late_bytes)
        started = trio.current_time()
        await http_proxy(
            proxy_client, idle_timeout=50.0, send_timeout=SEND_TIMEOUT, _connect=connect
        )
        # 1 s until the bytes arrive, then the send deadline.
        assert trio.current_time() - started == 1.0 + SEND_TIMEOUT


async def test_http_proxy_forwards_bufmaxsz() -> None:
    client, raw_client = tt.memory_stream_pair()
    raw_dest, dest_peer = tt.memory_stream_pair()
    proxy_client = RecordingStream(raw_client)
    dest_proxy = RecordingStream(raw_dest)

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    async with client, raw_client, raw_dest, dest_peer:
        await client.send_all(CONNECT)
        await dest_peer.send_all(b"reply")
        await client.send_eof()
        await dest_peer.send_eof()
        await http_proxy(proxy_client, bufmaxsz=256, _connect=connect)
        assert (
            await read_until_eof(client)
            == http.HTTP_200_CONNECTION_ESTABLISHED + b"reply"
        )
        assert await dest_peer.receive_some(4096) == b""

    # First read is in process_as_connect_request (via tunnel); subsequent reads are in pipe_with_timeout.
    assert len(proxy_client.read_sizes) >= 2
    assert all(size == 256 for size in proxy_client.read_sizes)
    assert len(dest_proxy.read_sizes) >= 2
    assert all(size == 256 for size in dest_proxy.read_sizes)
