"""Structured concurrency invariant tests for the minimal Trio HTTP CONNECT proxy.

Invariants verified:
1. Full-duplex half-close:
   When client sends EOF (b""), the proxy calls send_eof() on dest_stream if
   HalfCloseableStream, and dest_stream can still send remaining data back to client
   until dest also hits EOF.
2. Idle receive timeout:
   pipe_with_timeout raises trio.TooSlowError if no data is received within idle_timeout.
3. Stalled send timeout:
   pipe_with_timeout raises trio.TooSlowError if to_stream blocks on send beyond send_timeout.
4. Sibling cancellation:
   Inside http_proxy, when one direction fails or times out, the shared nursery
   immediately cancels the sibling task and cleanly closes both streams without leaked tasks.
"""

from __future__ import annotations

from functools import partial
from types import TracebackType
from typing import Self

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.config import ProxyConfig
from trio_http_proxy.proxy import http_proxy
from trio_http_proxy.streaming import pipe, pipe_with_timeout

# ---------------------------------------------------------------------------
# Test Stream Helpers
# ---------------------------------------------------------------------------


class NonHalfCloseableSendStream(trio.abc.SendStream):
    """A SendStream that intentionally does not implement HalfCloseableStream."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        self.chunks.append(bytes(data))

    async def wait_send_all_might_not_block(self) -> None:
        pass

    async def aclose(self) -> None:
        self.closed = True


class FaultyEofSendStream(trio.abc.HalfCloseableStream):
    """A HalfCloseableStream whose send_eof() raises an error."""

    def __init__(self, exc_to_raise: type[Exception]) -> None:
        self.exc_to_raise = exc_to_raise
        self.send_eof_called = False
        self.sent: list[bytes] = []
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        self.sent.append(bytes(data))

    async def wait_send_all_might_not_block(self) -> None:
        pass

    async def send_eof(self) -> None:
        self.send_eof_called = True
        raise self.exc_to_raise("send_eof failure")

    async def receive_some(self, max_bytes: int | None = 4096) -> bytes:
        return b""

    async def aclose(self) -> None:
        self.closed = True


class StalledSendStream(trio.abc.SendStream):
    """A SendStream whose send_all blocks forever unless unblocked."""

    def __init__(self) -> None:
        self.event = trio.Event()
        self.closed = False

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await self.event.wait()

    async def wait_send_all_might_not_block(self) -> None:
        await self.event.wait()

    async def aclose(self) -> None:
        self.closed = True


class TrackedStream(trio.abc.HalfCloseableStream):
    """Stream wrapper that monitors method calls, cancellation, and cleanup."""

    def __init__(self, inner: trio.abc.Stream) -> None:
        self.inner = inner
        self.closed = False
        self.cancelled_in_receive = False
        self.cancelled_in_send = False
        self.send_eof_called = False
        self.fail_on_receive: Exception | None = None
        self.fail_on_send: Exception | None = None

    async def receive_some(self, max_bytes: int | None = 4096) -> bytes | bytearray:
        if self.fail_on_receive is not None:
            exc = self.fail_on_receive
            self.fail_on_receive = None
            raise exc
        try:
            return await self.inner.receive_some(max_bytes)
        except trio.Cancelled:
            self.cancelled_in_receive = True
            raise

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self.fail_on_send is not None:
            exc = self.fail_on_send
            self.fail_on_send = None
            raise exc
        try:
            await self.inner.send_all(data)
        except trio.Cancelled:
            self.cancelled_in_send = True
            raise

    async def wait_send_all_might_not_block(self) -> None:
        return await self.inner.wait_send_all_might_not_block()

    async def send_eof(self) -> None:
        self.send_eof_called = True
        if isinstance(self.inner, trio.abc.HalfCloseableStream):
            await self.inner.send_eof()

    async def aclose(self) -> None:
        self.closed = True
        await self.inner.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


def make_connect_request(host: str = "example.com", port: int = 443) -> bytes:
    """Format an RFC 7231 CONNECT request line with Host header."""
    return f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode()


# ---------------------------------------------------------------------------
# Invariant 1: Full-Duplex Half-Close
# ---------------------------------------------------------------------------


async def test_pipe_half_close_calls_send_eof_on_half_closeable_stream() -> None:
    """pipe() must call send_eof() on to_stream when from_stream reaches EOF."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream, to_peer = tt.memory_stream_pair()

    await from_tx.send_all(b"payload")
    await from_tx.send_eof()

    await pipe(from_rx, to_stream)

    assert await to_peer.receive_some(4096) == b"payload"
    # Counterpart receives EOF because send_eof() was invoked on to_stream
    assert await to_peer.receive_some(4096) == b""


async def test_pipe_non_half_closeable_stream_clean_exit() -> None:
    """pipe() ignores send_eof() if to_stream is not a HalfCloseableStream."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream = NonHalfCloseableSendStream()

    await from_tx.send_all(b"data chunk")
    await from_tx.send_eof()

    await pipe(from_rx, to_stream)
    assert to_stream.chunks == [b"data chunk"]


@pytest.mark.parametrize(
    "exc_type", [trio.BrokenResourceError, trio.ClosedResourceError]
)
async def test_pipe_send_eof_broken_resource_suppressed(
    exc_type: type[Exception],
) -> None:
    """pipe() suppresses BrokenResourceError and ClosedResourceError on send_eof()."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream = FaultyEofSendStream(exc_type)

    await from_tx.send_all(b"data")
    await from_tx.send_eof()

    # Should exit cleanly without propagating the exception
    await pipe(from_rx, to_stream)
    assert to_stream.send_eof_called is True


async def test_full_duplex_half_close_client_eof_dest_continues() -> None:
    """Client sends EOF; dest receives EOF, then sends remaining data back to client."""
    client_client, client_proxy = tt.memory_stream_pair()
    dest_proxy, dest_server = tt.memory_stream_pair()

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=ProxyConfig(),
                _connect=fake_connect,
            )
        )

        # 1. Client completes CONNECT handshake
        await client_client.send_all(make_connect_request())
        handshake_resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in handshake_resp

        # 2. Client sends outbound data
        await client_client.send_all(b"CLIENT_DATA_1")
        assert await dest_server.receive_some(1024) == b"CLIENT_DATA_1"

        # 3. Client sends half-close EOF
        await client_client.send_eof()

        # 4. Proxy forwards EOF to dest_server
        assert await dest_server.receive_some(1024) == b""

        # 5. Dest server can still stream multiple chunks back to client
        await dest_server.send_all(b"DEST_RESPONSE_CHUNK_1")
        assert await client_client.receive_some(1024) == b"DEST_RESPONSE_CHUNK_1"

        await dest_server.send_all(b"DEST_RESPONSE_CHUNK_2")
        assert await client_client.receive_some(1024) == b"DEST_RESPONSE_CHUNK_2"

        # 6. Dest server finally closes its send direction
        await dest_server.send_eof()
        assert await client_client.receive_some(1024) == b""

    # Nursery completed cleanly with all tasks exited


async def test_full_duplex_half_close_dest_eof_client_continues() -> None:
    """Dest sends EOF first; client receives EOF, then sends remaining data back to dest."""
    client_client, client_proxy = tt.memory_stream_pair()
    dest_proxy, dest_server = tt.memory_stream_pair()

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=ProxyConfig(),
                _connect=fake_connect,
            )
        )

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

        # Dest sends first response, then sends EOF
        await dest_server.send_all(b"INITIAL_SERVER_GREETING")
        assert await client_client.receive_some(1024) == b"INITIAL_SERVER_GREETING"
        await dest_server.send_eof()

        # Client receives EOF on read side
        assert await client_client.receive_some(1024) == b""

        # Client can still transmit remaining data to dest
        await client_client.send_all(b"CLIENT_FINAL_CHUNK")
        assert await dest_server.receive_some(1024) == b"CLIENT_FINAL_CHUNK"

        # Client finally sends EOF
        await client_client.send_eof()
        assert await dest_server.receive_some(1024) == b""


# ---------------------------------------------------------------------------
# Invariant 2: Idle Receive Timeout
# ---------------------------------------------------------------------------


async def test_pipe_idle_receive_timeout_initial() -> None:
    """pipe_with_timeout raises trio.TooSlowError if no data is received within idle_timeout."""
    _from_tx, from_rx = tt.memory_stream_pair()
    to_stream, _to_peer = tt.memory_stream_pair()

    with pytest.raises(trio.TooSlowError, match="Pipe idle for 0.05s"):
        await pipe_with_timeout(from_rx, to_stream, idle_timeout=0.05, send_timeout=1.0)


async def test_pipe_idle_receive_timeout_after_data() -> None:
    """pipe_with_timeout raises trio.TooSlowError if connection goes idle after initial data."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream, to_peer = tt.memory_stream_pair()

    async with trio.open_nursery() as nursery:

        async def feeder() -> None:
            await from_tx.send_all(b"first chunk")
            # Then pause and don't send anything more
            await trio.sleep(0.2)

        nursery.start_soon(feeder)

        with pytest.raises(trio.TooSlowError, match="Pipe idle for 0.05s"):
            await pipe_with_timeout(
                from_rx, to_stream, idle_timeout=0.05, send_timeout=1.0
            )

        nursery.cancel_scope.cancel()

    assert await to_peer.receive_some(1024) == b"first chunk"


async def test_pipe_idle_timeout_progress_resets_deadline() -> None:
    """Regular progress resets the idle deadline, allowing long transfers exceeding idle_timeout."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream, to_peer = tt.memory_stream_pair()

    # Total transfer time will be ~0.08s with idle_timeout=0.05s
    async with trio.open_nursery() as nursery:

        async def slow_feeder() -> None:
            for i in range(4):
                await trio.sleep(0.02)
                await from_tx.send_all(f"chunk{i}".encode())
            await from_tx.send_eof()

        nursery.start_soon(slow_feeder)

        # Should NOT raise TooSlowError because each receive resets the deadline
        await pipe_with_timeout(from_rx, to_stream, idle_timeout=0.05, send_timeout=1.0)

    received = b""
    while chunk := await to_peer.receive_some(1024):
        received += chunk
    assert received == b"chunk0chunk1chunk2chunk3"


def test_pipe_idle_receive_timeout_mock_clock() -> None:
    """Virtual clock test verifying pipe_with_timeout idle deadline jumping."""
    clock = tt.MockClock(autojump_threshold=0)

    async def scenario() -> None:
        _from_tx, from_rx = tt.memory_stream_pair()
        to_stream, _to_peer = tt.memory_stream_pair()

        with pytest.raises(trio.TooSlowError, match="Pipe idle for 300.0s"):
            await pipe_with_timeout(
                from_rx,
                to_stream,
                idle_timeout=300.0,
                send_timeout=30.0,
            )

    trio.run(scenario, clock=clock)


# ---------------------------------------------------------------------------
# Invariant 3: Stalled Send Timeout
# ---------------------------------------------------------------------------


async def test_pipe_stalled_send_timeout() -> None:
    """pipe_with_timeout raises trio.TooSlowError if to_stream blocks beyond send_timeout."""
    from_tx, from_rx = tt.memory_stream_pair()
    stalled_stream = StalledSendStream()

    await from_tx.send_all(b"incoming data")

    with pytest.raises(trio.TooSlowError, match="Pipe send stalled for 0.05s"):
        await pipe_with_timeout(
            from_rx,
            stalled_stream,
            idle_timeout=10.0,
            send_timeout=0.05,
        )


async def test_pipe_send_progress_resets_deadline_to_idle() -> None:
    """Completing a send switches phase back to idle and applies idle_timeout."""
    from_tx, from_rx = tt.memory_stream_pair()
    to_stream, to_peer = tt.memory_stream_pair()

    await from_tx.send_all(b"payload")

    async with trio.open_nursery() as nursery:

        async def check_peer() -> None:
            chunk = await to_peer.receive_some(1024)
            assert chunk == b"payload"
            # Once sent, the pipe is waiting for receive with idle_timeout=0.05s
            # We don't send any more data, so it should time out in idle phase
            await trio.sleep(0.1)

        nursery.start_soon(check_peer)

        with pytest.raises(trio.TooSlowError, match="Pipe idle for 0.05s"):
            await pipe_with_timeout(
                from_rx,
                to_stream,
                idle_timeout=0.05,
                send_timeout=1.0,
            )


def test_pipe_stalled_send_timeout_mock_clock() -> None:
    """Virtual clock test verifying pipe_with_timeout stalled send jumping."""
    clock = tt.MockClock(autojump_threshold=0)

    async def scenario() -> None:
        from_tx, from_rx = tt.memory_stream_pair()
        stalled_stream = StalledSendStream()

        await from_tx.send_all(b"immediate data")

        with pytest.raises(trio.TooSlowError, match="Pipe send stalled for 30.0s"):
            await pipe_with_timeout(
                from_rx,
                stalled_stream,
                idle_timeout=300.0,
                send_timeout=30.0,
            )

    trio.run(scenario, clock=clock)


# ---------------------------------------------------------------------------
# Invariant 4: Sibling Cancellation and Clean Stream Closure
# ---------------------------------------------------------------------------


async def test_sibling_cancelled_when_client_fails() -> None:
    """When client-to-dest fails, shared nursery cancels dest-to-client and closes dest_stream."""
    client_client, client_proxy_raw = tt.memory_stream_pair()
    dest_proxy_raw, _dest_server = tt.memory_stream_pair()

    client_proxy = TrackedStream(client_proxy_raw)
    dest_proxy = TrackedStream(dest_proxy_raw)

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    cfg = ProxyConfig(pipe_idle_timeout=0.1, pipe_send_timeout=0.1)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=cfg,
                _connect=fake_connect,
            )
        )

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

        # Inject an error into client_proxy to fail client-to-dest
        client_proxy.fail_on_receive = trio.BrokenResourceError("client broke")
        await client_client.send_all(b"trigger client failure")

    # The nursery exited cleanly.
    # Verify dest_stream was cleanly closed by http_proxy async with block
    assert dest_proxy.closed is True
    # Verify dest_to_client was cancelled while waiting on dest_proxy.receive_some()
    assert dest_proxy.cancelled_in_receive is True


async def test_sibling_cancelled_when_dest_fails() -> None:
    """When dest-to-client fails, shared nursery cancels client-to-dest and closes dest_stream."""
    client_client, client_proxy_raw = tt.memory_stream_pair()
    dest_proxy_raw, dest_server = tt.memory_stream_pair()

    client_proxy = TrackedStream(client_proxy_raw)
    dest_proxy = TrackedStream(dest_proxy_raw)

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    cfg = ProxyConfig(pipe_idle_timeout=0.1, pipe_send_timeout=0.1)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=cfg,
                _connect=fake_connect,
            )
        )

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

        # Inject error into dest_proxy to fail dest-to-client
        dest_proxy.fail_on_receive = trio.BrokenResourceError("dest broke")
        await dest_server.send_all(b"trigger dest failure")

    # http_proxy exits cleanly
    assert dest_proxy.closed is True
    assert client_proxy.cancelled_in_receive is True


async def test_sibling_cancelled_on_idle_timeout() -> None:
    """When one direction times out, the sibling task is cancelled and dest_stream closed."""
    client_client, client_proxy_raw = tt.memory_stream_pair()
    dest_proxy_raw, _dest_server = tt.memory_stream_pair()

    client_proxy = TrackedStream(client_proxy_raw)
    dest_proxy = TrackedStream(dest_proxy_raw)

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    cfg = ProxyConfig(pipe_idle_timeout=0.05, pipe_send_timeout=1.0)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=cfg,
                _connect=fake_connect,
            )
        )

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

        # Neither client nor dest sends anything, so idle timeout triggers

    # Verify dest_stream closed and sibling task received cancellation
    assert dest_proxy.closed is True
    assert (
        client_proxy.cancelled_in_receive or dest_proxy.cancelled_in_receive
    ) is True


async def test_sibling_cancelled_on_stalled_send_timeout() -> None:
    """When a send stalls beyond send_timeout, sibling task is cancelled and dest_stream closed."""
    client_client, client_proxy_raw = tt.memory_stream_pair()
    dest_send = StalledSendStream()
    _dest_tx, dest_rx = tt.memory_stream_pair()

    class StalledDestStream(trio.abc.HalfCloseableStream):
        def __init__(self) -> None:
            self.cancelled_in_receive = False

        async def send_all(self, data: bytes | bytearray | memoryview) -> None:
            await dest_send.send_all(data)

        async def wait_send_all_might_not_block(self) -> None:
            await dest_send.wait_send_all_might_not_block()

        async def send_eof(self) -> None:
            pass

        async def receive_some(self, max_bytes: int | None = 4096) -> bytes | bytearray:
            try:
                return await dest_rx.receive_some(max_bytes)
            except trio.Cancelled:
                self.cancelled_in_receive = True
                raise

        async def aclose(self) -> None:
            await dest_send.aclose()
            await dest_rx.aclose()

    stalled_dest = StalledDestStream()
    client_proxy = TrackedStream(client_proxy_raw)

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return stalled_dest

    cfg = ProxyConfig(pipe_idle_timeout=10.0, pipe_send_timeout=0.05)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_proxy,
                config=cfg,
                _connect=fake_connect,
            )
        )

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

        # Client sends data to dest; dest stalls on send_all
        await client_client.send_all(b"some payload to dest")

    # Sibling task (dest -> client) was cancelled while waiting in dest.receive_some()
    assert stalled_dest.cancelled_in_receive is True
    assert dest_send.closed is True


async def test_both_streams_cleanly_closed_under_server_lifecycle() -> None:
    """In standard server lifecycle, both streams are closed without leaked tasks on failure."""
    client_client, client_proxy_raw = tt.memory_stream_pair()
    dest_proxy_raw, _dest_server = tt.memory_stream_pair()

    client_proxy = TrackedStream(client_proxy_raw)
    dest_proxy = TrackedStream(dest_proxy_raw)

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        return dest_proxy

    cfg = ProxyConfig(pipe_idle_timeout=0.05, pipe_send_timeout=1.0)

    # Mimic serve_tcp pattern: async with client_stream: await handler(client_stream)
    async def simulated_server_accept() -> None:
        async with client_proxy:
            await http_proxy(client_proxy, config=cfg, _connect=fake_connect)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(simulated_server_accept)

        await client_client.send_all(make_connect_request())
        resp = await client_client.receive_some(1024)
        assert b"200 Connection established" in resp

    # Both streams must be closed
    assert client_proxy.closed is True
    assert dest_proxy.closed is True
