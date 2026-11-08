"""Upstream ownership must be released when CONNECT setup fails."""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import ProxyProtocolError, http, http_proxy, tunnel
from trio_http_proxy.proxy import _reply_error

from .streams import RecordingStream


class FailingReplyStream(trio.abc.SendStream):
    """A real SendStream implementation with an injected transport failure."""

    def __init__(self, error: type[Exception]) -> None:
        self.error = error

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await trio.lowlevel.checkpoint()
        raise self.error("client gone")

    async def wait_send_all_might_not_block(self) -> None:
        await trio.lowlevel.checkpoint()

    async def aclose(self) -> None:
        await trio.lowlevel.checkpoint()


@pytest.mark.parametrize(
    "error", [trio.BrokenResourceError, trio.ClosedResourceError, OSError]
)
async def test_error_reply_suppresses_transport_failure(error: type[Exception]) -> None:
    escaped: Exception | None = None
    try:
        await _reply_error(FailingReplyStream(error), http.HTTP_400_BAD_REQUEST)
    except (trio.BrokenResourceError, trio.ClosedResourceError, OSError) as exc:
        escaped = exc
    assert escaped is None, f"best-effort error reply leaked {escaped!r}"


@pytest.mark.parametrize("stage", ["initial_data", "response"])
@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("entrypoint", [tunnel, http_proxy])
@pytest.mark.parametrize("error", [trio.BrokenResourceError, trio.ClosedResourceError])
async def test_failed_handshake_closes_upstream(
    stage: str,
    cancel: bool,
    entrypoint,
    error: type[Exception],
) -> None:
    client, raw_client = tt.memory_stream_pair()
    raw_upstream, peer = tt.memory_stream_pair()

    async def fail_send(data: bytes | bytearray | memoryview) -> None:
        if cancel:
            scope.cancel()
        await trio.lowlevel.checkpoint()
        raise error("write failed")

    proxy_client = RecordingStream(
        raw_client, before_send=fail_send if stage == "response" else None
    )
    upstream = RecordingStream(
        raw_upstream, before_send=fail_send if stage == "initial_data" else None
    )

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async with client, raw_client, raw_upstream, peer:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
            + (b"tunnel bytes" if stage == "initial_data" else b"")
        )
        with trio.CancelScope() as scope:
            if cancel or entrypoint is http_proxy:
                await entrypoint(proxy_client, _connect=connect)
            else:
                with pytest.raises(error):
                    await entrypoint(proxy_client, _connect=connect)
        assert scope.cancelled_caught == cancel
        # Observe the production close before the raw-stream contexts exit.
        assert upstream.close_calls == 1
        failed_stream = upstream if stage == "initial_data" else proxy_client
        assert failed_stream.cancelled_during_send == cancel
        assert await peer.receive_some(4096) == b""


async def test_successful_handshake_transfers_upstream_ownership() -> None:
    client, raw_client = tt.memory_stream_pair()
    raw_upstream, peer = tt.memory_stream_pair()
    proxy_client = RecordingStream(raw_client)
    upstream = RecordingStream(raw_upstream)

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async with client, raw_client, raw_upstream, peer:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\nearly bytes"
        )
        result = await tunnel(proxy_client, _connect=connect)
        assert result is upstream
        assert upstream.close_calls == 0
        assert upstream.sent_data == [b"early bytes"]
        assert proxy_client.sent_data == [http.HTTP_200_CONNECTION_ESTABLISHED]
        assert await peer.receive_some(4096) == b"early bytes"
        assert await client.receive_some(4096) == http.HTTP_200_CONNECTION_ESTABLISHED
        await result.aclose()
        assert upstream.close_calls == 1
        assert await peer.receive_some(4096) == b""


@pytest.mark.parametrize("error", [trio.BrokenResourceError, trio.ClosedResourceError])
@pytest.mark.parametrize(
    "wire_request",
    [
        b"CONNECT 127.0.0.1:443 HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
    ],
)
async def test_client_reset_during_error_reply_is_connection_local(
    error: type[Exception],
    wire_request: bytes,
) -> None:
    client, raw_client = tt.memory_stream_pair()

    async def fail_send(data: bytes | bytearray | memoryview) -> None:
        await trio.lowlevel.checkpoint()
        raise error("client reset")

    async def connect(host: str, port: int) -> trio.abc.Stream:
        raise OSError("connection refused")

    proxy_client = RecordingStream(raw_client, before_send=fail_send)
    async with client, raw_client:
        await client.send_all(wire_request)
        await http_proxy(proxy_client, _connect=connect)


@pytest.mark.parametrize("stage", ["initial_data", "response"])
@pytest.mark.parametrize("entrypoint", [tunnel, http_proxy])
async def test_stalled_handshake_closes_upstream(
    stage: str,
    entrypoint,
    autojump_clock: tt.MockClock,
) -> None:
    client, raw_client = tt.memory_stream_pair()
    raw_upstream, peer = tt.memory_stream_pair()

    async def stall(data: bytes | bytearray | memoryview) -> None:
        await trio.sleep_forever()

    proxy_client = RecordingStream(
        raw_client, before_send=stall if stage == "response" else None
    )
    upstream = RecordingStream(
        raw_upstream, before_send=stall if stage == "initial_data" else None
    )

    async def connect(host: str, port: int) -> trio.abc.Stream:
        return upstream

    async with client, raw_client, raw_upstream, peer:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
            + (b"early bytes" if stage == "initial_data" else b"")
        )
        started = trio.current_time()
        if entrypoint is tunnel:
            with pytest.raises(trio.TooSlowError):
                await tunnel(proxy_client, _connect=connect)
        else:
            await http_proxy(proxy_client, _connect=connect)
        assert trio.current_time() - started == 30
        assert upstream.close_calls == 1
        stalled_stream = upstream if stage == "initial_data" else proxy_client
        assert stalled_stream.cancelled_during_send
        assert await peer.receive_some(4096) == b""


async def test_stalled_error_reply_is_bounded(autojump_clock: tt.MockClock) -> None:
    client, raw_client = tt.memory_stream_pair()

    async def stall(data: bytes | bytearray | memoryview) -> None:
        await trio.sleep_forever()

    proxy_client = RecordingStream(raw_client, before_send=stall)
    async with client, raw_client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        started = trio.current_time()
        await http_proxy(proxy_client)
        assert trio.current_time() - started == 5
        assert proxy_client.cancelled_during_send
        assert proxy_client.close_calls == 1
        assert await client.receive_some(4096) == b""


@pytest.mark.parametrize(
    "write_error", [trio.BrokenResourceError, trio.ClosedResourceError, OSError]
)
async def test_tunnel_preserves_primary_exception_when_error_reply_fails(
    write_error: type[Exception],
) -> None:
    client, raw_client = tt.memory_stream_pair()

    async def fail_send(data: bytes | bytearray | memoryview) -> None:
        await trio.lowlevel.checkpoint()
        raise write_error("client gone")

    async def fail_connect(host: str, port: int) -> trio.abc.Stream:
        raise OSError("upstream unreachable")

    proxy_client = RecordingStream(raw_client, before_send=fail_send)
    async with client, raw_client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        with pytest.raises(ProxyProtocolError):
            await tunnel(proxy_client, _connect=fail_connect)

    client2, raw_client2 = tt.memory_stream_pair()
    proxy_client2 = RecordingStream(raw_client2, before_send=fail_send)
    async with client2, raw_client2:
        await client2.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
        )
        with pytest.raises(OSError, match="upstream unreachable"):
            await tunnel(proxy_client2, _connect=fail_connect)


async def test_connect_unicode_error_replies_502_and_is_caught_by_http_proxy() -> None:
    client, proxy_client = tt.memory_stream_pair()

    async def bad_idna_connect(host: str, port: int) -> trio.abc.Stream:
        raise UnicodeEncodeError("idna", host, 0, 1, "label empty")

    async with client, proxy_client:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
        )
        await http_proxy(proxy_client, _connect=bad_idna_connect)
        assert await client.receive_some(4096) == http.HTTP_502_BAD_GATEWAY
