"""Upstream ownership must be released when CONNECT setup fails."""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import http_proxy, tunnel


@pytest.mark.parametrize("stage", ["initial_data", "response"])
@pytest.mark.parametrize("cancel", [False, True])
@pytest.mark.parametrize("entrypoint", [tunnel, http_proxy])
@pytest.mark.parametrize("error", [trio.BrokenResourceError, trio.ClosedResourceError])
async def test_failed_handshake_closes_upstream(
    stage: str,
    cancel: bool,
    entrypoint,
    error: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, proxy_client = tt.memory_stream_pair()
    upstream, peer = tt.memory_stream_pair()
    closed = []
    original_close = trio.aclose_forcefully

    async def connect(host, port):
        return upstream

    async def fail_send(data):
        if cancel:
            scope.cancel()
            await trio.lowlevel.checkpoint()
        raise error

    async def close(stream):
        closed.append(stream)
        await original_close(stream)

    monkeypatch.setattr(trio, "aclose_forcefully", close)
    monkeypatch.setattr(
        upstream if stage == "initial_data" else proxy_client, "send_all", fail_send
    )
    async with client, proxy_client, upstream, peer:
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
        assert closed == [upstream]


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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, proxy_client = tt.memory_stream_pair()

    async def fail_send(data):
        raise error

    async def connect(host, port):
        raise OSError("connection refused")

    monkeypatch.setattr(proxy_client, "send_all", fail_send)
    async with client, proxy_client:
        await client.send_all(wire_request)
        await http_proxy(proxy_client, _connect=connect)


@pytest.mark.parametrize("stage", ["initial_data", "response"])
@pytest.mark.parametrize("entrypoint", [tunnel, http_proxy])
async def test_stalled_handshake_closes_upstream(
    stage: str,
    entrypoint,
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock: tt.MockClock,
) -> None:
    client, proxy_client = tt.memory_stream_pair()
    upstream, peer = tt.memory_stream_pair()

    async def connect(host, port):
        return upstream

    async def stall(data):
        await trio.sleep_forever()

    monkeypatch.setattr(
        upstream if stage == "initial_data" else proxy_client, "send_all", stall
    )
    async with client, proxy_client, upstream, peer:
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
        assert await peer.receive_some(4096) == b""


async def test_stalled_error_reply_is_bounded(
    monkeypatch: pytest.MonkeyPatch, autojump_clock: tt.MockClock
) -> None:
    client, proxy_client = tt.memory_stream_pair()

    async def stall(data):
        await trio.sleep_forever()

    monkeypatch.setattr(proxy_client, "send_all", stall)
    async with client, proxy_client:
        await client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
        started = trio.current_time()
        await http_proxy(proxy_client)
        assert trio.current_time() - started == 5
        assert await client.receive_some(4096) == b""
