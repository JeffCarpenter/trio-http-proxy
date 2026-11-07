"""Vetted-address racing must preserve liveness and close losing streams."""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.proxy import _connect_to_addresses


async def test_stalled_first_address_does_not_starve_second(
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock,
) -> None:
    upstream, peer = tt.memory_stream_pair()
    cancelled = trio.Event()
    calls = []

    async def connect(address, port):
        calls.append((address, port, trio.current_time()))
        if address == "2606:4700:4700::1111":
            try:
                await trio.sleep_forever()
            finally:
                cancelled.set()
        return upstream

    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    started = trio.current_time()
    async with upstream, peer:
        assert (
            await _connect_to_addresses(("2606:4700:4700::1111", "1.1.1.1"), 443)
            is upstream
        )
    assert cancelled.is_set()
    assert calls == [
        ("2606:4700:4700::1111", 443, started),
        ("1.1.1.1", 443, started + 0.25),
    ]


async def test_failed_first_address_starts_fallback_immediately(
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock,
) -> None:
    upstream, peer = tt.memory_stream_pair()

    async def connect(address, port):
        if address == "8.8.8.8":
            raise OSError("Unavailable")
        return upstream

    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    started = trio.current_time()
    async with upstream, peer:
        assert await _connect_to_addresses(("8.8.8.8", "1.1.1.1"), 443) is upstream
    assert trio.current_time() == started


async def test_simultaneous_success_closes_losing_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, first_peer = tt.memory_stream_pair()
    second, second_peer = tt.memory_stream_pair()
    gate = trio.Event()
    opened = []
    closed = []
    original_close = trio.aclose_forcefully

    async def connect(address, port):
        opened.append(address)
        if len(opened) == 2:
            gate.set()
        # Deliberately return even under cancellation to exercise loser cleanup.
        with trio.CancelScope(shield=True):
            await gate.wait()
        return first if address == "8.8.8.8" else second

    async def close(stream):
        closed.append(stream)
        await original_close(stream)

    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    monkeypatch.setattr(trio, "aclose_forcefully", close)
    async with first, first_peer, second, second_peer:
        with trio.fail_after(2):
            winner = await _connect_to_addresses(("8.8.8.8", "1.1.1.1"), 443)
        assert winner in (first, second)
        assert len(closed) == 1
        assert closed[0] is not winner


async def test_parent_cancellation_closes_acquired_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    upstream, peer = tt.memory_stream_pair()
    closed = []
    original_close = trio.aclose_forcefully

    async def connect(address, port):
        parent.cancel()
        return upstream

    async def close(stream):
        closed.append(stream)
        await original_close(stream)

    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    monkeypatch.setattr(trio, "aclose_forcefully", close)
    async with upstream, peer:
        with trio.CancelScope() as parent:
            await _connect_to_addresses(("8.8.8.8",), 443)
        assert parent.cancelled_caught
        assert closed == [upstream]
