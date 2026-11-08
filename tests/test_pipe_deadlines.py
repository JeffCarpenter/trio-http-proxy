"""pipe_with_timeout must apply the PipeTimeouts deadlines at the right moments.

PipeTimeouts has sync unit tests; these check that the deadlines it computes are
really set on the cancel scope, using a virtual clock and distinct values.
"""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.streaming import pipe, pipe_with_timeout


class ScriptedSource(trio.abc.ReceiveStream):
    """Yields (delay, data) steps; an empty data step is EOF."""

    def __init__(self, script: list[tuple[float, bytes]]) -> None:
        self.script = list(script)

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        delay, data = self.script.pop(0)
        await trio.sleep(delay)
        return data

    async def aclose(self) -> None:
        await trio.lowlevel.checkpoint()


class RecordingSink(trio.abc.SendStream):
    """Records what it was sent; optionally never completes a send."""

    def __init__(self, *, stall: bool = False) -> None:
        self.stall = stall
        self.received: list[bytes] = []

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self.stall:
            await trio.sleep_forever()
        self.received.append(bytes(data))

    async def wait_send_all_might_not_block(self) -> None:
        await trio.lowlevel.checkpoint()

    async def aclose(self) -> None:
        await trio.lowlevel.checkpoint()


async def test_idle_gap_longer_than_send_timeout_is_not_a_timeout(
    autojump_clock: tt.MockClock,
) -> None:
    """After a completed send the idle deadline applies, not the send one."""
    source = ScriptedSource([(1.0, b"a"), (100.0, b"b"), (1.0, b"")])
    sink = RecordingSink()

    await pipe_with_timeout(source, sink, idle_timeout=300.0, send_timeout=30.0)

    assert sink.received == [b"a", b"b"]


async def test_send_stall_fires_at_the_send_deadline_not_the_idle_one(
    autojump_clock: tt.MockClock,
) -> None:
    """A stalled send times out send_timeout after the receive, not idle_timeout."""
    source = ScriptedSource([(1.0, b"a")])
    started = trio.current_time()

    with pytest.raises(trio.TooSlowError, match="send stalled for 30.0s"):
        await pipe_with_timeout(
            source, RecordingSink(stall=True), idle_timeout=300.0, send_timeout=30.0
        )

    assert trio.current_time() - started == 1.0 + 30.0


async def test_pipe_does_not_call_on_receive_on_eof() -> None:
    """on_receive and on_send fire only for non-empty chunks, never on EOF."""
    events: list[str] = []
    source = ScriptedSource([(0.0, b"first"), (0.0, b"second"), (0.0, b"")])
    sink = RecordingSink()

    await pipe(
        source,
        sink,
        on_receive=lambda: events.append("receive"),
        on_send=lambda: events.append("send"),
    )

    assert sink.received == [b"first", b"second"]
    assert events == ["receive", "send", "receive", "send"]
