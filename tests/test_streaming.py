"""Deterministic deadlines, cancellation, and ownership for the micro pipe."""

from collections import deque
from dataclasses import dataclass, field
from math import inf

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import pipe


@dataclass
class TimedStream(trio.abc.HalfCloseableStream):
    """An in-memory stream with independently delayed I/O operations."""

    receive_delay: float = 0
    send_delay: float = 0
    eof_delay: float = 0
    chunks: deque[bytes] = field(default_factory=deque)
    sent: list[bytes] = field(default_factory=list)
    eof_sent: bool = False
    closed: bool = False

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        await trio.sleep(self.receive_delay)
        return self.chunks.popleft() if self.chunks else b""

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await trio.sleep(self.send_delay)
        self.sent.append(bytes(data))

    async def send_eof(self) -> None:
        await trio.sleep(self.eof_delay)
        self.eof_sent = True

    async def wait_send_all_might_not_block(self) -> None:
        await trio.lowlevel.checkpoint()

    async def aclose(self) -> None:
        self.closed = True
        await trio.lowlevel.checkpoint()


@pytest.mark.parametrize("operation", ["receive", "send", "eof"])
@pytest.mark.parametrize("use_defaults", [True, False])
def test_pipe_operation_deadlines(operation: str, use_defaults: bool) -> None:
    """Every blocking phase expires at exactly its own configured deadline."""

    async def scenario() -> None:
        source = TimedStream(
            receive_delay=inf if operation == "receive" else 0,
            chunks=deque([b"data"]),
        )
        destination = TimedStream(
            send_delay=inf if operation == "send" else 0,
            eof_delay=inf if operation == "eof" else 0,
        )
        idle_timeout, send_timeout = (300.0, 30.0) if use_defaults else (7.0, 2.0)
        expected = idle_timeout if operation == "receive" else send_timeout
        with pytest.raises(trio.TooSlowError):
            if use_defaults:
                await pipe(source, destination)
            else:
                await pipe(
                    source,
                    destination,
                    idle_timeout=idle_timeout,
                    send_timeout=send_timeout,
                )
        assert trio.current_time() == expected
        assert not source.closed and not destination.closed
        assert not destination.eof_sent
        assert destination.sent == ([b"data"] if operation == "eof" else [])

    trio.run(scenario, clock=tt.MockClock(autojump_threshold=0))


@pytest.mark.parametrize("operation", ["receive", "send", "eof"])
def test_pipe_outer_cancellation_is_not_a_timeout(operation: str) -> None:
    """Parent cancellation propagates without changing stream ownership."""

    async def scenario() -> None:
        source = TimedStream(
            receive_delay=inf if operation == "receive" else 0,
            chunks=deque([b"data"]),
        )
        destination = TimedStream(
            send_delay=inf if operation == "send" else 0,
            eof_delay=inf if operation == "eof" else 0,
        )
        with trio.move_on_after(1) as parent:
            await pipe(source, destination, idle_timeout=inf, send_timeout=inf)
        assert parent.cancelled_caught
        assert trio.current_time() == 1
        assert not source.closed and not destination.closed
        assert not destination.eof_sent

    trio.run(scenario, clock=tt.MockClock(autojump_threshold=0))


@pytest.mark.parametrize(
    ("idle_timeout", "send_timeout", "receive_delay", "send_delay"),
    [(5.0, 10.0, 4.0, 9.0), (10.0, 5.0, 9.0, 4.0)],
)
def test_pipe_renews_independent_budgets(
    idle_timeout: float, send_timeout: float, receive_delay: float, send_delay: float
) -> None:
    """Long transfers succeed when every operation fits its own fresh budget."""

    async def scenario() -> None:
        chunks = [b"one", b"two", b"three"]
        source = TimedStream(receive_delay=receive_delay, chunks=deque(chunks))
        destination = TimedStream(send_delay=send_delay, eof_delay=send_delay)
        await pipe(
            source, destination, idle_timeout=idle_timeout, send_timeout=send_timeout
        )
        assert destination.sent == chunks
        assert destination.eof_sent
        assert trio.current_time() == 4 * (receive_delay + send_delay)
        assert not source.closed and not destination.closed
        # No finished operation leaves a deadline attached to the caller.
        await trio.sleep(1000)

    trio.run(scenario, clock=tt.MockClock(autojump_threshold=0))
