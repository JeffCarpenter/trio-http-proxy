"""Small stream wrappers for observing I/O without replacing dependencies."""

from collections.abc import Awaitable, Callable

import trio


async def receive_headers(
    stream: trio.abc.ReceiveStream, *, max_bytes: int = 4096
) -> tuple[bytes, bytes]:
    """Read a complete HTTP header block and retain any coalesced payload."""
    buffer = bytearray()
    while (end := buffer.find(b"\r\n\r\n")) < 0:
        chunk = await stream.receive_some(max_bytes)
        if not chunk:
            raise EOFError("stream closed before the HTTP headers ended")
        buffer.extend(chunk)
    end += 4
    return bytes(buffer[:end]), bytes(buffer[end:])


async def receive_until_eof(
    stream: trio.abc.ReceiveStream, *, initial: bytes = b"", max_bytes: int = 4096
) -> bytes:
    """Assemble a close-delimited response, including already received bytes."""
    buffer = bytearray(initial)
    while chunk := await stream.receive_some(max_bytes):
        buffer.extend(chunk)
    return bytes(buffer)


class RecordingStream(trio.abc.HalfCloseableStream):
    """Wrap a half-closeable stream; optionally run an injected send operation first.

    Successful operations delegate unchanged, including checkpoints and receive
    limits. The constructor requires the capability advertised by this wrapper.
    """

    def __init__(
        self,
        inner: trio.abc.HalfCloseableStream,
        *,
        before_send: Callable[[bytes | bytearray | memoryview], Awaitable[None]]
        | None = None,
    ) -> None:
        self._inner = inner
        self._before_send = before_send
        self.read_sizes: list[int | None] = []
        self.sent_data: list[bytes] = []
        self.close_calls = 0
        self.cancelled_during_send = False

    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        self.read_sizes.append(max_bytes)
        return await self._inner.receive_some(max_bytes)

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        try:
            if self._before_send is not None:
                await self._before_send(data)
            await self._inner.send_all(data)
            self.sent_data.append(bytes(data))
        except trio.Cancelled:
            self.cancelled_during_send = True
            raise

    async def wait_send_all_might_not_block(self) -> None:
        await self._inner.wait_send_all_might_not_block()

    async def send_eof(self) -> None:
        await self._inner.send_eof()

    async def aclose(self) -> None:
        self.close_calls += 1
        await self._inner.aclose()
