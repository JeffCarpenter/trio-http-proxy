"""Small stream wrappers for observing I/O without replacing dependencies."""

from collections.abc import Awaitable, Callable

import trio


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
