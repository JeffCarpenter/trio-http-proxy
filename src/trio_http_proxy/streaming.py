"""Bounded stream forwarding with half-close support."""

from __future__ import annotations

import trio


async def pipe(
    from_stream: trio.abc.ReceiveStream,
    to_stream: trio.abc.SendStream,
    bufmaxsz: int = 8192,
    *,
    idle_timeout: float = 300.0,
    send_timeout: float = 30.0,
) -> None:
    """Forward chunks until EOF, then half-close the destination if supported.

    Each receive gets idle_timeout seconds; each send (including send_eof)
    gets send_timeout seconds. Expiry raises trio.TooSlowError. The caller
    owns both streams and remains responsible for closing them.
    """
    while True:
        with trio.fail_after(idle_timeout):
            chunk = await from_stream.receive_some(bufmaxsz)
        if not chunk:
            break
        with trio.fail_after(send_timeout):
            await to_stream.send_all(chunk)

    if isinstance(to_stream, trio.abc.HalfCloseableStream):
        with trio.fail_after(send_timeout):
            try:
                await to_stream.send_eof()
            except trio.BrokenResourceError, trio.ClosedResourceError:
                pass


__all__ = ["pipe"]
