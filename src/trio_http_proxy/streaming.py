"""Bidirectional stream piping with half-close support and timeout nudging."""

from __future__ import annotations

import logging
from collections.abc import Callable

import trio

from .timeouts import PipeTimeouts

logger = logging.getLogger(__name__)


async def pipe(
    from_stream: trio.abc.ReceiveStream,
    to_stream: trio.abc.SendStream,
    bufmaxsz: int = 8192,
    on_receive: Callable[[], None] | None = None,
    on_send: Callable[[], None] | None = None,
) -> None:
    """Forward chunks from from_stream to to_stream until EOF.

    When on_receive or on_send is given, call it immediately after each
    receive_some() or send_all(). On EOF, call to_stream.send_eof() if
    supported to signal half-close.
    """
    while True:
        chunk = await from_stream.receive_some(bufmaxsz)
        if on_receive is not None:
            on_receive()
        if not chunk:
            break
        await to_stream.send_all(chunk)
        if on_send is not None:
            on_send()
        logger.debug("Forwarded %d bytes", len(chunk))

    if isinstance(to_stream, trio.abc.HalfCloseableStream):
        try:
            await to_stream.send_eof()
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
    logger.debug("Pipe finished")


async def pipe_with_timeout(
    from_stream: trio.abc.ReceiveStream,
    to_stream: trio.abc.SendStream,
    bufmaxsz: int = 8192,
    idle_timeout: float = 300.0,
    send_timeout: float = 30.0,
) -> None:
    """Run pipe(from_stream, to_stream) with idle and send timeouts.

    Nudges a CancelScope deadline on each sign of progress.
    Raises trio.TooSlowError if idle_timeout passes without data or
    send_timeout passes while waiting for to_stream to accept data.
    """
    policy = PipeTimeouts(
        idle_timeout=idle_timeout, send_timeout=send_timeout, now=trio.current_time()
    )

    def on_receive() -> None:
        cancel_scope.deadline = policy.received(trio.current_time())

    def on_send() -> None:
        cancel_scope.deadline = policy.sent(trio.current_time())

    with trio.CancelScope(deadline=policy.deadline) as cancel_scope:
        await pipe(
            from_stream,
            to_stream,
            bufmaxsz=bufmaxsz,
            on_receive=on_receive,
            on_send=on_send,
        )
    if cancel_scope.cancelled_caught:
        logger.warning("Pipe %s for %ss, closing tunnel", policy.reason, policy.timeout)
        raise trio.TooSlowError(f"Pipe {policy.reason} for {policy.timeout}s")


__all__ = [
    "pipe",
    "pipe_with_timeout",
]
