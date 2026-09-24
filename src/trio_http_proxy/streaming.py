# Copyright 2018-2019 Joshua Bronson.    All Rights Reserved.
# Copyright 2026      Jeffrey Carpenter.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import annotations

import logging
from collections.abc import Callable

import trio

from .config import Settings, resolve_settings
from .connection import (
    CV_PIPE_FROM,
    ConnectionContext,
    ConnectionFilter,
    ConnectionLoggerAdapter,
)

LOGGER_NAME = "trio_http_proxy"
_raw_logger = logging.getLogger(LOGGER_NAME)
if not any(isinstance(f, ConnectionFilter) for f in _raw_logger.filters):
    _raw_logger.addFilter(ConnectionFilter())
logger = ConnectionLoggerAdapter(_raw_logger, {})


async def pipe(
    from_stream: trio.abc.ReceiveStream,
    to_stream: trio.abc.SendStream,
    bufmaxsz: int | None = None,
    on_receive: Callable[[], None] | None = None,
    on_send: Callable[[], None] | None = None,
    *,
    settings: Settings | None = None,
    context: ConnectionContext | None = None,
) -> None:
    """Forward chunks from from_stream to to_stream until EOF.
    on_receive and on_send, when given, are called with no arguments right
    after each successful receive_some()/send_all(). pipe() itself knows
    nothing about timeouts; a caller can use these hooks to implement one
    (see pipe_with_timeout) by nudging a CancelScope's deadline forward on
    every sign of progress -- Trio's recommended pattern for timeouts,
    which keeps a reusable function like this one oblivious to them.
    On EOF, calls to_stream.send_eof() if supported to signal half-close.
    """
    if bufmaxsz is None:
        cfg = resolve_settings(settings, context)
        bufmaxsz = cfg.bufmaxsz
    if context is not None:
        context.set_pipe_from(from_stream)
    else:
        CV_PIPE_FROM.set(from_stream)
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
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            pass
    logger.debug("Pipe finished")


async def pipe_with_timeout(
    from_stream: trio.abc.ReceiveStream,
    to_stream: trio.abc.SendStream,
    bufmaxsz: int | None = None,
    idle_timeout: float | None = None,
    send_timeout: float | None = None,
    *,
    settings: Settings | None = None,
    context: ConnectionContext | None = None,
) -> None:
    """Run pipe(from_stream, to_stream), raising trio.TooSlowError if
    idle_timeout seconds pass with no data received, or send_timeout
    seconds pass with to_stream not accepting a chunk already read --
    guards against a slowloris-style peer that opens a tunnel and then goes
    silent (or stops draining) forever. Owns a CancelScope here, in the
    caller, and nudges its deadline from pipe()'s on_receive/on_send hooks
    rather than making pipe() take a timeout= parameter of its own.
    """
    cfg = resolve_settings(settings, context)
    pipe_buf_size: int = int(bufmaxsz) if bufmaxsz is not None else int(cfg.bufmaxsz)
    idle_val: float = (
        float(idle_timeout)
        if idle_timeout is not None
        else float(cfg.pipe_idle_timeout)
    )
    send_val: float = (
        float(send_timeout)
        if send_timeout is not None
        else float(cfg.pipe_send_timeout)
    )
    phase = "idle"

    def on_receive():
        nonlocal phase
        phase = "send"
        cancel_scope.deadline = trio.current_time() + send_val

    def on_send():
        nonlocal phase
        phase = "idle"
        cancel_scope.deadline = trio.current_time() + idle_val

    with trio.CancelScope() as cancel_scope:
        cancel_scope.deadline = trio.current_time() + idle_val
        await pipe(
            from_stream,
            to_stream,
            bufmaxsz=pipe_buf_size,
            on_receive=on_receive,
            on_send=on_send,
            settings=cfg,
            context=context,
        )
    if cancel_scope.cancelled_caught:
        raw_timeout = send_timeout if phase == "send" else idle_timeout
        timeout = raw_timeout if raw_timeout is not None else (send_val if phase == "send" else idle_val)
        reason = "send stalled" if phase == "send" else "idle"
        logger.warning("Pipe %s for %ss, closing tunnel", reason, timeout)
        raise trio.TooSlowError(f"Pipe {reason} for {timeout}s")


__all__ = [
    "pipe",
    "pipe_with_timeout",
]
