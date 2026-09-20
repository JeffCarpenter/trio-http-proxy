# Copyright 2018-2019 Joshua Bronson.    All Rights Reserved.
# Copyright 2026      Jeffrey Carpenter.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import annotations

import logging

import trio

from .config import get_settings
from .connection import CV_PIPE_FROM, ConnectionFilter, ConnectionLoggerAdapter

LOGGER_NAME = "trio_http_proxy"
_raw_logger = logging.getLogger(LOGGER_NAME)
_raw_logger.addFilter(ConnectionFilter())
logger = ConnectionLoggerAdapter(_raw_logger, {})


async def pipe(
    from_stream, to_stream, bufmaxsz: int | None = None, on_receive=None, on_send=None
):
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
        bufmaxsz = get_settings().bufmaxsz
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
    if hasattr(to_stream, "send_eof"):
        try:
            await to_stream.send_eof()
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
    logger.debug("Pipe finished")


async def pipe_with_timeout(
    from_stream,
    to_stream,
    bufmaxsz: int | None = None,
    idle_timeout: float | None = None,
    send_timeout: float | None = None,
):
    """Run pipe(from_stream, to_stream), raising trio.TooSlowError if
    idle_timeout seconds pass with no data received, or send_timeout
    seconds pass with to_stream not accepting a chunk already read --
    guards against a slowloris-style peer that opens a tunnel and then goes
    silent (or stops draining) forever. Owns a CancelScope here, in the
    caller, and nudges its deadline from pipe()'s on_receive/on_send hooks
    rather than making pipe() take a timeout= parameter of its own.
    """
    _proxy_cfg = get_settings()
    if bufmaxsz is None:
        bufmaxsz = _proxy_cfg.bufmaxsz
    if idle_timeout is None:
        idle_timeout = _proxy_cfg.pipe_idle_timeout
    if send_timeout is None:
        send_timeout = _proxy_cfg.pipe_send_timeout
    phase = "idle"

    def on_receive():
        nonlocal phase
        phase = "send"
        cancel_scope.deadline = trio.current_time() + send_timeout

    def on_send():
        nonlocal phase
        phase = "idle"
        cancel_scope.deadline = trio.current_time() + idle_timeout

    with trio.CancelScope() as cancel_scope:
        cancel_scope.deadline = trio.current_time() + idle_timeout
        await pipe(
            from_stream, to_stream, bufmaxsz, on_receive=on_receive, on_send=on_send
        )
    if cancel_scope.cancelled_caught:
        timeout = send_timeout if phase == "send" else idle_timeout
        reason = "send stalled" if phase == "send" else "idle"
        logger.warning("Pipe %s for %ss, closing tunnel", reason, timeout)
        raise trio.TooSlowError(f"Pipe {reason} for {timeout}s")


__all__ = [
    "pipe",
    "pipe_with_timeout",
]
