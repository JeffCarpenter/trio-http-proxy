#!/usr/bin/env python3

# Copyright 2018-2019 Joshua Bronson. All Rights Reserved.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
from collections.abc import MutableMapping
from contextvars import ContextVar
from functools import partial
from io import DEFAULT_BUFFER_SIZE
from itertools import count
from os import getenv
from typing import Any
from urllib.parse import urlsplit

import h11
import trio


def _int_env(name, default, _getenv=getenv):
    return int(_getenv(name, default))


def _float_env(name, default, _getenv=getenv):
    return float(_getenv(name, default))


DEFAULT_PORT = 8080
PORT = _int_env("PORT", DEFAULT_PORT)

BUFMAXSZ = _int_env("BUFMAXSZ", DEFAULT_BUFFER_SIZE)
DEFAULT_MAXREQSZ = 16384
MAXREQSZ = _int_env("MAXREQSZ", DEFAULT_MAXREQSZ)

# Slowloris-shaped resource-exhaustion protection: without these, a slow or
# stalled client/destination can hold a task open forever (see
# .planning/known-shortcomings/findings.md).
DEFAULT_CONNECT_READ_TIMEOUT = 10.0
CONNECT_READ_TIMEOUT = _float_env("CONNECT_READ_TIMEOUT", DEFAULT_CONNECT_READ_TIMEOUT)

DEFAULT_DEST_CONNECT_TIMEOUT = 10.0
DEST_CONNECT_TIMEOUT = _float_env("DEST_CONNECT_TIMEOUT", DEFAULT_DEST_CONNECT_TIMEOUT)

DEFAULT_PIPE_IDLE_TIMEOUT = 300.0
PIPE_IDLE_TIMEOUT = _float_env("PIPE_IDLE_TIMEOUT", DEFAULT_PIPE_IDLE_TIMEOUT)

DEFAULT_PIPE_SEND_TIMEOUT = 30.0
PIPE_SEND_TIMEOUT = _float_env("PIPE_SEND_TIMEOUT", DEFAULT_PIPE_SEND_TIMEOUT)

# Connection cap: bounds how many tunnels run concurrently, so an unbounded
# flood of clients can't exhaust file descriptors / memory (see
# .planning/known-shortcomings/findings.md). A connection over the cap just
# waits for a slot to free up rather than being rejected outright.
DEFAULT_MAXCONNS = 1000
MAXCONNS = _int_env("MAXCONNS", DEFAULT_MAXCONNS)
CONN_LIMITER = trio.CapacityLimiter(MAXCONNS)

DEFAULT_LOG_LEVEL = "INFO"
LOG_LEVEL = getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()

HTTP_METHOD_CONNECT = b"CONNECT"
HTTP_400_BAD_REQUEST = b"HTTP/1.1 400 Bad Request\r\n\r\n"
HTTP_408_REQUEST_TIMEOUT = b"HTTP/1.1 408 Request Timeout\r\n\r\n"
HTTP_414_URI_TOO_LONG = b"HTTP/1.1 414 URI Too Long\r\n\r\n"
HTTP_502_BAD_GATEWAY = b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
HTTP_504_GATEWAY_TIMEOUT = b"HTTP/1.1 504 Gateway Timeout\r\n\r\n"


class ProxyProtocolError(ValueError):
    """The client's CONNECT request is malformed or violates the
    authority-form contract (RFC 7231 section 4.3.6). Raised in place of
    `assert` so validation survives `python -O` (asserts are stripped under
    optimization, silently turning every one of these checks into a no-op)."""


class RequestTooLargeError(ProxyProtocolError):
    """The CONNECT request exceeded maxreqsz before a complete request
    arrived. A subclass of ProxyProtocolError so it's still caught wherever
    that is, but distinguishable so tunnel() can reply 414 instead of 400."""


HTTP_200_CONNECTION_ESTABLISHED = b"HTTP/1.1 200 Connection established\r\n\r\n"

CV_CLIENT_STREAM: ContextVar[trio.SocketStream | None] = ContextVar(
    "client_stream", default=None
)
CV_DEST_STREAM: ContextVar[trio.SocketStream | None] = ContextVar(
    "dest_stream", default=None
)
CV_PIPE_FROM: ContextVar[trio.SocketStream | None] = ContextVar(
    "pipe_from", default=None
)
CV_DESTHOST: ContextVar[str | None] = ContextVar("desthost", default=None)
CV_CONN_ID: ContextVar[int | None] = ContextVar("conn_id", default=None)

_conn_ids = count(1)

LOGGER_NAME = "trio_http_proxy"
_raw_logger = logging.getLogger(LOGGER_NAME if __name__ == "__main__" else __name__)
_raw_logger.addHandler(logging.NullHandler())


def _connection_context() -> dict[str, Any]:
    conn_id = CV_CONN_ID.get()
    client_stream = CV_CLIENT_STREAM.get()
    dest_stream = CV_DEST_STREAM.get()
    desthost = CV_DESTHOST.get()
    pipe_from = CV_PIPE_FROM.get()
    direction = None
    prefix = ""

    if client_stream is not None:
        if dest_stream is not None:
            direction = "<>"
            if pipe_from is not None:
                direction = "->" if pipe_from is client_stream else "<-"
            prefix = f"[conn{conn_id} {direction} {desthost}]"
        else:
            prefix = f"[conn{conn_id}]"

    return {
        "conn_id": conn_id,
        "desthost": desthost,
        "direction": direction,
        "conn_prefix": prefix,
    }


def format_conn_prefix() -> str:
    return str(_connection_context()["conn_prefix"])


class ConnectionFilter(logging.Filter):
    """Ensure connection attributes (conn_id, desthost, direction, conn_prefix)
    are always populated on LogRecord instances."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _connection_context()
        for k, v in ctx.items():
            if not hasattr(record, k):
                setattr(record, k, v)
        return True


class ConnectionLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that prepends connection context to log messages and injects
    structured attributes into LogRecord extra."""

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        ctx = _connection_context()
        prefix = ctx["conn_prefix"]
        if prefix:
            escaped_prefix = prefix.replace("%", "%%")
            msg = f"{escaped_prefix} {msg}"
        extra = kwargs.setdefault("extra", {})
        for k, v in ctx.items():
            extra.setdefault(k, v)
        return msg, kwargs

    def addHandler(self, hdlr: logging.Handler) -> None:
        self.logger.addHandler(hdlr)

    def removeHandler(self, hdlr: logging.Handler) -> None:
        self.logger.removeHandler(hdlr)

    def addFilter(self, fltr: logging.Filter | Any) -> None:
        self.logger.addFilter(fltr)

    def removeFilter(self, fltr: logging.Filter | Any) -> None:
        self.logger.removeFilter(fltr)


_raw_logger.addFilter(ConnectionFilter())
logger = ConnectionLoggerAdapter(_raw_logger, {})


async def http_proxy(
    client_stream,
    _nextid=_conn_ids.__next__,
    _connect=trio.open_tcp_stream,
    idle_timeout=PIPE_IDLE_TIMEOUT,
    send_timeout=PIPE_SEND_TIMEOUT,
    limiter=CONN_LIMITER,
):
    CV_CONN_ID.set(_nextid())
    CV_CLIENT_STREAM.set(client_stream)
    async with client_stream, limiter:
        try:
            dest_stream = await tunnel(client_stream, _connect=_connect)
        except ValueError, h11.ProtocolError, OSError, trio.TooSlowError:
            # ValueError here covers ProxyProtocolError/RequestTooLargeError
            # (both subclass it). No AssertionError: tunnel() no longer uses
            # assert for request validation, so one reaching here would be a
            # genuine bug, not a bad request -- let it fall through to the
            # except* Exception handler below instead of being swallowed.
            return  # tunnel() already logged and notified the client
        try:
            async with dest_stream, trio.open_nursery() as nursery:
                nursery.start_soon(
                    partial(
                        pipe_with_timeout,
                        client_stream,
                        dest_stream,
                        idle_timeout=idle_timeout,
                        send_timeout=send_timeout,
                    )
                )
                nursery.start_soon(
                    partial(
                        pipe_with_timeout,
                        dest_stream,
                        client_stream,
                        idle_timeout=idle_timeout,
                        send_timeout=send_timeout,
                    )
                )
        except* trio.BrokenResourceError, trio.ClosedResourceError:
            logger.info("Peer disconnected")
        except* trio.TooSlowError:
            pass  # pipe() already logged the reason before raising
        except* Exception:
            logger.exception("Unexpected exception in connection handler")


async def start_server(server=http_proxy, port=PORT, _serve=trio.serve_tcp):
    logger.info(
        "* Starting %s on port %s...",
        server.__name__,
        port or "(OS-selected port)",
    )
    try:
        await _serve(server, port)
    except KeyboardInterrupt:
        logger.info("Goodbye for now.")


async def tunnel(
    client_stream,
    _connect=trio.open_tcp_stream,
    read_timeout=CONNECT_READ_TIMEOUT,
    dest_connect_timeout=DEST_CONNECT_TIMEOUT,
):
    """Given a stream from a client containing an HTTP CONNECT request,
    open a connection to the destination server specified in the CONNECT request,
    and notify the client when the end-to-end connection has been established.
    Return the destination stream and the corresponding host.
    """
    try:
        desthost, destport = await process_as_connect_request(
            client_stream, read_timeout=read_timeout
        )
    except RequestTooLargeError:
        logger.warning(
            "Oversized CONNECT request, sending 414 to client...", exc_info=True
        )
        await client_stream.send_all(HTTP_414_URI_TOO_LONG)
        raise
    except ProxyProtocolError, h11.ProtocolError:
        logger.warning("Bad CONNECT request, sending 400 to client...", exc_info=True)
        await client_stream.send_all(HTTP_400_BAD_REQUEST)
        raise
    except trio.TooSlowError:
        logger.warning(
            "CONNECT request took too long to arrive, sending 408 to client...",
            exc_info=True,
        )
        await client_stream.send_all(HTTP_408_REQUEST_TIMEOUT)
        raise
    logger.info("Got CONNECT request for %s:%d, connecting...", desthost, destport)
    try:
        with trio.fail_after(dest_connect_timeout):
            dest_stream = await _connect(desthost, destport)
    except OSError:
        logger.exception(
            "Could not connect to %s:%d, sending 502 to client...",
            desthost,
            destport,
        )
        await client_stream.send_all(HTTP_502_BAD_GATEWAY)
        raise
    except trio.TooSlowError:
        logger.exception(
            "Timed out connecting to %s:%d, sending 504 to client...",
            desthost,
            destport,
        )
        await client_stream.send_all(HTTP_504_GATEWAY_TIMEOUT)
        raise
    CV_DEST_STREAM.set(dest_stream)
    CV_DESTHOST.set(desthost)
    logger.info("Connected to %s, sending 200 to client...", desthost)
    await client_stream.send_all(HTTP_200_CONNECTION_ESTABLISHED)
    logger.info("Sent 200 to client, tunnel established.")
    return dest_stream


async def process_as_connect_request(
    stream, bufmaxsz=BUFMAXSZ, maxreqsz=MAXREQSZ, read_timeout=CONNECT_READ_TIMEOUT
):
    """Read a stream expected to contain a valid HTTP CONNECT request to desthost:destport.
    Parse and return the destination host. Validate (lightly) and raise if request invalid.
    See https://tools.ietf.org/html/rfc7231#section-4.3.6 for the CONNECT spec.
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout
    seconds -- guards against a slowloris-style client trickling bytes forever.
    """
    logger.debug("Reading...")
    # h11's own request-line/header buffering ceiling defaults to 16384 bytes
    # regardless of our maxreqsz. Left unset, a MAXREQSZ configured above
    # 16384 doesn't just get mislabeled -- a legitimate request that's well
    # within *our* configured limit gets rejected outright, since h11 raises
    # its own RemoteProtocolError ("Receive buffer too long") once buffered-
    # but-still-incomplete data passes 16384, before our check below ever
    # gets a say. Tying the two together keeps h11's ceiling from being
    # stricter than the one we actually promised.
    h11_conn = h11.Connection(our_role=h11.SERVER, max_incomplete_event_size=maxreqsz)
    total_bytes_read = 0
    with trio.fail_after(read_timeout):
        while (h11_nextevt := h11_conn.next_event()) == h11.NEED_DATA:
            bytes_read = await stream.receive_some(bufmaxsz)
            total_bytes_read += len(bytes_read)
            # Checked (and raised) before receive_data() below, so h11 never
            # sees a cumulative total >= maxreqsz -- which is also what keeps
            # a *small* maxreqsz (tightening h11's ceiling below its 16384
            # default) safe: our guard always wins the race, never h11's.
            if total_bytes_read >= maxreqsz:
                raise RequestTooLargeError(f"Request did not fit in {maxreqsz} bytes")
            h11_conn.receive_data(bytes_read)
    if not isinstance(h11_nextevt, h11.Request):
        raise ProxyProtocolError(f"{h11_nextevt=} is not a h11.Request")
    if h11_nextevt.method != HTTP_METHOD_CONNECT:
        raise ProxyProtocolError(f"{h11_nextevt.method=} != CONNECT")
    target = h11_nextevt.target
    # h11's request-target grammar (vchar = [\x21-\x7e]) is validated against
    # the raw request line before a Request event is ever constructed -- an
    # out-of-range byte here already raised h11.RemoteProtocolError upstream,
    # caught by tunnel()'s h11.ProtocolError handler. So .decode() below can't
    # raise UnicodeDecodeError; confirmed against h11's grammar and by direct
    # reproduction, not assumed.
    decoded_target = target.decode()
    # urlsplit handles bracketed IPv6 authorities (e.g. "[::1]:443") correctly;
    # a naive split on the first ":" tears such addresses apart.
    parsed = urlsplit(f"//{decoded_target}")
    # RFC 7231 4.3.6: a CONNECT request-target must be bare authority-form
    # (host:port) -- no userinfo, path, query, or fragment. urlsplit() is lax
    # about this and silently discards everything but host/port, so a
    # smuggled path or "user:pass@" prefix must be rejected explicitly,
    # before any parsed component is trusted.
    if (
        parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ProxyProtocolError(f"{target=} is not authority-form (RFC 7231 4.3.6)")
    try:
        desthost, destport = parsed.hostname, parsed.port
    except ValueError as e:
        # parsed.port raises ValueError for a non-numeric port (e.g. "notaport").
        raise ProxyProtocolError(f"{target=} has an invalid port") from e
    if destport is None:
        raise ProxyProtocolError(f"{target=} missing port")
    if desthost is None:
        raise ProxyProtocolError(f"{target=} missing host")
    return desthost, destport


async def pipe(
    from_stream, to_stream, bufmaxsz=BUFMAXSZ, on_receive=None, on_send=None
):
    """Forward chunks from from_stream to to_stream until EOF.
    on_receive and on_send, when given, are called with no arguments right
    after each successful receive_some()/send_all(). pipe() itself knows
    nothing about timeouts; a caller can use these hooks to implement one
    (see pipe_with_timeout) by nudging a CancelScope's deadline forward on
    every sign of progress -- Trio's recommended pattern for timeouts,
    which keeps a reusable function like this one oblivious to them.
    """
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
    logger.debug("Pipe finished")


async def pipe_with_timeout(
    from_stream,
    to_stream,
    bufmaxsz=BUFMAXSZ,
    idle_timeout=PIPE_IDLE_TIMEOUT,
    send_timeout=PIPE_SEND_TIMEOUT,
):
    """Run pipe(from_stream, to_stream), raising trio.TooSlowError if
    idle_timeout seconds pass with no data received, or send_timeout
    seconds pass with to_stream not accepting a chunk already read --
    guards against a slowloris-style peer that opens a tunnel and then goes
    silent (or stops draining) forever. Owns a CancelScope here, in the
    caller, and nudges its deadline from pipe()'s on_receive/on_send hooks
    rather than making pipe() take a timeout= parameter of its own.
    """
    phase = "idle"  # which of the two blocking calls the deadline is guarding

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


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    trio.run(start_server)
