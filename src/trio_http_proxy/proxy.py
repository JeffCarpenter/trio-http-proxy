#!/usr/bin/env python3

# Copyright 2018-2019 Joshua Bronson. All Rights Reserved.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import errno
import inspect
import logging
import re
from collections.abc import MutableMapping
from contextlib import nullcontext
from contextvars import ContextVar
from functools import partial
from io import DEFAULT_BUFFER_SIZE
from itertools import count
from os import getenv, strerror
from typing import Any
from urllib.parse import urlsplit

import h11
import trio

from .cache import (
    DEFAULT_PROXY_CACHE,
    DEFAULT_RATE_LIMIT_PERIOD,
    CacheBackendUnavailable,
    CircuitBreakerOpen,
    RateLimitError,
    get_client_key,
)


def _int_env(
    name: str,
    default: int,
    min_val: int | None = None,
    max_val: int | None = None,
    _getenv: Any = getenv,
) -> int:
    try:
        val = _getenv(name, default)
    except TypeError:
        val = _getenv(name)
    if val is None or (isinstance(val, str) and not val.strip()):
        res = default
    else:
        try:
            res = int(val)
        except ValueError:
            raise ValueError(f"Invalid integer for {name}: {val!r}") from None
    if min_val is not None and res < min_val:
        raise ValueError(f"{name} must be >= {min_val}, got {res}")
    if max_val is not None and res > max_val:
        raise ValueError(f"{name} must be <= {max_val}, got {res}")
    return res


def _validate_bufmaxsz(val: int) -> int:
    if val < 1:
        raise ValueError(f"BUFMAXSZ must be >= 1, got {val}")
    return val


def _float_env(name: str, default: float, _getenv: Any = getenv) -> float:
    return float(_getenv(name, default))


def _retry_after_seconds(period: str) -> int:
    if period.endswith("s"):
        try:
            return int(period[:-1])
        except ValueError:
            pass
    try:
        return int(period)
    except ValueError:
        return 60


DEFAULT_PORT = 8080
PORT = _int_env("PORT", DEFAULT_PORT, min_val=0, max_val=65535)

BUFMAXSZ = _validate_bufmaxsz(_int_env("BUFMAXSZ", DEFAULT_BUFFER_SIZE, min_val=1))
DEFAULT_MAXREQSZ = 16384
MAXREQSZ = _int_env("MAXREQSZ", DEFAULT_MAXREQSZ, min_val=1)

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

# Connection cap: bounds how many active proxy tunnels run concurrently.
# Note: this limits active tunnels/sessions, not raw accepted socket file
# descriptors, as connections exceeding the limit wait in the limiter's
# acquire queue holding their open client socket until a slot frees up.
DEFAULT_MAXCONNS = 1000
MAXCONNS = _int_env("MAXCONNS", DEFAULT_MAXCONNS, min_val=1)
CONN_LIMITER = trio.CapacityLimiter(MAXCONNS)

DEFAULT_LOG_LEVEL = "INFO"
LOG_LEVEL = getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()

HTTP_METHOD_CONNECT = b"CONNECT"
HTTP_400_BAD_REQUEST = (
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
HTTP_408_REQUEST_TIMEOUT = (
    b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
HTTP_414_URI_TOO_LONG = (
    b"HTTP/1.1 414 URI Too Long\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
RETRY_AFTER_SECS = _retry_after_seconds(DEFAULT_RATE_LIMIT_PERIOD)
HTTP_429_TOO_MANY_REQUESTS = (
    f"HTTP/1.1 429 Too Many Requests\r\nRetry-After: {RETRY_AFTER_SECS}\r\n"
    f"Connection: close\r\nContent-Length: 0\r\n\r\n".encode()
)
HTTP_502_BAD_GATEWAY = (
    b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
HTTP_503_SERVICE_UNAVAILABLE = b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
HTTP_504_GATEWAY_TIMEOUT = (
    b"HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)


def _http_429_response(retry_after: Any = None) -> bytes:
    """Build a rate-limit response using the rejection's expiry metadata."""
    for value in (
        getattr(retry_after, "retry_after_seconds", None),
        getattr(retry_after, "retry_after", None),
        retry_after,
    ):
        if value is None:
            continue
        try:
            seconds = int(value)
        except TypeError, ValueError:
            continue
        if seconds >= 1:
            return (
                f"HTTP/1.1 429 Too Many Requests\r\nRetry-After: {seconds}\r\n"
                f"Connection: close\r\nContent-Length: 0\r\n\r\n"
            ).encode()
    return HTTP_429_TOO_MANY_REQUESTS


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
_DEFAULT_PROXY_CACHE_SENTINEL = object()
CV_PROXY_CACHE: ContextVar[Any] = ContextVar(
    "proxy_cache", default=_DEFAULT_PROXY_CACHE_SENTINEL
)

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
    limiter: trio.CapacityLimiter | None = CONN_LIMITER,
    proxy_cache: Any = _DEFAULT_PROXY_CACHE_SENTINEL,
):
    if proxy_cache is _DEFAULT_PROXY_CACHE_SENTINEL:
        cv_cache = CV_PROXY_CACHE.get()
        if cv_cache is not _DEFAULT_PROXY_CACHE_SENTINEL:
            proxy_cache = cv_cache
        else:
            proxy_cache = DEFAULT_PROXY_CACHE
    CV_CONN_ID.set(_nextid())
    CV_CLIENT_STREAM.set(client_stream)
    limiter_ctx = limiter if limiter is not None else nullcontext()
    try:
        async with client_stream, limiter_ctx:
            if proxy_cache is not None:
                try:
                    client_key = get_client_key(client_stream)
                    if client_key:
                        try:
                            await proxy_cache.check_rate_limit(client_key)
                        except RateLimitError as exc:
                            logger.warning(
                                "Rate limit exceeded for %s, sending 429 to client...",
                                client_key,
                            )
                            try:
                                await client_stream.send_all(_http_429_response(exc))
                            except trio.BrokenResourceError, trio.ClosedResourceError:
                                pass
                            return
                except CacheBackendUnavailable:
                    logger.warning(
                        "Cache backend unavailable during rate limit check, sending 503 to client..."
                    )
                    try:
                        await client_stream.send_all(HTTP_503_SERVICE_UNAVAILABLE)
                    except trio.BrokenResourceError, trio.ClosedResourceError:
                        pass
                    return
                except trio.BrokenResourceError, trio.ClosedResourceError:
                    logger.info("Peer disconnected before rate limit check")
                    return
                except Exception:
                    logger.exception("Unexpected error in rate limiting check")

            try:
                dest_stream = await tunnel(
                    client_stream, _connect=_connect, proxy_cache=proxy_cache
                )
            except CacheBackendUnavailable:
                logger.warning(
                    "Cache backend unavailable during tunnel, sending 503 to client..."
                )
                try:
                    await client_stream.send_all(HTTP_503_SERVICE_UNAVAILABLE)
                except trio.BrokenResourceError, trio.ClosedResourceError:
                    pass
                return
            except (
                ValueError,
                h11.ProtocolError,
                OSError,
                trio.TooSlowError,
                CircuitBreakerOpen,
            ):
                return
            except trio.BrokenResourceError, trio.ClosedResourceError:
                logger.info("Peer disconnected during handshake")
                return

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
    except* trio.BrokenResourceError, trio.ClosedResourceError:
        logger.info("Peer disconnected")
    except* Exception:
        logger.exception("Unexpected exception in connection handler")


async def _serve_one_listener_gated(
    listener: trio.SocketListener,
    handler_nursery: trio.Nursery,
    handler: Any,
    limiter: trio.CapacityLimiter,
) -> None:
    async with listener:
        while True:
            if hasattr(listener, "socket"):
                await trio.lowlevel.wait_readable(listener.socket)
            token = object()
            await limiter.acquire_on_behalf_of(token)
            try:
                stream = await listener.accept()
            except OSError as exc:
                limiter.release_on_behalf_of(token)
                if exc.errno in {
                    errno.EMFILE,
                    errno.ENFILE,
                    errno.ENOBUFS,
                    errno.ENOMEM,
                }:
                    logger.exception(
                        "accept returned %s (%s); retrying in %s seconds",
                        errno.errorcode[exc.errno],
                        strerror(exc.errno),
                        0.1,
                    )
                    await trio.sleep(0.1)
                    continue
                raise
            except BaseException:
                limiter.release_on_behalf_of(token)
                raise
            try:
                handler_nursery.start_soon(
                    _run_gated_handler, stream, handler, limiter, token
                )
            except BaseException:
                try:
                    await trio.aclose_forcefully(stream)
                finally:
                    limiter.release_on_behalf_of(token)
                raise


async def _run_gated_handler(
    stream: trio.SocketStream,
    handler: Any,
    limiter: trio.CapacityLimiter,
    token: object,
) -> None:
    try:
        async with stream:
            await handler(stream)
    except* trio.BrokenResourceError, trio.ClosedResourceError:
        logger.info("Peer disconnected")
    except* Exception:
        logger.exception("Unexpected exception in connection handler")
    finally:
        limiter.release_on_behalf_of(token)


async def _serve_tcp_gated(
    handler: Any,
    port: int,
    *,
    host: str | bytes | None = None,
    backlog: int | None = None,
    limiter: trio.CapacityLimiter = CONN_LIMITER,
    listeners: list[trio.SocketListener] | None = None,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
) -> None:
    if listeners is None:
        listeners = await trio.open_tcp_listeners(port, host=host, backlog=backlog)
    async with trio.open_nursery() as nursery:
        for listener in listeners:
            nursery.start_soon(
                _serve_one_listener_gated, listener, nursery, handler, limiter
            )
        task_status.started(listeners)


async def start_server(
    server=http_proxy,
    port=PORT,
    _serve=None,
    proxy_cache: Any = _DEFAULT_PROXY_CACHE_SENTINEL,
    limiter: trio.CapacityLimiter | None = CONN_LIMITER,
    task_status: trio.TaskStatus[Any] = trio.TASK_STATUS_IGNORED,
):
    if proxy_cache is _DEFAULT_PROXY_CACHE_SENTINEL:
        proxy_cache = DEFAULT_PROXY_CACHE
    cache_token = CV_PROXY_CACHE.set(proxy_cache)
    try:
        if proxy_cache is not None and hasattr(proxy_cache, "start"):
            proxy_cache.start()
        logger.info(
            "* Starting %s on port %s...",
            getattr(server, "__name__", str(server)),
            port or "(OS-selected port)",
        )

        handler = server
        if server is http_proxy:
            handler = partial(
                http_proxy,
                limiter=None if _serve is None else limiter,
                proxy_cache=proxy_cache,
            )
        else:
            try:
                sig = inspect.signature(server)
                if "proxy_cache" in sig.parameters:
                    handler = partial(server, proxy_cache=proxy_cache)
            except ValueError, TypeError:
                pass

        try:
            if _serve is not None:
                try:
                    sig = inspect.signature(_serve)
                    if "task_status" in sig.parameters:
                        await _serve(handler, port, task_status=task_status)
                    else:
                        await _serve(handler, port)
                except ValueError, TypeError:
                    await _serve(handler, port)
            else:
                await _serve_tcp_gated(
                    handler,
                    port,
                    limiter=limiter if limiter is not None else CONN_LIMITER,
                    task_status=task_status,
                )
        except* KeyboardInterrupt:
            logger.info("Goodbye for now.")
    finally:
        try:
            if proxy_cache is not None and hasattr(proxy_cache, "stop"):
                proxy_cache.stop()
        finally:
            CV_PROXY_CACHE.reset(cache_token)


async def tunnel(
    client_stream,
    _connect=trio.open_tcp_stream,
    read_timeout=CONNECT_READ_TIMEOUT,
    dest_connect_timeout=DEST_CONNECT_TIMEOUT,
    proxy_cache: Any = _DEFAULT_PROXY_CACHE_SENTINEL,
):
    """Given a stream from a client containing an HTTP CONNECT request,
    open a connection to the destination server specified in the CONNECT request,
    and notify the client when the end-to-end connection has been established.
    Return the destination stream.
    """
    if proxy_cache is _DEFAULT_PROXY_CACHE_SENTINEL:
        cv_cache = CV_PROXY_CACHE.get()
        if cv_cache is not _DEFAULT_PROXY_CACHE_SENTINEL:
            proxy_cache = cv_cache
        else:
            proxy_cache = DEFAULT_PROXY_CACHE
    try:
        connect_res = await process_as_connect_request(
            client_stream, read_timeout=read_timeout
        )
        if len(connect_res) == 3:
            desthost, destport, trailing_data = connect_res
        else:
            desthost, destport = connect_res[0], connect_res[1]
            trailing_data = b""
    except RequestTooLargeError:
        logger.warning(
            "Oversized CONNECT request, sending 414 to client...", exc_info=True
        )
        try:
            await client_stream.send_all(HTTP_414_URI_TOO_LONG)
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
        raise
    except ProxyProtocolError, h11.ProtocolError:
        logger.warning("Bad CONNECT request, sending 400 to client...", exc_info=True)
        try:
            await client_stream.send_all(HTTP_400_BAD_REQUEST)
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
        raise
    except trio.TooSlowError:
        logger.warning(
            "CONNECT request took too long to arrive, sending 408 to client...",
            exc_info=True,
        )
        try:
            await client_stream.send_all(HTTP_408_REQUEST_TIMEOUT)
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
        raise

    CV_DESTHOST.set(desthost)

    if proxy_cache is not None:
        try:
            await proxy_cache.check_circuit(desthost, destport)
        except CircuitBreakerOpen:
            logger.warning(
                "Circuit breaker open for %s:%d, sending 502 to client...",
                desthost,
                destport,
            )
            try:
                await client_stream.send_all(HTTP_502_BAD_GATEWAY)
            except trio.BrokenResourceError, trio.ClosedResourceError:
                pass
            raise

    logger.info("Got CONNECT request for %s:%d, connecting...", desthost, destport)

    target_hosts = [desthost]
    if proxy_cache is not None:
        try:
            if hasattr(proxy_cache, "resolve_hosts"):
                target_hosts = await proxy_cache.resolve_hosts(desthost)
            else:
                resolved = await proxy_cache.resolve_host(desthost)
                target_hosts = [resolved]
        except OSError as exc:
            if proxy_cache is not None:
                await proxy_cache.record_connection_result(desthost, destport, exc)
            logger.warning(
                "Could not resolve %s, sending 502 to client...",
                desthost,
            )
            try:
                await client_stream.send_all(HTTP_502_BAD_GATEWAY)
            except trio.BrokenResourceError, trio.ClosedResourceError:
                pass
            raise

    dest_stream = None
    last_exc: OSError | trio.TooSlowError | None = None
    overall_deadline = trio.current_time() + dest_connect_timeout

    for i, host in enumerate(target_hosts):
        time_left = overall_deadline - trio.current_time()
        if time_left <= 0:
            last_exc = trio.TooSlowError("Overall connect timeout exceeded")
            break
        rem = len(target_hosts) - i
        per_host_timeout = time_left / rem if rem > 1 else time_left

        try:
            with trio.fail_after(per_host_timeout):
                dest_stream = await _connect(host, destport)
            break
        except OSError as exc:
            last_exc = exc
            continue
        except trio.TooSlowError as exc:
            last_exc = exc
            continue

    if dest_stream is None:
        if isinstance(last_exc, trio.TooSlowError):
            if proxy_cache is not None:
                await proxy_cache.record_connection_result(desthost, destport, last_exc)
            logger.warning(
                "Timed out connecting to %s:%d, sending 504 to client...",
                desthost,
                destport,
            )
            try:
                await client_stream.send_all(HTTP_504_GATEWAY_TIMEOUT)
            except trio.BrokenResourceError, trio.ClosedResourceError:
                pass
            raise last_exc
        else:
            if proxy_cache is not None:
                await proxy_cache.record_connection_result(desthost, destport, last_exc)
            logger.warning(
                "Could not connect to %s:%d, sending 502 to client...",
                desthost,
                destport,
            )
            try:
                await client_stream.send_all(HTTP_502_BAD_GATEWAY)
            except trio.BrokenResourceError, trio.ClosedResourceError:
                pass
            if last_exc is not None:
                raise last_exc
            raise OSError(f"Failed to connect to {desthost}:{destport}")

    try:
        if proxy_cache is not None:
            await proxy_cache.record_connection_result(desthost, destport, None)

        CV_DEST_STREAM.set(dest_stream)
        logger.info("Connected to %s, sending 200 to client...", desthost)
        await client_stream.send_all(HTTP_200_CONNECTION_ESTABLISHED)
        if trailing_data:
            await dest_stream.send_all(trailing_data)
        logger.info("Sent 200 to client, tunnel established.")
        return dest_stream
    except BaseException:
        await dest_stream.aclose()
        raise


async def process_as_connect_request(
    stream, bufmaxsz=BUFMAXSZ, maxreqsz=MAXREQSZ, read_timeout=CONNECT_READ_TIMEOUT
):
    """Read a stream expected to contain a valid HTTP CONNECT request to desthost:destport.
    Parse and return (destination host, port, trailing_bytes). Validate (lightly) and raise if request invalid.
    See https://tools.ietf.org/html/rfc7231#section-4.3.6 for the CONNECT spec.
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout
    seconds -- guards against a slowloris-style client trickling bytes forever.
    """
    logger.debug("Reading...")
    raw_buf = bytearray()
    with trio.fail_after(read_timeout):
        while True:
            match = re.search(rb"\n\r?\n", raw_buf)
            if match is not None:
                header_len = match.end()
                if header_len > maxreqsz:
                    raise RequestTooLargeError(
                        f"Request did not fit in {maxreqsz} bytes"
                    )
                break
            if len(raw_buf) >= maxreqsz:
                raise RequestTooLargeError(f"Request did not fit in {maxreqsz} bytes")

            chunk = await stream.receive_some(bufmaxsz)
            if not chunk:
                break
            raw_buf.extend(chunk)

    h11_conn = h11.Connection(
        our_role=h11.SERVER, max_incomplete_event_size=max(maxreqsz, len(raw_buf))
    )
    h11_conn.receive_data(bytes(raw_buf))
    if not raw_buf or not re.search(rb"\n\r?\n", raw_buf):
        h11_conn.receive_data(b"")
    h11_nextevt = h11_conn.next_event()

    if not isinstance(h11_nextevt, h11.Request):
        raise ProxyProtocolError(f"{h11_nextevt=} is not a h11.Request")
    if h11_nextevt.method != HTTP_METHOD_CONNECT:
        raise ProxyProtocolError(f"{h11_nextevt.method=} != CONNECT")
    target = h11_nextevt.target
    try:
        decoded_target = target.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ProxyProtocolError(f"{target=} contains invalid octets") from exc

    try:
        parsed = urlsplit(f"//{decoded_target}")
    except ValueError as exc:
        raise ProxyProtocolError(f"{target=} is not a valid authority") from exc

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
        raise ProxyProtocolError(f"{target=} has an invalid port") from e
    if destport is None:
        raise ProxyProtocolError(f"{target=} missing port")
    if desthost is None:
        raise ProxyProtocolError(f"{target=} missing host")

    trailing_data, _ = h11_conn.trailing_data

    return desthost, destport, trailing_data


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
    On EOF, calls to_stream.send_eof() if supported to signal half-close.
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
    if hasattr(to_stream, "send_eof"):
        try:
            await to_stream.send_eof()
        except trio.BrokenResourceError, trio.ClosedResourceError:
            pass
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


def main(args: list[str] | None = None) -> None:
    import sys

    if args is None:
        args = sys.argv[1:]
    if "--help" in args or "-h" in args:
        print("Usage: trio_http_proxy [PORT]")
        return
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    trio.run(start_server)


if __name__ == "__main__":
    main()


__all__ = [
    "BUFMAXSZ",
    "CONNECT_READ_TIMEOUT",
    "CONN_LIMITER",
    "CV_CLIENT_STREAM",
    "CV_CONN_ID",
    "CV_DESTHOST",
    "CV_DEST_STREAM",
    "CV_PIPE_FROM",
    "CV_PROXY_CACHE",
    "DEFAULT_BUFFER_SIZE",
    "DEFAULT_CONNECT_READ_TIMEOUT",
    "DEFAULT_DEST_CONNECT_TIMEOUT",
    "DEFAULT_LOG_LEVEL",
    "DEFAULT_MAXCONNS",
    "DEFAULT_MAXREQSZ",
    "DEFAULT_PIPE_IDLE_TIMEOUT",
    "DEFAULT_PIPE_SEND_TIMEOUT",
    "DEFAULT_PORT",
    "DEST_CONNECT_TIMEOUT",
    "HTTP_200_CONNECTION_ESTABLISHED",
    "HTTP_400_BAD_REQUEST",
    "HTTP_408_REQUEST_TIMEOUT",
    "HTTP_414_URI_TOO_LONG",
    "HTTP_429_TOO_MANY_REQUESTS",
    "HTTP_502_BAD_GATEWAY",
    "HTTP_503_SERVICE_UNAVAILABLE",
    "HTTP_504_GATEWAY_TIMEOUT",
    "HTTP_METHOD_CONNECT",
    "LOG_LEVEL",
    "MAXCONNS",
    "MAXREQSZ",
    "PIPE_IDLE_TIMEOUT",
    "PIPE_SEND_TIMEOUT",
    "PORT",
    "ConnectionFilter",
    "ConnectionLoggerAdapter",
    "ProxyProtocolError",
    "RequestTooLargeError",
    "format_conn_prefix",
    "http_proxy",
    "main",
    "pipe",
    "pipe_with_timeout",
    "process_as_connect_request",
    "start_server",
    "tunnel",
]
