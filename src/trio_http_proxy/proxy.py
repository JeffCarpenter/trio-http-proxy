#!/usr/bin/env python3

# Copyright 2018-2019 Joshua Bronson.    All Rights Reserved.
# Copyright 2026      Jeffrey Carpenter.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import annotations

import errno
import inspect
import logging
from contextlib import nullcontext
from functools import cache, partial
from os import strerror
from typing import Any, Self

import h11
import trio

from . import http
from .cache import (
    get_client_key,
)
from .circuit_breaker import CircuitPermit
from .config import Settings, get_settings, resolve_settings
from .config.builder import ProxyCacheBuilder, SettingsBuilderMixin
from .connection import (
    _DEFAULT_PROXY_CACHE_SENTINEL,
    CV_CLIENT_STREAM,
    CV_CONN_ID,
    CV_CURRENT_CONTEXT,
    CV_DEST_STREAM,
    CV_DESTHOST,
    CV_PROXY_CACHE,
    ConnectionContext,
    ConnectionFilter,
    ConnectionLoggerAdapter,
    _conn_ids,
    format_conn_prefix,
    resolve_proxy_cache,
)
from .errors import (
    CacheBackendUnavailable,
    CircuitBreakerOpen,
    ProxyProtocolError,
    RateLimitError,
    RequestTooLargeError,
)
from .protocol import process_as_connect_request
from .proxy_cache import (
    ConnectionRecord,
    ConnectionResultBuilder,
    ProxyCacheProtocol,
    get_default_proxy_cache,
)
from .streaming import pipe_with_timeout


@cache
def get_conn_limiter() -> trio.CapacityLimiter:
    """Shared connection-capacity limiter, sized from current settings (MAXCONNS)."""
    return trio.CapacityLimiter(get_settings().maxconns)


reset_conn_limiter = get_conn_limiter.cache_clear


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
        except (TypeError, ValueError):
            continue
        if seconds >= 1:
            return (
                f"HTTP/1.1 429 Too Many Requests\r\nRetry-After: {seconds}\r\n"
                f"Connection: close\r\nContent-Length: 0\r\n\r\n"
            ).encode()
    return (
        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\n"
        b"Connection: close\r\nContent-Length: 0\r\n\r\n"
    )


LOGGER_NAME = "trio_http_proxy"
_raw_logger = logging.getLogger(LOGGER_NAME if __name__ == "__main__" else __name__)
_raw_logger.addHandler(logging.NullHandler())
if not any(isinstance(f, ConnectionFilter) for f in _raw_logger.filters):
    _raw_logger.addFilter(ConnectionFilter())
logger = ConnectionLoggerAdapter(_raw_logger, {})

class _DefaultLimiterSentinel:
    """Sentinel indicating default capacity limiter resolution."""

    def __repr__(self) -> str:
        return "<DEFAULT_LIMITER>"

    def __bool__(self) -> bool:
        return False


_DEFAULT_LIMITER = _DefaultLimiterSentinel()


async def http_proxy(
    client_stream: trio.abc.Stream,
    _nextid: Any = _conn_ids.__next__,
    _connect: Any = trio.open_tcp_stream,
    idle_timeout: float | None = None,
    send_timeout: float | None = None,
    limiter: trio.CapacityLimiter | None | _DefaultLimiterSentinel = _DEFAULT_LIMITER,
    proxy_cache: ProxyCacheProtocol | None | Any = _DEFAULT_PROXY_CACHE_SENTINEL,
    *,
    settings: Settings | None = None,
    context: ConnectionContext | None = None,
) -> None:
    _proxy_cfg = resolve_settings(settings, context)
    idle_timeout = (
        idle_timeout if idle_timeout is not None else _proxy_cfg.pipe_idle_timeout
    )
    send_timeout = (
        send_timeout if send_timeout is not None else _proxy_cfg.pipe_send_timeout
    )
    active_limiter: trio.CapacityLimiter | None = (
        get_conn_limiter()
        if limiter is _DEFAULT_LIMITER
        else limiter
        if isinstance(limiter, trio.CapacityLimiter)
        else None
    )
    proxy_cache = resolve_proxy_cache(
        proxy_cache, context, default_factory=get_default_proxy_cache
    )

    if context is None:
        context = ConnectionContext(
            conn_id=_nextid(),
            client_stream=client_stream,
            settings=_proxy_cfg,
            proxy_cache=proxy_cache,
        )

    with context.activate():
        limiter_ctx = active_limiter if active_limiter is not None else nullcontext()
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
                                except (
                                    trio.BrokenResourceError,
                                    trio.ClosedResourceError,
                                ):
                                    pass
                                return
                    except CacheBackendUnavailable:
                        logger.warning(
                            "Cache backend unavailable during rate limit check, sending 503 to client..."
                        )
                        try:
                            await client_stream.send_all(http.http_503_service_unavailable)
                        except (trio.BrokenResourceError, trio.ClosedResourceError):
                            pass
                        return
                    except (trio.BrokenResourceError, trio.ClosedResourceError):
                        logger.info("Peer disconnected before rate limit check")
                        return
                    except Exception:
                        logger.exception("Unexpected error in rate limiting check")

                try:
                    dest_stream = await tunnel(
                        client_stream,
                        _connect=_connect,
                        proxy_cache=proxy_cache,
                        settings=_proxy_cfg,
                        context=context,
                    )
                except CacheBackendUnavailable:
                    logger.warning(
                        "Cache backend unavailable during tunnel, sending 503 to client..."
                    )
                    try:
                        await client_stream.send_all(http.http_503_service_unavailable)
                    except (trio.BrokenResourceError, trio.ClosedResourceError):
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
                except (trio.BrokenResourceError, trio.ClosedResourceError):
                    logger.info("Peer disconnected during handshake")
                    return

                try:
                    async with dest_stream, trio.open_nursery() as nursery:

                        async def _forward_client_to_dest() -> None:
                            try:
                                await pipe_with_timeout(
                                    client_stream,
                                    dest_stream,
                                    bufmaxsz=_proxy_cfg.bufmaxsz,
                                    idle_timeout=idle_timeout,
                                    send_timeout=send_timeout,
                                    settings=_proxy_cfg,
                                    context=context,
                                )
                            finally:
                                # If dest_stream cannot support half-close, closing client sending
                                # must terminate the entire tunnel immediately.
                                if not isinstance(
                                    dest_stream, trio.abc.HalfCloseableStream
                                ):
                                    nursery.cancel_scope.cancel()

                        async def _forward_dest_to_client() -> None:
                            try:
                                await pipe_with_timeout(
                                    dest_stream,
                                    client_stream,
                                    bufmaxsz=_proxy_cfg.bufmaxsz,
                                    idle_timeout=idle_timeout,
                                    send_timeout=send_timeout,
                                    settings=_proxy_cfg,
                                    context=context,
                                )
                            finally:
                                # Per RFC 7231 §4.3.6: once destination closes, the tunnel is complete;
                                # cancel remaining client->dest forwarding to avoid dangling on idle_timeout.
                                nursery.cancel_scope.cancel()

                        nursery.start_soon(_forward_client_to_dest)
                        nursery.start_soon(_forward_dest_to_client)
                except* (trio.BrokenResourceError, trio.ClosedResourceError):
                    logger.info("Peer disconnected")
                except* trio.TooSlowError:
                    pass  # pipe() already logged the reason before raising
                except* Exception:
                    logger.exception("Unexpected exception in connection handler")
        except* (trio.BrokenResourceError, trio.ClosedResourceError):
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
    except* (trio.BrokenResourceError, trio.ClosedResourceError):
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
    limiter: trio.CapacityLimiter | None = None,
    listeners: list[trio.SocketListener] | None = None,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
) -> None:
    if limiter is None:
        limiter = get_conn_limiter()
    if listeners is None:
        listeners = await trio.open_tcp_listeners(port, host=host, backlog=backlog)
    async with trio.open_nursery() as nursery:
        for listener in listeners:
            nursery.start_soon(
                _serve_one_listener_gated, listener, nursery, handler, limiter
            )
        task_status.started(listeners)


class ProxyBuilder(SettingsBuilderMixin):
    """Fluent builder for configuring and launching the HTTP proxy server."""

    def __init__(self, settings: Settings | None = None, **kwargs: Any) -> None:
        self._base_settings: Settings | None = settings
        self._overrides: dict[str, Any] = dict(kwargs)
        self._proxy_cache: ProxyCacheProtocol | ProxyCacheBuilder | None | Any = (
            _DEFAULT_PROXY_CACHE_SENTINEL
        )
        self._limiter: trio.CapacityLimiter | None = None
        self._server: Any = http_proxy

    def with_port(self, port: int) -> Self:
        self._overrides["port"] = port
        return self

    def with_buffer_sizes(
        self,
        bufmaxsz: int | None = None,
        maxreqsz: int | None = None,
    ) -> Self:
        if bufmaxsz is not None:
            self._overrides["bufmaxsz"] = bufmaxsz
        if maxreqsz is not None:
            self._overrides["maxreqsz"] = maxreqsz
        return self

    def with_timeouts(
        self,
        connect_read: float | None = None,
        dest_connect: float | None = None,
        pipe_idle: float | None = None,
        pipe_send: float | None = None,
    ) -> Self:
        if connect_read is not None:
            self._overrides["connect_read_timeout"] = connect_read
        if dest_connect is not None:
            self._overrides["dest_connect_timeout"] = dest_connect
        if pipe_idle is not None:
            self._overrides["pipe_idle_timeout"] = pipe_idle
        if pipe_send is not None:
            self._overrides["pipe_send_timeout"] = pipe_send
        return self

    def with_maxconns(self, maxconns: int) -> Self:
        self._overrides["maxconns"] = maxconns
        return self

    def with_log_level(self, log_level: str) -> Self:
        self._overrides["log_level"] = log_level
        return self

    def with_cache(
        self, cache: ProxyCacheProtocol | ProxyCacheBuilder | None | Any
    ) -> Self:
        self._proxy_cache = cache
        return self

    def with_limiter(self, limiter: trio.CapacityLimiter) -> Self:
        self._limiter = limiter
        return self

    def with_server(self, server: Any) -> Self:
        self._server = server
        return self

    async def run(
        self,
        task_status: trio.TaskStatus[Any] = trio.TASK_STATUS_IGNORED,
        _serve: Any = None,
    ) -> None:
        settings = self.build_settings()
        port = settings.port
        limiter = (
            self._limiter
            if self._limiter is not None
            else trio.CapacityLimiter(settings.maxconns)
        )
        proxy_cache = self._proxy_cache
        if isinstance(proxy_cache, ProxyCacheBuilder):
            proxy_cache = proxy_cache.build()
        else:
            proxy_cache = resolve_proxy_cache(
                proxy_cache, default_factory=get_default_proxy_cache
            )

        cache_token = CV_PROXY_CACHE.set(proxy_cache)
        try:
            if proxy_cache is not None and hasattr(proxy_cache, "start"):
                await trio.to_thread.run_sync(proxy_cache.start)
            logger.info(
                "* Starting %s on port %s...",
                getattr(self._server, "__name__", str(self._server)),
                port or "(OS-selected port)",
            )

            handler = self._server
            if self._server is http_proxy:
                handler = partial(
                    http_proxy,
                    limiter=None if _serve is None else limiter,
                    proxy_cache=proxy_cache,
                    settings=settings,
                )
            else:
                try:
                    parameters = inspect.signature(handler).parameters.values()
                except (TypeError, ValueError):
                    parameters = ()
                extra_kwargs: dict[str, Any] = {}
                param_names = {p.name for p in parameters}
                has_var_keyword = any(
                    p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters
                )
                if "proxy_cache" in param_names or has_var_keyword:
                    extra_kwargs["proxy_cache"] = proxy_cache
                if "settings" in param_names or has_var_keyword:
                    extra_kwargs["settings"] = settings
                if extra_kwargs:
                    handler = partial(handler, **extra_kwargs)

            if _serve is not None:
                try:
                    await _serve(handler, port, task_status=task_status)
                except TypeError:
                    await _serve(handler, port)
            else:
                await _serve_tcp_gated(
                    handler,
                    port,
                    limiter=limiter,
                    task_status=task_status,
                )
        except* KeyboardInterrupt:
            logger.info("Goodbye for now.")
        finally:
            try:
                if proxy_cache is not None and hasattr(proxy_cache, "stop"):
                    await trio.to_thread.run_sync(proxy_cache.stop)
            finally:
                CV_PROXY_CACHE.reset(cache_token)


async def start_server(
    server: Any = http_proxy,
    port: int | None = None,
    _serve: Any = None,
    proxy_cache: ProxyCacheProtocol | ProxyCacheBuilder | None | Any = (
        _DEFAULT_PROXY_CACHE_SENTINEL
    ),
    limiter: trio.CapacityLimiter | None = None,
    task_status: trio.TaskStatus[Any] = trio.TASK_STATUS_IGNORED,
    settings: Settings | None = None,
) -> None:
    builder = (
        ProxyBuilder(settings=settings).with_server(server).with_cache(proxy_cache)
    )
    if port is not None:
        builder = builder.with_port(port)
    if limiter is not None:
        builder = builder.with_limiter(limiter)
    await builder.run(task_status=task_status, _serve=_serve)


async def _record_connection_result(
    proxy_cache: ProxyCacheProtocol | None | Any,
    record: ConnectionRecord | ConnectionResultBuilder,
) -> None:
    # Every call site here passes a ConnectionResultBuilder; delegate to its
    # own record_to() dispatch (proxy_cache=None guard + legacy
    # record_result()-only fallback) instead of duplicating that logic.
    if isinstance(record, ConnectionResultBuilder):
        await record.record_to(proxy_cache)
        return
    if proxy_cache is None:
        return
    await proxy_cache.record_connection_result(record)


async def tunnel(
    client_stream: trio.abc.Stream,
    _connect: Any = trio.open_tcp_stream,
    read_timeout: float | None = None,
    dest_connect_timeout: float | None = None,
    proxy_cache: ProxyCacheProtocol | None | Any = _DEFAULT_PROXY_CACHE_SENTINEL,
    *,
    settings: Settings | None = None,
    context: ConnectionContext | None = None,
) -> trio.abc.Stream:
    """Given a stream from a client containing an HTTP CONNECT request,
    open a connection to the destination server specified in the CONNECT request,
    and notify the client when the end-to-end connection has been established.
    Return the destination stream.
    """
    cfg = resolve_settings(settings, context)
    req_read_timeout: float = (
        float(read_timeout)
        if read_timeout is not None
        else float(cfg.connect_read_timeout)
    )
    dest_timeout: float = (
        float(dest_connect_timeout)
        if dest_connect_timeout is not None
        else float(cfg.dest_connect_timeout)
    )

    proxy_cache = resolve_proxy_cache(
        proxy_cache, context, default_factory=get_default_proxy_cache
    )
    try:
        desthost, destport, trailing_data = await process_as_connect_request(
            client_stream,
            read_timeout=req_read_timeout,
            settings=cfg,
            context=context,
        )
    except RequestTooLargeError:
        logger.warning(
            "Oversized CONNECT request, sending 414 to client...", exc_info=True
        )
        try:
            await client_stream.send_all(http.http_414_uri_too_long)
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            pass
        raise
    except (ProxyProtocolError, h11.ProtocolError):
        logger.warning("Bad CONNECT request, sending 400 to client...", exc_info=True)
        try:
            await client_stream.send_all(http.http_400_bad_request)
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            pass
        raise
    except trio.TooSlowError:
        logger.warning(
            "CONNECT request took too long to arrive, sending 408 to client...",
            exc_info=True,
        )
        try:
            await client_stream.send_all(http.http_408_request_timeout)
        except (trio.BrokenResourceError, trio.ClosedResourceError):
            pass
        raise

    if context is None:
        # When a context is present, process_as_connect_request() already
        # called context.set_desthost() (which sets CV_DESTHOST too); only
        # the context-less legacy path needs this direct set.
        CV_DESTHOST.set(desthost)

    result_builder = ConnectionResultBuilder().with_target(desthost, destport)
    permit: CircuitPermit | None = None
    if proxy_cache is not None:
        try:
            permit = await proxy_cache.check_circuit(desthost, destport)
            result_builder.with_permit(permit)
        except CircuitBreakerOpen:
            logger.warning(
                "Circuit breaker open for %s:%d, sending 502 to client...",
                desthost,
                destport,
            )
            try:
                await client_stream.send_all(http.http_502_bad_gateway)
            except (trio.BrokenResourceError, trio.ClosedResourceError):
                pass
            raise

    logger.info("Got CONNECT request for %s:%d, connecting...", desthost, destport)

    permit_recorded = False
    dest_stream = None
    dest_connected = False
    try:
        target_hosts = [desthost]
        if proxy_cache is not None:
            try:
                target_hosts = await proxy_cache.resolve_hosts(desthost)
            except OSError as exc:
                await _record_connection_result(
                    proxy_cache, result_builder.with_error(exc)
                )
                permit_recorded = True
                logger.warning(
                    "Could not resolve %s, sending 502 to client...",
                    desthost,
                )
                try:
                    await client_stream.send_all(http.http_502_bad_gateway)
                except (trio.BrokenResourceError, trio.ClosedResourceError):
                    pass
                raise

        last_exc: Exception = OSError(
            f"No target addresses resolved for {desthost}:{destport}"
        )
        overall_deadline = trio.current_time() + dest_timeout

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
                    await _record_connection_result(
                        proxy_cache, result_builder.with_error(last_exc)
                    )
                    permit_recorded = True
                logger.warning(
                    "Timed out connecting to %s:%d, sending 504 to client...",
                    desthost,
                    destport,
                )
                try:
                    await client_stream.send_all(http.http_504_gateway_timeout)
                except (trio.BrokenResourceError, trio.ClosedResourceError):
                    pass
                raise last_exc
            else:
                if proxy_cache is not None:
                    await _record_connection_result(
                        proxy_cache, result_builder.with_error(last_exc)
                    )
                    permit_recorded = True
                logger.warning(
                    "Could not connect to %s:%d (%s), sending 502 to client...",
                    desthost,
                    destport,
                    last_exc,
                )
                try:
                    await client_stream.send_all(http.http_502_bad_gateway)
                except (trio.BrokenResourceError, trio.ClosedResourceError):
                    pass
                raise last_exc

        if proxy_cache is not None:
            await _record_connection_result(proxy_cache, result_builder.with_success())
            permit_recorded = True
        dest_connected = True
    finally:
        if permit is not None and not permit_recorded:
            permit.abort()
        if dest_stream is not None and not dest_connected:
            await trio.aclose_forcefully(dest_stream)

    assert dest_stream is not None
    try:
        if context is not None:
            # set_dest_stream() already sets CV_DEST_STREAM internally.
            context.set_dest_stream(dest_stream)
        else:
            CV_DEST_STREAM.set(dest_stream)
        logger.info("Connected to %s, sending 200 to client...", desthost)
        await client_stream.send_all(http.http_200_connection_established)
        if trailing_data:
            with trio.fail_after(dest_timeout):
                await dest_stream.send_all(trailing_data)
        logger.info("Sent 200 to client, tunnel established.")
        return dest_stream
    except BaseException:
        if dest_stream is not None:
            with trio.CancelScope(shield=True):
                await trio.aclose_forcefully(dest_stream)
        raise


def main(args: list[str] | None = None) -> None:
    import sys

    if args is None:
        args = sys.argv[1:]
    if "--help" in args or "-h" in args:
        print("Usage: trio_http_proxy [PORT]")
        return
    logging.basicConfig(
        level=getattr(logging, get_settings().log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    trio.run(start_server)


if __name__ == "__main__":
    main()


__all__ = [
    "ConnectionContext",
    "ConnectionRecord",
    "ConnectionResultBuilder",
    "ProxyBuilder",
    "ProxyCacheBuilder",
    "ProxyCacheProtocol",
    "format_conn_prefix",
    "get_conn_limiter",
    "get_default_proxy_cache",
    "http_proxy",
    "main",
    "reset_conn_limiter",
    "start_server",
    "tunnel",
]
