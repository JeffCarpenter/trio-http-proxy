"""Core HTTP CONNECT proxy server implementation using Trio structured concurrency."""

from __future__ import annotations

import itertools
import logging
from collections.abc import Awaitable, Callable
from functools import cache, partial
from typing import Any

import h11
import trio

from . import http
from .config import ProxyConfig
from .errors import ProxyProtocolError, RequestTooLargeError
from .protocol import process_as_connect_request
from .streaming import pipe_with_timeout

logger = logging.getLogger(__name__)

_conn_ids = itertools.count(1)


@cache
def get_conn_limiter(maxconns: int = 1000) -> trio.CapacityLimiter:
    """Shared connection-capacity limiter for backpressure."""
    return trio.CapacityLimiter(maxconns)


def reset_conn_limiter() -> None:
    """Reset the memoized connection capacity limiter."""
    get_conn_limiter.cache_clear()


async def tunnel(
    client_stream: trio.abc.Stream,
    *,
    config: ProxyConfig | None = None,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] = trio.open_tcp_stream,
) -> trio.abc.Stream:
    """Perform the HTTP CONNECT handshake and establish the upstream tunnel.

    Reads the CONNECT request, opens a TCP connection to the destination,
    and returns the upstream stream. Replies to the client with appropriate
    HTTP status codes on success or failure.
    """
    cfg = config or ProxyConfig()

    try:
        desthost, destport, trailing_data = await process_as_connect_request(
            client_stream,
            config=cfg,
        )
    except RequestTooLargeError:
        logger.warning("CONNECT request too large, sending 414 to client")
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.HTTP_414_URI_TOO_LONG)
        raise
    except ProxyProtocolError, h11.ProtocolError:
        logger.warning("Bad CONNECT request, sending 400 to client")
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.HTTP_400_BAD_REQUEST)
        raise
    except trio.TooSlowError:
        logger.warning("CONNECT request timed out, sending 408 to client")
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.HTTP_408_REQUEST_TIMEOUT)
        raise

    logger.info("Connecting to %s:%d...", desthost, destport)
    try:
        with trio.fail_after(cfg.dest_connect_timeout):
            dest_stream = await _connect(desthost, destport)
    except OSError:
        logger.warning(
            "Could not connect to %s:%d, sending 502 to client", desthost, destport
        )
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.HTTP_502_BAD_GATEWAY)
        raise
    except trio.TooSlowError:
        logger.warning(
            "Timed out connecting to %s:%d, sending 504 to client", desthost, destport
        )
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.HTTP_504_GATEWAY_TIMEOUT)
        raise

    if trailing_data:
        await dest_stream.send_all(trailing_data)

    logger.info("Connected to %s:%d, sending 200 to client...", desthost, destport)
    await client_stream.send_all(http.HTTP_200_CONNECTION_ESTABLISHED)
    return dest_stream


async def http_proxy(
    client_stream: trio.abc.Stream,
    *,
    config: ProxyConfig | None = None,
    limiter: trio.CapacityLimiter | None = None,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] = trio.open_tcp_stream,
    _nextid: Callable[[], int] = _conn_ids.__next__,
) -> None:
    """Handle a single inbound client connection."""
    cfg = config or ProxyConfig()
    conn_limiter = limiter if limiter is not None else get_conn_limiter(cfg.maxconns)
    conn_id = _nextid()

    logger.debug("[conn%d] Accepted connection", conn_id)

    async with conn_limiter:
        try:
            dest_stream = await tunnel(
                client_stream,
                config=cfg,
                _connect=_connect,
            )
        except (
            RequestTooLargeError,
            ProxyProtocolError,
            h11.ProtocolError,
            trio.TooSlowError,
            OSError,
        ):
            return

        async with dest_stream:
            try:
                async with trio.open_nursery() as nursery:
                    nursery.start_soon(
                        pipe_with_timeout,
                        client_stream,
                        dest_stream,
                        cfg.bufmaxsz,
                        cfg.pipe_idle_timeout,
                        cfg.pipe_send_timeout,
                    )
                    nursery.start_soon(
                        pipe_with_timeout,
                        dest_stream,
                        client_stream,
                        cfg.bufmaxsz,
                        cfg.pipe_idle_timeout,
                        cfg.pipe_send_timeout,
                    )
            except* (
                trio.BrokenResourceError,
                trio.ClosedResourceError,
                trio.TooSlowError,
                OSError,
            ) as excgroup:
                logger.debug(
                    "[conn%d] Tunnel ended with exception: %s", conn_id, excgroup
                )

    logger.debug("[conn%d] Connection closed", conn_id)


async def start_server(
    config: ProxyConfig | None = None,
    *,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
    _serve: Any = trio.serve_tcp,
) -> None:
    """Run the HTTP proxy TCP server."""
    cfg = config or ProxyConfig.from_env()
    limiter = get_conn_limiter(cfg.maxconns)
    handler = partial(http_proxy, config=cfg, limiter=limiter)
    logger.info("Starting proxy on port %d...", cfg.port)
    await _serve(handler, cfg.port, task_status=task_status)


def main() -> None:
    """CLI entry point for running the proxy server."""
    config = ProxyConfig.from_env()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    trio.run(start_server, config)


__all__ = [
    "get_conn_limiter",
    "http_proxy",
    "main",
    "reset_conn_limiter",
    "start_server",
    "tunnel",
]
