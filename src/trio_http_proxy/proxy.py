"""A small HTTP CONNECT server using Trio's TCP and task lifecycle primitives."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Awaitable, Callable
from functools import partial

import trio

from . import http
from .errors import ProxyProtocolError
from .protocol import ConnectParser, ConnectRequest, error_response
from .streaming import pipe_with_timeout

logger = logging.getLogger(__name__)


async def _reply_error(stream: trio.abc.SendStream, response: bytes) -> None:
    with trio.move_on_after(5):
        await stream.send_all(response)


async def process_as_connect_request(
    stream: trio.abc.ReceiveStream,
    *,
    bufmaxsz: int = 8192,
    maxreqsz: int = 16384,
    read_timeout: float = 10.0,
) -> ConnectRequest:
    """Read a stream expected to contain a valid HTTP CONNECT request.

    Drives the sans-io ConnectParser: receive, feed, repeat.
    Returns a ConnectRequest, which unpacks as (host, port, trailing_bytes).
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout.
    """
    logger.debug("Reading CONNECT request...")
    parser = ConnectParser(maxreqsz=maxreqsz)
    with trio.fail_after(read_timeout):
        while (request := parser.next_request()) is None:
            chunk = await stream.receive_some(bufmaxsz)
            if chunk:
                parser.receive_data(chunk)
            else:
                parser.receive_eof()
    return request


async def tunnel(
    client_stream: trio.abc.Stream,
    *,
    read_timeout: float = 10.0,
    connect_timeout: float = 10.0,
    maxreqsz: int = 16384,
    send_timeout: float = 30.0,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] | None = None,
) -> trio.abc.Stream:
    """Validate CONNECT, open the destination and transfer ownership to the caller.

    On failure, send an HTTP error when possible and close any opened upstream.
    The caller owns the client stream throughout. send_timeout bounds the
    handshake writes: early tunnel bytes upstream and the 200 reply to the client.
    """
    try:
        host, port, trailing_data = await process_as_connect_request(
            client_stream, read_timeout=read_timeout, maxreqsz=maxreqsz
        )
    except ProxyProtocolError as exc:
        await _reply_error(client_stream, error_response(exc))
        raise
    except trio.TooSlowError:
        await _reply_error(client_stream, http.HTTP_408_REQUEST_TIMEOUT)
        raise

    logger.info("Connecting to %s:%d", host, port)
    try:
        with trio.fail_after(connect_timeout):
            dest_stream = await (_connect or trio.open_tcp_stream)(host, port)
    except OSError:
        await _reply_error(client_stream, http.HTTP_502_BAD_GATEWAY)
        raise
    except trio.TooSlowError:
        await _reply_error(client_stream, http.HTTP_504_GATEWAY_TIMEOUT)
        raise

    try:
        with trio.fail_after(send_timeout):
            if trailing_data:
                await dest_stream.send_all(trailing_data)
            await client_stream.send_all(http.HTTP_200_CONNECTION_ESTABLISHED)
    except BaseException:
        # Ownership passes to the caller only after the handshake succeeds.
        await trio.aclose_forcefully(dest_stream)
        raise
    return dest_stream


async def http_proxy(
    client_stream: trio.abc.Stream,
    *,
    idle_timeout: float = 300.0,
    send_timeout: float = 30.0,
    read_timeout: float = 10.0,
    connect_timeout: float = 10.0,
    maxreqsz: int = 16384,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] | None = None,
) -> None:
    """Handle one client, closing both streams on EOF, failure or cancellation."""
    async with client_stream:
        try:
            dest_stream = await tunnel(
                client_stream,
                read_timeout=read_timeout,
                connect_timeout=connect_timeout,
                maxreqsz=maxreqsz,
                send_timeout=send_timeout,
                _connect=_connect,
            )
        except (
            ProxyProtocolError,
            trio.TooSlowError,
            trio.BrokenResourceError,
            trio.ClosedResourceError,
            OSError,
        ):
            logger.debug("CONNECT failed", exc_info=True)
            return

        async with dest_stream:
            try:
                async with trio.open_nursery() as nursery:
                    for source, target in (
                        (client_stream, dest_stream),
                        (dest_stream, client_stream),
                    ):
                        nursery.start_soon(
                            partial(
                                pipe_with_timeout,
                                source,
                                target,
                                idle_timeout=idle_timeout,
                                send_timeout=send_timeout,
                            )
                        )
            except* (
                trio.BrokenResourceError,
                trio.ClosedResourceError,
                trio.TooSlowError,
                OSError,
            ):
                logger.debug("Tunnel closed", exc_info=True)


async def start_server(
    port: int = 8080,
    *,
    host: str = "127.0.0.1",
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
) -> None:
    """Serve CONNECT tunnels until cancelled; no custom shutdown protocol."""
    await trio.serve_tcp(http_proxy, port, host=host, task_status=task_status)


def main() -> None:
    parser = argparse.ArgumentParser(description="Nano HTTP CONNECT proxy")
    parser.add_argument(
        "--host", default="127.0.0.1", help="listen address (127.0.0.1)"
    )
    parser.add_argument("--port", type=int, default=8080, help="listen port (8080)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        trio.run(partial(start_server, args.port, host=args.host))
    except KeyboardInterrupt:
        pass


__all__ = [
    "http_proxy",
    "main",
    "process_as_connect_request",
    "start_server",
    "tunnel",
]
