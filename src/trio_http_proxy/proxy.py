"""Core HTTP CONNECT proxy server implementation using Trio structured concurrency."""

from __future__ import annotations

import itertools
import logging
import signal
from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from functools import cache, partial
from typing import Any

import h11
import trio

from . import http
from .config import ProxyConfig
from .errors import ProxyProtocolError, RequestTooLargeError
from .protocol import is_address_allowed, is_target_allowed, process_as_connect_request
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


async def _open_target_stream(
    host: str, port: int, *, config: ProxyConfig
) -> trio.SocketStream:
    results = await trio.socket.getaddrinfo(host, port, type=trio.socket.SOCK_STREAM)
    addresses = tuple(
        dict.fromkeys(
            str(sockaddr[0])
            + (f"%{sockaddr[3]}" if len(sockaddr) == 4 and sockaddr[3] else "")
            for family, _, _, _, sockaddr in results
            if family in (trio.socket.AF_INET, trio.socket.AF_INET6)
        )
    )
    if not addresses:
        raise OSError("No addresses found for target")
    # Reject mixed public/private DNS answers and never reconnect by hostname.
    if not all(is_address_allowed(address, port, config) for address in addresses):
        raise ProxyProtocolError("Resolved target address denied by policy")
    return await _connect_to_addresses(addresses, port)


async def _connect_to_addresses(
    addresses: tuple[str, ...], port: int
) -> trio.SocketStream:
    """Race vetted addresses with Trio's default 250 ms connection stagger."""
    winner: trio.SocketStream | None = None
    errors: list[OSError] = []

    async def attempt(address: str, failed: trio.Event) -> None:
        nonlocal winner
        try:
            stream = await trio.open_tcp_stream(address, port)
        except OSError as exc:
            errors.append(exc)
            failed.set()
            return
        if winner is None:
            winner = stream
            nursery.cancel_scope.cancel()
        else:
            await trio.aclose_forcefully(stream)

    try:
        async with trio.open_nursery() as nursery:
            for address in addresses:
                failed = trio.Event()
                nursery.start_soon(attempt, address, failed)
                with trio.move_on_after(0.25):
                    await failed.wait()
    except BaseException:
        if winner is not None:
            await trio.aclose_forcefully(winner)
        raise
    if winner is not None:
        return winner
    raise OSError("All target connection attempts failed") from ExceptionGroup(
        "Target connection failures", errors
    )


async def tunnel(
    client_stream: trio.abc.Stream,
    *,
    config: ProxyConfig | None = None,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] | None = None,
) -> trio.abc.Stream:
    """Perform the HTTP CONNECT handshake and establish the upstream tunnel.

    Reads the CONNECT request, opens a TCP connection to the destination,
    and returns the upstream stream. Replies to the client with appropriate
    HTTP status codes on success or failure. An explicitly injected connector
    is trusted to enforce DNS/address safety itself.
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

    if not is_target_allowed(desthost, destport, cfg):
        logger.warning(
            "Target '%s:%d' forbidden by policy, sending 403 to client",
            desthost,
            destport,
        )
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.RESP_403_FORBIDDEN)
        raise ProxyProtocolError(f"Target '{desthost}:{destport}' forbidden by policy")

    logger.info("Connecting to %s:%d...", desthost, destport)
    try:
        with trio.fail_after(cfg.dest_connect_timeout):
            dest_stream = (
                await _connect(desthost, destport)
                if _connect is not None
                else await _open_target_stream(desthost, destport, config=cfg)
            )
    except ProxyProtocolError:
        logger.warning("Resolved target denied by policy, sending 403 to client")
        with trio.move_on_after(5.0):
            await client_stream.send_all(http.RESP_403_FORBIDDEN)
        raise
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

    try:
        if trailing_data:
            await dest_stream.send_all(trailing_data)
        logger.info("Connected to %s:%d, sending 200 to client...", desthost, destport)
        await client_stream.send_all(http.HTTP_200_CONNECTION_ESTABLISHED)
    except BaseException:
        # Ownership passes to the caller only after the handshake succeeds.
        await trio.aclose_forcefully(dest_stream)
        raise
    return dest_stream


async def http_proxy(
    client_stream: trio.abc.Stream,
    *,
    config: ProxyConfig | None = None,
    limiter: trio.CapacityLimiter | None = None,
    _connect: Callable[[str, int], Awaitable[trio.abc.Stream]] | None = None,
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
            trio.BrokenResourceError,
            trio.ClosedResourceError,
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


async def _serve_with_signals(
    handler: Callable[..., Awaitable[Any]],
    listeners: list[trio.SocketListener],
    cfg: ProxyConfig,
    *,
    task_status: trio.TaskStatus[list[trio.SocketListener]],
    signal_aiter: Any,
) -> None:
    stop_accepting = trio.Event()
    drained = trio.Event()
    listener_scope = trio.CancelScope()

    async def conn_runner(
        *,
        task_status: trio.TaskStatus[trio.Nursery] = trio.TASK_STATUS_IGNORED,
    ) -> None:
        try:
            async with trio.open_nursery() as conn_nursery:
                task_status.started(conn_nursery)
                await stop_accepting.wait()
        finally:
            drained.set()

    async with trio.open_nursery() as supervisor:
        conn_nursery = await supervisor.start(conn_runner)

        async def serve_listeners_task() -> None:
            with listener_scope:
                await trio.serve_listeners(
                    handler,
                    listeners,
                    handler_nursery=conn_nursery,
                    task_status=task_status,
                )

        supervisor.start_soon(serve_listeners_task)

        # Wait for first signal
        async for sig in signal_aiter:
            logger.info(
                "Received signal %s, initiating graceful shutdown (timeout=%.1fs)...",
                sig,
                cfg.shutdown_timeout,
            )
            listener_scope.cancel()
            stop_accepting.set()
            break

        if not stop_accepting.is_set():
            listener_scope.cancel()
            stop_accepting.set()

        # Wait for conn_nursery to drain up to shutdown_timeout seconds
        with trio.move_on_after(cfg.shutdown_timeout) as drain_scope:
            async with trio.open_nursery() as drain_nursery:

                async def wait_second_signal() -> None:
                    try:
                        sig2 = await anext(signal_aiter)
                        logger.warning(
                            "Received second signal %s, forcing immediate shutdown...",
                            sig2,
                        )
                        conn_nursery.cancel_scope.cancel()
                        drain_nursery.cancel_scope.cancel()
                    except StopAsyncIteration, trio.Cancelled:
                        pass

                drain_nursery.start_soon(wait_second_signal)
                await drained.wait()
                drain_nursery.cancel_scope.cancel()

        if drain_scope.cancelled_caught:
            logger.warning(
                "Drain timeout (%.1fs) expired before connections drained; forcing shutdown...",
                cfg.shutdown_timeout,
            )
            conn_nursery.cancel_scope.cancel()
            # The supervisor nursery joins handlers without shielding parent cancellation.


async def start_server(
    config: ProxyConfig | None = None,
    enable_signals: bool = True,
    *,
    task_status: trio.TaskStatus[list[trio.SocketListener]] = trio.TASK_STATUS_IGNORED,
    _serve: Any = None,
) -> None:
    """Run the HTTP proxy TCP server."""
    cfg = config or ProxyConfig.from_env()
    limiter = get_conn_limiter(cfg.maxconns)
    handler = partial(http_proxy, config=cfg, limiter=limiter)
    logger.info("Starting proxy on port %d...", cfg.port)

    if _serve is not None:
        await _serve(handler, cfg.port, task_status=task_status)
        return

    listeners = await trio.open_tcp_listeners(cfg.port)
    try:
        if enable_signals:
            with ExitStack() as signal_stack:
                try:
                    signal_aiter = signal_stack.enter_context(
                        trio.open_signal_receiver(signal.SIGTERM, signal.SIGINT)
                    )
                except (NotImplementedError, ValueError, RuntimeError, OSError) as exc:
                    logger.info(
                        "Signal receiver unavailable (%s), running without signal handling",
                        exc,
                    )
                else:
                    await _serve_with_signals(
                        handler,
                        listeners,
                        cfg,
                        task_status=task_status,
                        signal_aiter=signal_aiter,
                    )
                    return

        async with trio.open_nursery() as conn_nursery:
            await trio.serve_listeners(
                handler,
                listeners,
                handler_nursery=conn_nursery,
                task_status=task_status,
            )
    finally:
        for listener in listeners:
            with trio.CancelScope(shield=True):
                await listener.aclose()


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
