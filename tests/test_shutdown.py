"""Tests for graceful signal shutdown and drain timeout in start_server."""

from __future__ import annotations

import os
import signal
from collections.abc import Generator
from contextlib import contextmanager, nullcontext
from functools import partial
from typing import Any

import pytest
import trio
from trio.testing import wait_all_tasks_blocked

import trio_http_proxy.proxy as proxy_module
from trio_http_proxy import (
    ProxyConfig,
    http,
    start_server,
)


def test_proxy_config_shutdown_timeout_defaults() -> None:
    """Verify shutdown_timeout default value is 15.0."""
    config = ProxyConfig()
    assert config.shutdown_timeout == 15.0


def test_proxy_config_shutdown_timeout_from_env() -> None:
    """Verify shutdown_timeout is parsed from environment variables."""
    # From TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT
    cfg1 = ProxyConfig.from_env({"TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT": "25.5"})
    assert cfg1.shutdown_timeout == 25.5

    # From PROXY_SHUTDOWN_TIMEOUT
    cfg2 = ProxyConfig.from_env({"PROXY_SHUTDOWN_TIMEOUT": "35.5"})
    assert cfg2.shutdown_timeout == 35.5

    # Precedence: TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT > PROXY_SHUTDOWN_TIMEOUT
    cfg3 = ProxyConfig.from_env(
        {
            "TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT": "12.0",
            "PROXY_SHUTDOWN_TIMEOUT": "44.0",
        }
    )
    assert cfg3.shutdown_timeout == 12.0

    legacy = ProxyConfig.from_env({"SHUTDOWN_TIMEOUT": "9.5"})
    assert legacy.shutdown_timeout == 9.5
    skipped_empty = ProxyConfig.from_env(
        {
            "TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT": "",
            "PROXY_SHUTDOWN_TIMEOUT": "7.5",
            "SHUTDOWN_TIMEOUT": "9.5",
        }
    )
    assert skipped_empty.shutdown_timeout == 7.5
    assert ProxyConfig.from_env({}).shutdown_timeout == 15.0


@pytest.mark.parametrize("value", ["0", "0.25", "60"])
def test_proxy_config_shutdown_timeout_float_values(value: str) -> None:
    cfg = ProxyConfig.from_env({"TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT": value})
    assert cfg.shutdown_timeout == float(value)


def test_proxy_config_shutdown_timeout_invalid_value() -> None:
    with pytest.raises(ValueError):
        ProxyConfig.from_env({"TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT": "not-a-number"})


@pytest.mark.trio
async def test_graceful_shutdown_drains_existing_connections(
    dest_server_port: int,
) -> None:
    """SIGTERM initiates drain: stops accepting new conns and waits for active conns to finish."""
    cfg = ProxyConfig(port=0, allow_private_networks=True, shutdown_timeout=5.0)

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(start_server, cfg)
        proxy_port = listeners[0].socket.getsockname()[1]

        # Inbound client connects and establishes tunnel
        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        connect_req = (
            f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_200_CONNECTION_ESTABLISHED

        # Send SIGTERM to initiate graceful shutdown
        os.kill(os.getpid(), signal.SIGTERM)

        # Wait until signal processing and listener cancellation have settled.
        await wait_all_tasks_blocked()

        # Verify new connections are rejected / fail to connect
        with pytest.raises(OSError):
            await trio.open_tcp_stream("127.0.0.1", proxy_port)

        # In-flight connection can still send and receive data through the tunnel
        req_line = b"GET /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        await client.send_all(req_line)
        echo_resp = await client.receive_some(4096)
        assert echo_resp.startswith(b"HTTP/1.1 200 OK\r\n")

        # Now close client connection
        await client.aclose()


@pytest.mark.trio
async def test_graceful_shutdown_sigint_drains_existing_connections(
    dest_server_port: int,
) -> None:
    """SIGINT initiates drain: stops accepting new conns and waits for active conns to finish."""
    cfg = ProxyConfig(port=0, allow_private_networks=True, shutdown_timeout=5.0)

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(start_server, cfg)
        proxy_port = listeners[0].socket.getsockname()[1]

        # Inbound client connects and establishes tunnel
        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        connect_req = (
            f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_200_CONNECTION_ESTABLISHED

        # Send SIGINT
        os.kill(os.getpid(), signal.SIGINT)

        # Wait until signal processing and listener cancellation have settled.
        await wait_all_tasks_blocked()

        # New connection should be rejected
        with pytest.raises(OSError):
            await trio.open_tcp_stream("127.0.0.1", proxy_port)

        # In-flight connection completes transfer
        req_line = b"GET /echo HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        await client.send_all(req_line)
        echo_resp = await client.receive_some(4096)
        assert echo_resp.startswith(b"HTTP/1.1 200 OK\r\n")

        await client.aclose()


@pytest.mark.trio
async def test_shutdown_timeout_cancels_stalled_connections(
    dest_server_port: int,
) -> None:
    """If connections do not drain within shutdown_timeout, they are forcibly cancelled."""
    cfg = ProxyConfig(port=0, allow_private_networks=True, shutdown_timeout=0.2)
    start_time = trio.current_time()

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(start_server, cfg)
        proxy_port = listeners[0].socket.getsockname()[1]

        # Connect and establish tunnel, but keep connection idle (do not close)
        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        connect_req = (
            f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_200_CONNECTION_ESTABLISHED

        # Send SIGTERM; server should wait up to shutdown_timeout (0.2s) and then cancel
        os.kill(os.getpid(), signal.SIGTERM)

    elapsed = trio.current_time() - start_time
    # Should have completed around 0.2s, definitely well below 5.0s
    assert 0.15 <= elapsed < 2.0

    # Client connection stream was closed/cancelled by proxy
    try:
        data = await client.receive_some(4096)
        assert data == b""
    except trio.BrokenResourceError, trio.ClosedResourceError, OSError:
        pass
    finally:
        await client.aclose()


@pytest.mark.trio
async def test_second_signal_forces_immediate_cancellation(
    dest_server_port: int,
) -> None:
    """A second signal arriving during drain cancels remaining connections immediately."""
    cfg = ProxyConfig(port=0, allow_private_networks=True, shutdown_timeout=10.0)
    start_time = trio.current_time()

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(start_server, cfg)
        proxy_port = listeners[0].socket.getsockname()[1]

        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        connect_req = (
            f"CONNECT 127.0.0.1:{dest_server_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{dest_server_port}\r\n\r\n"
        ).encode()
        await client.send_all(connect_req)
        resp = await client.receive_some(4096)
        assert resp == http.HTTP_200_CONNECTION_ESTABLISHED

        # First signal: start draining with a long timeout (10s)
        os.kill(os.getpid(), signal.SIGTERM)
        await wait_all_tasks_blocked()

        # Second signal: force immediate shutdown
        os.kill(os.getpid(), signal.SIGINT)

    elapsed = trio.current_time() - start_time
    # Finished immediately on second signal, far under 10.0s
    assert elapsed < 2.0

    await client.aclose()


@pytest.mark.trio
async def test_start_server_enable_signals_false() -> None:
    """With enable_signals=False, server starts and serves without signal receiver."""
    cfg = ProxyConfig(port=0)

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(start_server, enable_signals=False),
            cfg,
        )
        proxy_port = listeners[0].socket.getsockname()[1]

        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        # Server accepted the connection
        await client.aclose()
        # Cancel server nursery to exit test cleanly
        nursery.cancel_scope.cancel()

    assert all(listener.socket.fileno() == -1 for listener in listeners)


@pytest.mark.trio
@pytest.mark.parametrize("at_enter", [False, True])
@pytest.mark.parametrize(
    "error_type", [NotImplementedError, ValueError, RuntimeError, OSError]
)
async def test_signal_receiver_fallback_on_error(
    monkeypatch: pytest.MonkeyPatch,
    at_enter: bool,
    error_type: type[Exception],
) -> None:
    """Only receiver creation/entry failures select the no-signal fallback."""

    @contextmanager
    def failing_receiver() -> Generator[Any]:
        raise error_type("Platform does not support signals")
        yield  # pragma: no cover

    def fake_open_signal_receiver(*args: Any, **kwargs: Any) -> Any:
        if at_enter:
            return failing_receiver()
        raise error_type("Platform does not support signals")

    monkeypatch.setattr(trio, "open_signal_receiver", fake_open_signal_receiver)

    cfg = ProxyConfig(port=0)
    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(start_server, enable_signals=True),
            cfg,
        )
        proxy_port = listeners[0].socket.getsockname()[1]

        client = await trio.open_tcp_stream("127.0.0.1", proxy_port)
        await client.aclose()
        nursery.cancel_scope.cancel()

    assert all(listener.socket.fileno() == -1 for listener in listeners)


@pytest.fixture
def shutdown_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[trio.MemorySendChannel[int]]:
    """Inject signals without process-wide handlers or timing sleeps."""
    sender, receiver = trio.open_memory_channel[int](2)
    monkeypatch.setattr(
        trio, "open_signal_receiver", lambda *args: nullcontext(receiver)
    )
    with sender, receiver:
        yield sender


@pytest.mark.trio
@pytest.mark.parametrize(
    "phase", ["serving", "draining", "timeout_cleanup", "second_signal_cleanup"]
)
async def test_parent_cancellation_joins_handler_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    shutdown_signals: trio.MemorySendChannel[int],
    phase: str,
) -> None:
    """Parent cancellation during timeout cleanup must not deadlock on drained."""
    entered = trio.Event()
    cleanup_started = trio.Event()
    release_cleanup = trio.Event()
    handler_done = trio.Event()
    server_done = trio.Event()

    async def handler(client: trio.SocketStream, **kwargs: Any) -> None:
        try:
            entered.set()
            await trio.sleep_forever()
        finally:
            with trio.CancelScope(shield=True):
                cleanup_started.set()
                await release_cleanup.wait()
                await client.aclose()
                handler_done.set()

    monkeypatch.setattr(proxy_module, "http_proxy", handler)
    cfg = ProxyConfig(
        port=0, shutdown_timeout=0.02 if phase == "timeout_cleanup" else 60.0
    )

    async def run_server(
        *, task_status: trio.TaskStatus[Any] = trio.TASK_STATUS_IGNORED
    ) -> None:
        async with trio.open_nursery() as server_nursery:
            listeners = await server_nursery.start(start_server, cfg)
            task_status.started((server_nursery.cancel_scope, listeners))
        server_done.set()

    with trio.fail_after(3):
        async with trio.open_nursery() as nursery:
            server_scope, listeners = await nursery.start(run_server)
            client = await trio.open_tcp_stream(
                "127.0.0.1", listeners[0].socket.getsockname()[1]
            )
            async with client:
                await entered.wait()
                if phase != "serving":
                    await shutdown_signals.send(signal.SIGTERM)
                    await wait_all_tasks_blocked()
                    assert all(listener.socket.fileno() == -1 for listener in listeners)
                if phase == "second_signal_cleanup":
                    await shutdown_signals.send(signal.SIGINT)
                if phase in {"timeout_cleanup", "second_signal_cleanup"}:
                    await cleanup_started.wait()
                server_scope.cancel()
                await cleanup_started.wait()
                # Parent cancellation must join, rather than abandon, shielded cleanup.
                assert not server_done.is_set()
                release_cleanup.set()
                await server_done.wait()

    assert handler_done.is_set()
    assert all(listener.socket.fileno() == -1 for listener in listeners)


@pytest.mark.trio
@pytest.mark.parametrize("error_type", [ValueError, RuntimeError, OSError])
@pytest.mark.parametrize("at_exit", [False, True])
async def test_runtime_failures_do_not_select_signal_fallback(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[Exception],
    at_exit: bool,
) -> None:
    """Serving and receiver-exit errors propagate and still close listeners."""
    listeners: list[trio.SocketListener] = []
    open_listeners = trio.open_tcp_listeners
    failure = error_type("runtime failure")

    async def track_listeners(port: int) -> list[trio.SocketListener]:
        listeners.extend(await open_listeners(port))
        return listeners

    @contextmanager
    def receiver() -> Generator[None]:
        yield None
        if at_exit:
            raise failure

    async def serve(*args: Any, **kwargs: Any) -> None:
        if not at_exit:
            raise failure

    async def unexpected_fallback(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Runtime failure incorrectly triggered signal fallback")

    monkeypatch.setattr(trio, "open_tcp_listeners", track_listeners)
    monkeypatch.setattr(trio, "open_signal_receiver", lambda *args: receiver())
    monkeypatch.setattr(proxy_module, "_serve_with_signals", serve)
    monkeypatch.setattr(trio, "serve_listeners", unexpected_fallback)

    with pytest.raises(error_type) as exc_info:
        await start_server(ProxyConfig(port=0))
    assert exc_info.value is failure
    assert listeners
    assert all(listener.socket.fileno() == -1 for listener in listeners)


@pytest.mark.trio
async def test_no_signal_mode_never_opens_receiver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_receiver(*args: Any) -> Any:
        pytest.fail("enable_signals=False must not open a signal receiver")

    monkeypatch.setattr(trio, "open_signal_receiver", unexpected_receiver)
    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(start_server, enable_signals=False), ProxyConfig(port=0)
        )
        nursery.cancel_scope.cancel()
    assert all(listener.socket.fileno() == -1 for listener in listeners)


@pytest.mark.trio
async def test_zero_timeout_without_connections_closes_listeners(
    shutdown_signals: trio.MemorySendChannel[int],
) -> None:
    """Immediate shutdown is safe even if the handler nursery already drained."""
    with trio.fail_after(3):
        async with trio.open_nursery() as nursery:
            listeners = await nursery.start(
                start_server, ProxyConfig(port=0, shutdown_timeout=0.0)
            )
            await shutdown_signals.send(signal.SIGTERM)
    assert all(listener.socket.fileno() == -1 for listener in listeners)
