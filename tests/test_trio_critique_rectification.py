"""Tests verifying rectifications of the code review by Lamport, Fowler, and Guido."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import h11
import pytest
import trio
import trio.testing as tt

from trio_http_proxy.circuit_breaker import (
    CircuitState,
    DestinationCircuitBreaker,
)
from trio_http_proxy.connection import (
    CV_CLIENT_STREAM,
    CV_CONN_ID,
    CV_CURRENT_CONTEXT,
    CV_DESTHOST,
    CV_DEST_STREAM,
    CV_PIPE_FROM,
    ConnectionContext,
    _connection_context,
)
from trio_http_proxy.errors import CircuitBreakerOpen, ProxyProtocolError
from trio_http_proxy.proxy import (
    _DEFAULT_LIMITER,
    http_proxy,
    logger,
    resolve_capacity_limiter,
    tunnel,
)


# ===========================================================================
# 1. Leslie Lamport: Concurrency Invariants & Circuit Breaker State Machine
# ===========================================================================


def test_lamport_invariant_probe_lease_timeout_epoch_fencing() -> None:
    """When a probe times out in HALF_OPEN, its epoch is fenced off monotonically.

    Invariant I2: Any probe lease timeout strictly increments breaker epoch,
    preventing any late completion by the timed-out probe from altering state.
    """
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("connection refused"), now=1000.0)
    assert cb.state == CircuitState.OPEN
    initial_epoch = cb.epoch

    # Acquire probe 1 at 1006.0 (after open TTL)
    p1 = cb.acquire_permit(now=1006.0)
    assert p1 is not None
    assert p1.is_probe is True
    assert p1.epoch == initial_epoch
    assert cb.state == CircuitState.HALF_OPEN

    # In flight before probe TTL -> gated
    assert cb.acquire_permit(now=1008.0) is None

    # After probe TTL (1006.0 + 5.0 = 1011.0), probe 1 expires.
    # Acquiring probe 2 at 1012.0 invalidates probe 1 and bumps epoch.
    p2 = cb.acquire_permit(now=1012.0)
    assert p2 is not None
    assert p2.is_probe is True
    assert p2.permit_id != p1.permit_id
    assert p2.epoch > p1.epoch
    assert cb.epoch == p2.epoch

    # Late arrival of probe 1: must be fenced off and ignored!
    cb.record_result(None, now=1012.5, permit=p1)
    assert cb.state == CircuitState.HALF_OPEN  # Not transitioned by stale probe 1!
    assert p1.was_completed is False

    # Legitimate arrival of probe 2: successfully closes circuit
    cb.record_result(None, now=1013.0, permit=p2)
    assert cb.state == CircuitState.CLOSED
    assert p2.was_completed is True


def test_lamport_invariant_inactive_probe_id_rejected_in_half_open() -> None:
    """In HALF_OPEN, completion without matching active probe_id is strictly rejected.

    Invariant I1 & I4: A completion can only transition HALF_OPEN if it matches
    the currently active probe ID.
    """
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("fail"), now=1000.0)
    assert cb.state == CircuitState.OPEN

    probe = cb.acquire_permit(now=1006.0)
    assert probe is not None
    assert cb.state == CircuitState.HALF_OPEN

    # Abort the probe (e.g. client canceled early)
    probe.abort()
    assert probe.was_aborted is True

    # Attempt to record result with the aborted permit: rejected
    cb.record_result(None, now=1007.0, permit=probe)
    assert cb.state == CircuitState.HALF_OPEN


# ===========================================================================
# 2. Guido van Rossum: Clean API, Type Safety & Structured Logging
# ===========================================================================


def test_guido_api_signature_clean_and_typed() -> None:
    """http_proxy and tunnel signatures must be properly typed without Any leaks."""
    hp_sig = inspect.signature(http_proxy)
    assert hp_sig.parameters["_nextid"].default is None
    assert hp_sig.parameters["_connect"].default is None

    tunnel_sig = inspect.signature(tunnel)
    assert tunnel_sig.parameters["_connect"].default is None


def test_guido_limiter_resolution_clean() -> None:
    """resolve_capacity_limiter flattens sentinel resolution cleanly."""
    # Sentinel resolves to default CapacityLimiter
    default_limiter = resolve_capacity_limiter(_DEFAULT_LIMITER)
    assert isinstance(default_limiter, trio.CapacityLimiter)

    # Explicit instance passes through
    custom = trio.CapacityLimiter(42)
    assert resolve_capacity_limiter(custom) is custom

    # None passes through as None
    assert resolve_capacity_limiter(None) is None


@tt.trio_test
async def test_guido_handshake_exceptions_are_logged_not_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Handshake failures must produce structured warning logs rather than silent returns."""
    caplog.set_level(logging.DEBUG)

    # 1. Test CircuitBreakerOpen logging
    class TrippedCircuitCache:
        circuit_breaker_enabled = True

        async def check_rate_limit(self, _key: str) -> None:
            pass

        async def check_circuit(self, host: str, port: int) -> Any:
            raise CircuitBreakerOpen(f"Destination {host}:{port} is open")

    proxy_stream, test_client = tt.memory_stream_pair()
    await test_client.send_all(
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
    )

    await http_proxy(proxy_stream, proxy_cache=TrippedCircuitCache())
    assert any(
        "Tunnel rejected: destination circuit breaker is open" in r.message
        and r.levelno == logging.WARNING
        for r in caplog.records
    )

    # 2. Test HTTP protocol error logging
    caplog.clear()
    bad_proxy, bad_client = tt.memory_stream_pair()
    await bad_client.send_all(b"GET / HTTP/1.1\r\n\r\n")  # Not CONNECT

    await http_proxy(bad_proxy, proxy_cache=None)
    assert any(
        "Tunnel rejected due to HTTP protocol error" in r.message
        and r.levelno == logging.WARNING
        for r in caplog.records
    )

    # 3. Test Connection failed logging
    caplog.clear()
    failing_proxy, failing_client = tt.memory_stream_pair()
    await failing_client.send_all(
        b"CONNECT failing.example.com:443 HTTP/1.1\r\nHost: failing.example.com:443\r\n\r\n"
    )

    async def fail_connect(_h: str, _p: int) -> Any:
        raise OSError("Host unreachable")

    await http_proxy(failing_proxy, _connect=fail_connect, proxy_cache=None)
    assert any(
        "Tunnel connection failed" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )

    # 4. Test Handshake timeout logging
    caplog.clear()
    slow_proxy, _ = tt.memory_stream_pair()
    from trio_http_proxy.config import Settings
    quick_settings = Settings(connect_read_timeout=0.01)
    await http_proxy(slow_proxy, settings=quick_settings, proxy_cache=None)
    assert any(
        "Tunnel handshake timed out" in r.message and r.levelno == logging.WARNING
        for r in caplog.records
    )


# ===========================================================================
# 3. Martin Fowler: Single Source of Truth for Connection Context
# ===========================================================================


def test_fowler_connection_context_is_single_source_of_truth() -> None:
    """When a ConnectionContext is active, _connection_context() reads directly from it."""
    client = tt.memory_stream_pair()[0]
    dest = tt.memory_stream_pair()[0]

    ctx = ConnectionContext(conn_id=777, client_stream=client)
    assert ctx.conn_prefix == "[conn777]"

    with ctx.activate():
        # Inside activation, _connection_context() is derived from ctx.to_dict()
        data = _connection_context()
        assert data["conn_id"] == 777
        assert data["conn_prefix"] == "[conn777]"
        assert data["desthost"] is None
        assert data["direction"] is None

        # Updating context properties updates the single source of truth
        ctx.set_desthost("api.example.com", 443)
        ctx.set_dest_stream(dest)
        data = _connection_context()
        assert data["desthost"] == "api.example.com"
        assert data["direction"] == "<>"
        assert data["conn_prefix"] == "[conn777 <> api.example.com]"

        # Setting pipe_from updates direction
        ctx.set_pipe_from(client)
        data = _connection_context()
        assert data["direction"] == "->"
        assert data["conn_prefix"] == "[conn777 -> api.example.com]"


def test_fowler_legacy_contextvars_fallback_when_no_context_active() -> None:
    """When no ConnectionContext is active, _connection_context() cleanly falls back to CV_*."""
    assert CV_CURRENT_CONTEXT.get() is None

    tok_id = CV_CONN_ID.set(888)
    tok_host = CV_DESTHOST.set("legacy.example.com")
    try:
        data = _connection_context()
        assert data["conn_id"] == 888
        assert data["desthost"] == "legacy.example.com"
    finally:
        CV_CONN_ID.reset(tok_id)
        CV_DESTHOST.reset(tok_host)
