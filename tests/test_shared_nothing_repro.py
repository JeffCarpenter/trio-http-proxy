"""Tests verifying shared-nothing architecture invariants and audit remediations."""

from __future__ import annotations

from functools import partial
from typing import Any, cast

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.circuit_breaker import (
    CircuitBreakerManager,
    CircuitPermit,
    CircuitState,
    DestinationCircuitBreaker,
)
from trio_http_proxy.config import Settings
from trio_http_proxy.config.builder import ProxyCacheBuilder
from trio_http_proxy.connection import (
    _DEFAULT_PROXY_CACHE_SENTINEL,
    CV_CLIENT_STREAM,
    CV_CONN_ID,
    CV_DESTHOST,
    CV_DEST_STREAM,
    ConnectionContext,
    format_conn_prefix,
    resolve_proxy_cache,
)
from trio_http_proxy.errors import ProxyProtocolError, RateLimitError
from trio_http_proxy.protocol import process_as_connect_request
from trio_http_proxy.proxy import (
    _DEFAULT_LIMITER,
    ConnectionResultBuilder,
    ProxyBuilder,
    _http_429_response,
    http_proxy,
    tunnel,
)
from trio_http_proxy.proxy_cache import ProxyCache


# ---------------------------------------------------------------------------
# 1. ConnectionContext isolation & lifecycle
# ---------------------------------------------------------------------------
def test_connection_context_isolation():
    client1, _ = tt.memory_stream_pair()
    client2, _ = tt.memory_stream_pair()

    ctx1 = ConnectionContext(conn_id=101, client_stream=client1)
    ctx2 = ConnectionContext(conn_id=102, client_stream=client2)

    assert ctx1.conn_id == 101
    assert ctx2.conn_id == 102
    assert ctx1.conn_prefix == "[conn101]"
    assert ctx2.conn_prefix == "[conn102]"

    ctx1.set_desthost("alpha.example.com", 443)
    assert ctx1.desthost == "alpha.example.com"
    assert ctx2.desthost is None

    with ctx1.activate():
        assert CV_CONN_ID.get() == 101
        assert CV_CLIENT_STREAM.get() is client1
        assert CV_DESTHOST.get() == "alpha.example.com"
        assert format_conn_prefix() == "[conn101]"

        with ctx2.activate():
            assert CV_CONN_ID.get() == 102
            assert CV_CLIENT_STREAM.get() is client2
            assert CV_DESTHOST.get() is None
            assert format_conn_prefix() == "[conn102]"

        assert CV_CONN_ID.get() == 101
        assert CV_DESTHOST.get() == "alpha.example.com"


# ---------------------------------------------------------------------------
# 2. ProxyBuilder passing settings to connection pipeline (Finding 12)
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_proxy_builder_wires_custom_settings():
    settings = (
        Settings()
        .model_copy(
            update={
                "bufmaxsz": 4096,
                "connect_read_timeout": 0.05,
                "dest_connect_timeout": 0.05,
            }
        )
    )
    captured_settings = []

    async def custom_server(stream, **kwargs):
        captured_settings.append(kwargs.get("settings"))

    builder = ProxyBuilder(settings=settings).with_server(custom_server)

    async def fake_serve(handler, port, task_status=trio.TASK_STATUS_IGNORED):
        proxy_client, _ = tt.memory_stream_pair()
        await handler(proxy_client)
        task_status.started()

    await builder.run(_serve=fake_serve)

    assert len(captured_settings) == 1
    assert captured_settings[0] is not None
    assert captured_settings[0].connect_read_timeout == 0.05
    assert captured_settings[0].bufmaxsz == 4096


# ---------------------------------------------------------------------------
# 3. Connection capacity limiter gating does not deadlock (Finding 11)
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_listener_gating_no_double_limiter_deadlock():
    limiter = trio.CapacityLimiter(1)

    proxy_client, test_client = tt.memory_stream_pair()
    token = object()
    await limiter.acquire_on_behalf_of(token)

    try:
        # http_proxy with limiter=None must not re-acquire limiter
        async def run_handler():
            await test_client.send_all(b"GARBAGE REQUEST\r\n\r\n")
            await test_client.aclose()
            # If http_proxy tried to acquire a default limiter that had capacity 1,
            # or tried to acquire `limiter`, it would deadlock if shared.
            # Passing limiter=None ensures it completes without deadlocking!
            await http_proxy(proxy_client, limiter=None, proxy_cache=None)

        with trio.fail_after(1.0):
            await run_handler()
    finally:
        limiter.release_on_behalf_of(token)


# ---------------------------------------------------------------------------
# 4. ProxyCacheBuilder context manager isolation (Finding 10)
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_proxy_cache_builder_reentrant_context_manager():
    builder = ProxyCacheBuilder().with_cache_url("mem://")

    async with builder as cache1:
        assert cache1.is_running is True
        async with builder as cache2:
            assert cache2.is_running is True
            assert cache1 is not cache2
        # After cache2 exits, cache2 is stopped but cache1 MUST still be running
        assert cache2.is_stopped is True
        assert cache1.is_running is True

    assert cache1.is_stopped is True


# ---------------------------------------------------------------------------
# 5. ConnectionResultBuilder validation & permit forwarding (Findings 20, 27)
# ---------------------------------------------------------------------------
def test_connection_result_builder_validation():
    with pytest.raises(ValueError, match="host must be specified"):
        ConnectionResultBuilder(host="", port=80).build()

    with pytest.raises(ValueError, match="host must be specified"):
        ConnectionResultBuilder(host="   ", port=80).build()

    with pytest.raises(ValueError, match="port must be specified"):
        ConnectionResultBuilder(host="example.com").build()

    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ConnectionResultBuilder(host="example.com", port=0).build()

    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ConnectionResultBuilder(host="example.com", port=70000).build()


@tt.trio_test
async def test_connection_result_builder_fallback_forwards_permit():
    received_permit = []

    class LegacyRecorder:
        def record_result(self, host, port, error, permit=None):
            received_permit.append(permit)

    dummy_permit = cast(CircuitPermit, object())
    builder = (
        ConnectionResultBuilder("dest.local", 443)
        .with_success()
        .with_permit(dummy_permit)
    )
    await builder.record_to(LegacyRecorder())

    assert len(received_permit) == 1
    assert received_permit[0] is dummy_permit


# ---------------------------------------------------------------------------
# 6. CircuitBreakerManager eviction & max_destinations bounds (Findings 21, 23)
# ---------------------------------------------------------------------------
def test_circuit_breaker_manager_max_destinations_bounded():
    cbm = CircuitBreakerManager(max_destinations=0)
    assert cbm.max_destinations >= 1
    breaker = cbm.get_breaker("example.com", 443)
    assert breaker is not None


def test_circuit_breaker_manager_eviction_protects_open_breakers():
    cbm = CircuitBreakerManager(
        max_destinations=2,
        min_calls=1,
        errors_rate=1,
        ttl=60.0,
    )
    # Trip breaker 1 to OPEN
    b1 = cbm.get_breaker("open.example.com", 443)
    p1 = b1.acquire_permit()
    b1.record_result(OSError("fail"), permit=p1)
    assert b1.state == CircuitState.OPEN

    # Breaker 2 is CLOSED
    b2 = cbm.get_breaker("closed.example.com", 443)
    assert b2.state == CircuitState.CLOSED

    # Now add third breaker, triggering eviction. It MUST evict b2 (CLOSED), NOT b1 (OPEN)
    b3 = cbm.get_breaker("new.example.com", 443)
    assert b3 is not None

    # b1 must still be in cache and OPEN
    key1 = ("open.example.com", 443)
    assert key1 in cbm._breakers
    assert cbm._breakers[key1].state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# 7. Strict RFC 7231 authority-form target validation (Finding 13)
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_strict_authority_form_validation():
    invalid_targets = [
        b"user@example.com:443",
        b"@example.com:443",
        b":@example.com:443",
        b"example.com:443/path",
        b"example.com:443?query",
        b"example.com:443#frag",
    ]

    for target in invalid_targets:
        proxy_client, test_client = tt.memory_stream_pair()
        req = b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n"
        await test_client.send_all(req)

        with pytest.raises(ProxyProtocolError, match="not authority-form"):
            await process_as_connect_request(proxy_client)


# ---------------------------------------------------------------------------
# 8. RateLimitError & _http_429_response robustness (Findings 31, 33)
# ---------------------------------------------------------------------------
def test_rate_limit_error_accepts_arbitrary_kwargs():
    err = RateLimitError("rate limit exceeded", retry_after=30, extra_param="ignored")
    assert err.retry_after == 30
    assert err.retry_after_seconds == 30.0


def test_http_429_response_handles_non_numeric_exception():
    err = RateLimitError("rate limit")
    cast(Any, err).retry_after = "invalid_number"
    resp = _http_429_response(err)
    assert b"HTTP/1.1 429 Too Many Requests" in resp
    assert b"Retry-After: 1" in resp

    # Even with raw exception instance passed
    resp2 = _http_429_response(Exception("non-numeric"))
    assert b"HTTP/1.1 429 Too Many Requests" in resp2
    assert b"Retry-After: 1" in resp2


# ---------------------------------------------------------------------------
# 9. Settings constructor safety & sub-model unpacking (Findings 2, 3, 4)
# ---------------------------------------------------------------------------
def test_settings_forbids_positional_arguments():
    with pytest.raises(TypeError, match="positional"):
        Settings("positional_arg")


def test_settings_unpacks_sub_models():
    settings = Settings(
        proxy={"port": 8888, "maxconns": 50},
        cache={"cache_url": "redis://localhost:6379"},
    )
    assert settings.port == 8888
    assert settings.maxconns == 50
    assert settings.cache_url == "redis://localhost:6379"


def test_settings_rate_limit_backward_compatibility():
    settings = Settings(rate_limit=42)
    assert settings.rate_limit == 42
    assert settings.rate_limit_count == 42


# ---------------------------------------------------------------------------
# 10. Luminaries' Review Rectifications: Sentinels, Resolution, and Teardown
# ---------------------------------------------------------------------------
def test_sentinels_representation_and_boolean_falsiness():
    assert repr(_DEFAULT_LIMITER) == "<DEFAULT_LIMITER>"
    assert not _DEFAULT_LIMITER
    assert repr(_DEFAULT_PROXY_CACHE_SENTINEL) == "<DEFAULT_PROXY_CACHE>"
    assert not _DEFAULT_PROXY_CACHE_SENTINEL


def test_resolve_proxy_cache_precedence():
    # 1. Explicit argument wins
    custom_cache = object()
    assert resolve_proxy_cache(custom_cache) is custom_cache

    # 2. Context settings win over ambient/fallback
    ctx_cache = object()
    ctx = ConnectionContext(proxy_cache=ctx_cache)
    assert resolve_proxy_cache(context=ctx) is ctx_cache

    # 3. Default factory when nothing is provided
    default_obj = object()
    assert (
        resolve_proxy_cache(default_factory=lambda: default_obj)
        is default_obj
    )


def test_circuit_breaker_open_state_none_opened_at_recovery():
    breaker = DestinationCircuitBreaker()
    breaker.state = CircuitState.OPEN
    breaker.opened_at = None
    # acquire_permit should recover opened_at without deadlock or error
    now = 100.0
    permit = breaker.acquire_permit(now=now)
    assert permit is None
    assert breaker.opened_at == now
    # After TTL, it transitions to HALF_OPEN
    permit2 = breaker.acquire_permit(now=now + breaker.ttl + 1.0)
    assert permit2 is not None
    assert breaker.state == CircuitState.HALF_OPEN


async def test_http_proxy_prompt_teardown_on_dest_eof():
    # Tests Leslie Lamport's invariant: prompt teardown when destination stream closes
    # Verify that client->dest pump doesn't hang for idle timeout when dest closes.
    client_stream, client_peer = tt.memory_stream_pair()
    dest_stream, dest_peer = tt.memory_stream_pair()

    async def fake_connect(host: str, port: int):
        return dest_stream

    # Client sends CONNECT request with mandatory Host header
    await client_peer.send_all(
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                http_proxy,
                client_stream,
                _connect=fake_connect,
                proxy_cache=None,
                limiter=None,
            )
        )

        # Wait for 200 Connection Established
        established = await client_peer.receive_some(1024)
        assert b"200 Connection established" in established

        # Destination sends some payload then closes (origin server hangup)
        await dest_peer.send_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
        await dest_peer.aclose()

        # Destination data should arrive on client_peer
        data = await client_peer.receive_some(1024)
        assert b"HTTP/1.1 200 OK" in data

        # http_proxy should terminate promptly without hanging
