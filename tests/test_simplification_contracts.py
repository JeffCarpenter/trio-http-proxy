"""Behavioral contracts for shared response, validation, and permit paths."""

from time import monotonic
from types import SimpleNamespace

import pytest
import trio
import trio.testing as tt
from pydantic import ValidationError

from trio_http_proxy import http, proxy
from trio_http_proxy.cache import ProxyCache
from trio_http_proxy.circuit_breaker import CircuitState, DestinationCircuitBreaker
from trio_http_proxy.config import Settings
from trio_http_proxy.errors import CircuitBreakerOpen, RateLimitError
from trio_http_proxy.proxy_cache import ConnectionRecord, ConnectionResultBuilder


@pytest.mark.parametrize(
    ("value", "seconds"),
    [
        (None, 1),
        ("17", 1),
        (0, 1),
        (-3, 1),
        (False, 1),
        (17, 17),
        (2.9, 2),
        (float("nan"), 1),
        (RateLimitError(retry_after=17), 17),
        (RateLimitError(retry_after_seconds=2.9), 2),
    ],
)
def test_retry_after_response_preserves_wire_format(value, seconds):
    assert proxy._http_429_response(value) == http.http_429_too_many_requests % seconds


@pytest.mark.parametrize(
    "field", ["circuit_breaker_period", "circuit_breaker_ttl", "rate_limit_period"]
)
@pytest.mark.parametrize("value", ["0s", "-1s", "not-a-duration"])
def test_positive_duration_validation_identifies_field(field, value):
    with pytest.raises(ValidationError) as raised:
        Settings(**{field: value})
    assert raised.value.errors()[0]["loc"] == (field,)
    assert field in str(raised.value)


@pytest.mark.parametrize("finalization", ["unresolved", "completed", "aborted"])
@pytest.mark.parametrize("exceptional_exit", [False, True])
def test_permit_context_exit_is_idempotent(finalization, exceptional_exit):
    breaker = DestinationCircuitBreaker(min_calls=1, ttl=1)
    breaker.record_result(OSError("open circuit"), now=0)
    permit = breaker.acquire_permit(now=1)
    assert permit is not None and permit.is_probe
    assert breaker.acquire_permit(now=1) is None
    error = RuntimeError("cancel probe")

    def use_permit():
        with permit:
            if finalization == "completed":
                breaker.record_result(None, now=1, permit=permit)
            elif finalization == "aborted":
                permit.abort()
            if exceptional_exit:
                raise error

    if exceptional_exit:
        with pytest.raises(RuntimeError) as raised:
            use_permit()
        assert raised.value is error
    else:
        use_permit()

    assert permit.was_completed == (finalization == "completed")
    assert permit.was_aborted == (finalization != "completed")
    assert breaker.state is (
        CircuitState.CLOSED if finalization == "completed" else CircuitState.HALF_OPEN
    )
    successor = breaker.acquire_permit(now=1)
    assert successor is not None

    # Re-finalizing an old lease must not release its successor.
    permit.__exit__(None, None, None)
    permit.abort()
    if successor.is_probe:
        assert breaker.acquire_permit(now=1) is None
    breaker.record_result(None, now=1, permit=successor)
    assert successor.was_completed
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.parametrize("timeout", [False, True])
@pytest.mark.parametrize("probe", [False, True])
@tt.trio_test
async def test_connect_failure_records_once_then_releases_permit(timeout, probe):
    error = trio.TooSlowError("timeout") if timeout else OSError("refused")
    attempts = []

    async def connect(host, port):
        attempts.append((host, port))
        raise error

    async with ProxyCache(
        resolver=lambda host: ["192.0.2.1", "192.0.2.2"],
        dns_cache_ttl="60s",
        circuit_breaker_min_calls=2,
    ) as cache:
        if probe:
            breaker = cache._circuit_manager.get_breaker("example.com", 443)
            expired = monotonic() - breaker.ttl - 1
            breaker.record_result(error, now=expired)
            breaker.record_result(error, now=expired)
            assert breaker.state is CircuitState.OPEN

        client, peer = tt.memory_stream_pair()
        async with client, peer:
            await peer.send_all(
                b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
            )
            with pytest.raises(type(error)) as raised:
                await proxy.tunnel(client, _connect=connect, proxy_cache=cache)
            assert raised.value is error
            assert attempts == [("192.0.2.1", 443), ("192.0.2.2", 443)]
            expected = (
                http.http_504_gateway_timeout if timeout else http.http_502_bad_gateway
            )
            assert await peer.receive_some(4096) == expected

        successor = await cache.check_circuit("example.com", 443)
        assert successor is not None
        assert successor.is_probe == probe
        if probe:
            # A failed attempt relinquishes its probe lease immediately.
            with pytest.raises(CircuitBreakerOpen):
                await cache.check_circuit("example.com", 443)
        else:
            # Two failed addresses count as one failed CONNECT, not two or zero.
            await cache.record_connection_result(
                ConnectionRecord("example.com", 443, error=error)
            )
            with pytest.raises(CircuitBreakerOpen):
                await cache.check_circuit("example.com", 443)
        successor.abort()


@pytest.mark.parametrize("use_builder", [False, True])
@pytest.mark.parametrize("legacy_cache", [False, True])
@pytest.mark.parametrize("async_recorder", [False, True])
@pytest.mark.parametrize("success", [False, True])
@tt.trio_test
async def test_result_dispatch_preserves_sync_async_and_legacy_support(
    use_builder, legacy_cache, async_recorder, success
):
    error = None if success else OSError("refused")
    permit = DestinationCircuitBreaker().acquire_permit() if success else None
    record = ConnectionRecord("example.com", 443, error, permit)
    calls = []

    def record_sync(*args, **kwargs):
        calls.append((args, kwargs))

    async def record_async(*args, **kwargs):
        calls.append((args, kwargs))

    class Cache:
        pass

    cache = Cache()
    method = "record_result" if legacy_cache else "record_connection_result"
    setattr(cache, method, record_async if async_recorder else record_sync)
    if use_builder:
        with pytest.warns(DeprecationWarning):
            result = ConnectionResultBuilder("example.com", 443, error, permit)
    else:
        result = record
    await proxy._record_connection_result(cache, result)

    expected = (
        (("example.com", 443, error), {"permit": permit})
        if legacy_cache
        else ((record,), {})
    )
    assert calls == [expected]


@pytest.mark.parametrize("use_builder", [False, True])
@pytest.mark.parametrize(
    "shape", ["missing", "legacy_noncallable", "modern_noncallable", "both"]
)
@tt.trio_test
async def test_recorder_compatibility_policy_is_explicit(use_builder, shape):
    calls = []
    cache = SimpleNamespace()
    if shape == "legacy_noncallable":
        cache.record_result = None
    elif shape == "modern_noncallable":
        cache.record_connection_result = None
    elif shape == "both":
        cache.record_connection_result = lambda record: calls.append(record)
        cache.record_result = lambda *args, **kwargs: pytest.fail(
            "modern recorder takes precedence"
        )

    record = ConnectionRecord("example.com", 443)
    if use_builder:
        with pytest.warns(DeprecationWarning):
            result = ConnectionResultBuilder("example.com", 443)
    else:
        result = record
    should_raise = shape == "modern_noncallable" or (
        use_builder and shape == "legacy_noncallable"
    )
    if should_raise:
        with pytest.raises(TypeError):
            await proxy._record_connection_result(cache, result)
    else:
        await proxy._record_connection_result(cache, result)
    assert calls == ([record] if shape == "both" else [])


@tt.trio_test
async def test_disabled_recording_does_not_build_invalid_legacy_record():
    with pytest.warns(DeprecationWarning):
        invalid = ConnectionResultBuilder()
    await proxy._record_connection_result(None, invalid)
    await invalid.record_to(None)
