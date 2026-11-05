"""Comprehensive tests for ProxyCacheBuilder and ProxyBuilder API."""

from __future__ import annotations

from typing import cast

import pytest
import trio.testing as tt

from trio_http_proxy.cache import ProxyCache, ProxyCacheBuilder
from trio_http_proxy.circuit_breaker import CircuitPermit, DestinationCircuitBreaker
from trio_http_proxy.config import Settings
from trio_http_proxy.errors import CircuitBreakerOpen
from trio_http_proxy.proxy import (
    ConnectionRecord,
    ConnectionResultBuilder,
    ProxyBuilder,
)


def test_proxy_cache_builder_defaults():
    builder = ProxyCacheBuilder()
    pc = builder.build()
    assert pc.cache_url == "mem://"
    assert pc.enabled is True
    assert pc.namespace == "trio_http_proxy"
    assert pc.dns_cache_ttl == "0s"
    assert pc.failure_policy == "fail-open"
    assert pc.circuit_breaker_enabled is True
    assert pc.circuit_breaker_errors_rate == 50
    assert pc.rate_limit_enabled is False


def test_proxy_cache_builder_individual_helpers():
    builder = (
        ProxyCacheBuilder()
        .with_cache_url("mem://helpers")
        .with_namespace("helper_ns")
        .with_dns_cache("15s")
        .with_failure_policy("fail-closed")
        .with_probe_interval(25.0)
        .with_startup_timeout(3.5)
        .with_cache_enabled(False)
    )
    pc = builder.build()
    assert pc.cache_url == "mem://helpers"
    assert pc.namespace == "helper_ns"
    assert pc.dns_cache_ttl == "15s"
    assert pc.failure_policy == "fail-closed"
    assert pc.probe_interval == 25.0
    assert pc.startup_timeout == 3.5
    assert pc.enabled is False


def test_proxy_cache_builder_with_settings():
    base = Settings(cache_url="mem://base", rate_limit=5)
    builder = ProxyCacheBuilder(settings=base).with_rate_limit(15)
    pc = builder.build()
    assert pc.cache_url == "mem://base"
    assert pc.rate_limit == 15


def test_proxy_builder_configuration():
    pb = (
        ProxyBuilder()
        .with_port(9090)
        .with_buffer_sizes(bufmaxsz=8192, maxreqsz=32768)
        .with_timeouts(
            connect_read=2.5,
            dest_connect=3.5,
            pipe_idle=120.0,
            pipe_send=15.0,
        )
        .with_maxconns(500)
        .with_log_level("DEBUG")
    )
    settings = pb.build_settings()
    assert settings.port == 9090
    assert settings.bufmaxsz == 8192
    assert settings.maxreqsz == 32768
    assert settings.connect_read_timeout == 2.5
    assert settings.dest_connect_timeout == 3.5
    assert settings.pipe_idle_timeout == 120.0
    assert settings.pipe_send_timeout == 15.0
    assert settings.maxconns == 500
    assert settings.log_level == "DEBUG"


@pytest.fixture
def permit() -> CircuitPermit:
    lease = DestinationCircuitBreaker().acquire_permit()
    assert lease is not None
    return lease


def test_connection_result_builder_permit(permit):
    with pytest.deprecated_call():
        builder = ConnectionResultBuilder(host="svc.local", port=80).with_permit(permit)
    record = builder.build()
    assert record.has_permit is True
    assert record.permit is permit


def test_connection_result_builder_validation():
    with pytest.deprecated_call(), pytest.raises(
        ValueError, match="host must be specified"
    ):
        ConnectionResultBuilder().with_port(80).build()

    with pytest.deprecated_call(), pytest.raises(
        ValueError, match="port must be specified"
    ):
        ConnectionResultBuilder().with_host("example.com").build()


def test_proxy_cache_classmethod_result_builder():
    with pytest.deprecated_call():
        builder = ProxyCache.result_builder("dest.com", 9000)
    record = builder.with_success().build()
    assert record.host == "dest.com"
    assert record.port == 9000
    assert record.is_success is True


@pytest.mark.parametrize("use_builder", [False, True])
@pytest.mark.parametrize("success", [False, True])
@tt.trio_test
async def test_connection_recording_updates_real_breaker(use_builder, success):
    async with ProxyCache(circuit_breaker_min_calls=1) as cache:
        permit = await cache.check_circuit("test.local", 443)
        assert permit is not None
        error = None if success else OSError("connection refused")
        if use_builder:
            with pytest.deprecated_call():
                builder = ConnectionResultBuilder("test.local", 443)
            if success:
                builder.with_success().with_permit(permit)
            else:
                builder.with_error(error)
            await builder.record_to(cache)
        else:
            await cache.record_connection_result(
                ConnectionRecord("test.local", 443, error, permit if success else None)
            )

        if success:
            assert permit.was_completed
            successor = await cache.check_circuit("test.local", 443)
            assert successor is not None
            successor.abort()
        else:
            with pytest.raises(CircuitBreakerOpen):
                await cache.check_circuit("test.local", 443)
        permit.abort()


def test_connection_record_direct_kwargs(permit):
    record = ConnectionRecord(host="svc.local", port=80)
    assert record.host == "svc.local"
    assert record.port == 80
    assert record.error is None
    assert record.permit is None
    assert record.is_success is True
    assert record.has_permit is False

    record_with_permit = ConnectionRecord(host="svc.local", port=443, permit=permit)
    assert record_with_permit.has_permit is True
    assert record_with_permit.permit is permit
    assert record_with_permit.is_success is True

    err = OSError("connect failed")
    record_with_error = ConnectionRecord(host="svc.local", port=443, error=err)
    assert record_with_error.error is err
    assert record_with_error.is_success is False
    assert record_with_error.has_permit is False


def test_connection_record_validation_invariants(permit):
    # Host must be non-empty string
    with pytest.raises(ValueError, match="host must be specified"):
        ConnectionRecord(host="", port=80)

    with pytest.raises(ValueError, match="host must be specified"):
        ConnectionRecord(host="   ", port=80)

    # Whitespace in host is trimmed
    rec = ConnectionRecord(host="  example.com  ", port=80)
    assert rec.host == "example.com"

    # Port range bounds
    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ConnectionRecord(host="example.com", port=0)

    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ConnectionRecord(host="example.com", port=65536)

    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ConnectionRecord(host="example.com", port=cast(int, True))

    # Error and permit are mutually exclusive (Philip Wadler's vision)
    with pytest.raises(
        ValueError, match="cannot simultaneously hold both an error and a permit"
    ):
        ConnectionRecord(
            host="example.com",
            port=443,
            error=OSError("fail"),
            permit=permit,
        )
