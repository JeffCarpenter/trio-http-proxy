"""Comprehensive tests for ProxyCacheBuilder and ProxyBuilder API."""

from __future__ import annotations

from typing import cast

import pytest
import trio.testing as tt

from trio_http_proxy.cache import ProxyCache, ProxyCacheBuilder
from trio_http_proxy.circuit_breaker import CircuitPermit
from trio_http_proxy.config import Settings
from trio_http_proxy.proxy import (
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


def test_connection_result_builder_permit():
    dummy_permit = cast(CircuitPermit, object())
    builder = ConnectionResultBuilder(host="svc.local", port=80).with_permit(
        dummy_permit
    )
    record = builder.build()
    assert record.has_permit is True
    assert record.permit is dummy_permit


def test_connection_result_builder_validation():
    with pytest.raises(ValueError, match="host must be specified"):
        ConnectionResultBuilder().with_port(80).build()

    with pytest.raises(ValueError, match="port must be specified"):
        ConnectionResultBuilder().with_host("example.com").build()


def test_proxy_cache_classmethod_result_builder():
    builder = ProxyCache.result_builder("dest.com", 9000)
    record = builder.with_success().build()
    assert record.host == "dest.com"
    assert record.port == 9000
    assert record.is_success is True


@tt.trio_test
async def test_connection_result_builder_record_to():
    recorded = []

    class MockCache:
        async def record_connection_result(self, record):
            recorded.append(record)

    cache = MockCache()
    builder = ConnectionResultBuilder("test.local", 443).with_success()
    await builder.record_to(cache)

    assert len(recorded) == 1
    assert recorded[0].host == "test.local"
    assert recorded[0].port == 443
    assert recorded[0].is_success is True
