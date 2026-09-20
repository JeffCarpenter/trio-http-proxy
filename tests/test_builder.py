"""Comprehensive tests for ProxyCacheBuilder and ProxyBuilder API."""

from __future__ import annotations

import logging

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.cache import ProxyCache, ProxyCacheBuilder
from trio_http_proxy.config import Settings
from trio_http_proxy.proxy import (
    ConnectionRecord,
    ConnectionResultBuilder,
    ProxyBuilder,
    _record_connection_result,
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


def test_proxy_cache_builder_fluent_cache():
    builder = ProxyCacheBuilder().with_cache(
        url="mem://custom",
        enabled=True,
        namespace="custom_ns",
        dns_cache_ttl="30s",
        failure_policy="fail-closed",
        probe_interval=15.0,
        startup_timeout=2.0,
    )
    pc = builder.build()
    assert pc.cache_url == "mem://custom"
    assert pc.enabled is True
    assert pc.namespace == "custom_ns"
    assert pc.dns_cache_ttl == "30s"
    assert pc.dns_cache_ttl_sec == 30.0
    assert pc.failure_policy == "fail-closed"
    assert pc.probe_interval == 15.0
    assert pc.startup_timeout == 2.0


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


def test_proxy_cache_builder_circuit_breaker():
    builder = ProxyCacheBuilder().with_circuit_breaker(
        enabled=True,
        errors_rate=40,
        period="30s",
        ttl="15s",
        min_calls=5,
        max_destinations=500,
    )
    pc = builder.build()
    assert pc.circuit_breaker_enabled is True
    assert pc.circuit_breaker_errors_rate == 40
    assert pc.circuit_breaker_period == "30s"
    assert pc.circuit_breaker_ttl == "15s"
    assert pc.circuit_breaker_min_calls == 5
    assert pc.circuit_breaker_max_destinations == 500

    disabled = builder.with_circuit_breaker_disabled().build()
    assert disabled.circuit_breaker_enabled is False


def test_proxy_cache_builder_rate_limit():
    builder = ProxyCacheBuilder().with_rate_limit(
        10,
        period="45s",
        ttl="120s",
    )
    pc = builder.build()
    assert pc.rate_limit_enabled is True
    assert pc.rate_limit == 10
    assert pc.rate_limit_period == "45s"
    assert pc.rate_limit_period_sec == 45.0
    assert pc.rate_limit_ttl == "120s"
    assert pc.rate_limit_ttl_sec == 120.0

    disabled = builder.with_rate_limit_disabled().build()
    assert disabled.rate_limit_enabled is False


def test_proxy_cache_builder_with_settings():
    base = Settings(cache_url="mem://base", rate_limit=5)
    builder = ProxyCacheBuilder(settings=base).with_rate_limit(15)
    pc = builder.build()
    assert pc.cache_url == "mem://base"
    assert pc.rate_limit == 15


@tt.trio_test
async def test_proxy_cache_builder_async_context_manager():
    async with ProxyCacheBuilder().with_cache(url="mem://").build() as pc:
        assert pc.is_running is True
        assert pc.cache_url == "mem://"


def test_proxy_cache_builder_sync_context_manager():
    with ProxyCacheBuilder().with_cache(url="mem://").build() as pc:
        assert pc.is_running is True
        assert pc.cache_url == "mem://"


def test_proxy_cache_classmethod_builder():
    pc = (
        ProxyCache.builder()
        .with_cache(url="mem://via_classmethod")
        .with_circuit_breaker(errors_rate=30)
        .build()
    )
    assert pc.cache_url == "mem://via_classmethod"
    assert pc.circuit_breaker_errors_rate == 30


def test_proxy_cache_constructor_accepts_builder():
    builder = (
        ProxyCacheBuilder()
        .with_cache(url="mem://passed_to_constructor")
        .with_rate_limit(20)
    )
    pc1 = ProxyCache(builder)
    assert pc1.cache_url == "mem://passed_to_constructor"
    assert pc1.rate_limit == 20

    pc2 = ProxyCache(builder=builder)
    assert pc2.cache_url == "mem://passed_to_constructor"
    assert pc2.rate_limit == 20


def test_proxy_cache_builder_validation():
    with pytest.raises(ValueError):
        ProxyCacheBuilder().with_circuit_breaker(errors_rate=150).build()

    with pytest.raises(ValueError):
        ProxyCacheBuilder().with_rate_limit(-5).build()

    with pytest.raises(ValueError):
        ProxyCacheBuilder().with_dns_cache("invalid_duration").build()


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


@tt.trio_test
async def test_proxy_builder_run(caplog):
    caplog.set_level(logging.INFO)
    calls = []

    async def fake_serve(handler, port, *, task_status=trio.TASK_STATUS_IGNORED):
        calls.append(port)

    async def custom_server():
        pass

    builder = (
        ProxyBuilder()
        .with_port(7777)
        .with_server(custom_server)
        .with_cache(ProxyCacheBuilder().with_cache(url="mem://").build())
    )
    await builder.run(_serve=fake_serve)
    assert calls == [7777]
    assert "Starting custom_server on port 7777" in caplog.text


def test_proxy_cache_builder_build_proxy():
    cache_builder = ProxyCacheBuilder().with_cache(url="mem://for_proxy")
    proxy_builder = cache_builder.build_proxy(port=8888)
    assert proxy_builder.build_settings().port == 8888
    assert proxy_builder.build_settings().cache_url == "mem://for_proxy"


def test_connection_result_builder_success():
    builder = (
        ConnectionResultBuilder().with_host("example.com").with_port(443).with_success()
    )
    record = builder.build()
    assert isinstance(record, ConnectionRecord)
    assert record.host == "example.com"
    assert record.port == 443
    assert record.error is None
    assert record.exc is None
    assert record.is_success is True
    assert record.has_permit is False


def test_connection_result_builder_failure():
    err = OSError("connection refused")
    builder = (
        ConnectionResultBuilder().with_target("api.internal", 8080).with_error(err)
    )
    record = builder.build()
    assert record.host == "api.internal"
    assert record.port == 8080
    assert record.error is err
    assert record.exc is err
    assert record.is_success is False

    record2 = (
        ConnectionResultBuilder(host="api.internal", port=8080).with_exc(err).build()
    )
    assert record2.error is err


def test_connection_result_builder_permit():
    dummy_permit = object()
    builder = (
        ConnectionResultBuilder(host="svc.local", port=80).with_permit(dummy_permit)  # type: ignore[arg-type]
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


@tt.trio_test
async def test_proxy_cache_consumes_builder_and_record():
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_errors_rate=50,
        circuit_breaker_min_calls=1,
    ) as pc:
        # Consumes ConnectionRecord
        record = (
            ConnectionResultBuilder()
            .with_target("host1.test", 443)
            .with_error(OSError("err"))
            .build()
        )
        await pc.record_connection_result(record)

        # Consumes ConnectionResultBuilder
        builder = (
            ConnectionResultBuilder().with_target("host2.test", 443).with_success()
        )
        await pc.record_connection_result(builder)

        # Consumes legacy positional
        await pc.record_connection_result("host3.test", 443, exc=None)


@tt.trio_test
async def test_record_connection_result_helper_dispatch():
    results = []

    class ModernCache:
        async def record_connection_result(self, record):
            results.append(("modern", record))

    class LegacyCache:
        async def record_connection_result(self, host, port, exc):
            results.append(("legacy", host, port, exc))

    modern = ModernCache()
    legacy = LegacyCache()

    # Via builder
    b = ConnectionResultBuilder("modern.test", 443).with_success()
    await _record_connection_result(modern, b)
    assert len(results) == 1
    assert results[0][0] == "modern"
    assert results[0][1].host == "modern.test"

    # Via legacy signature to legacy cache
    await _record_connection_result(legacy, "legacy.test", 80, OSError("fail"), None)
    assert len(results) == 2
    assert results[1][0] == "legacy"
    assert results[1][1] == "legacy.test"
