"""Unit and integration tests for pydantic-settings configuration."""

import pytest
from pydantic import ValidationError

from trio_http_proxy.circuit_breaker import CircuitBreakerManager
from trio_http_proxy.config import (
    DEFAULT_SETTINGS,
    CacheSettings,
    CircuitBreakerSettings,
    ProxySettings,
    RateLimitSettings,
    Settings,
)
from trio_http_proxy.proxy_cache import ProxyCache


class TestProxySettings:
    """Tests for ProxySettings model."""

    def test_defaults(self):
        settings = ProxySettings()
        assert settings.port == 8080
        assert settings.maxconns == 1000
        assert settings.maxreqsz == 16384
        assert settings.connect_read_timeout == 10.0
        assert settings.dest_connect_timeout == 10.0
        assert settings.pipe_idle_timeout == 300.0
        assert settings.pipe_send_timeout == 30.0
        assert settings.log_level == "INFO"

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("PORT", "9999")
        monkeypatch.setenv("MAXCONNS", "2500")
        monkeypatch.setenv("CONNECT_READ_TIMEOUT", "4.5")
        monkeypatch.setenv("LOG_LEVEL", "debug")

        settings = ProxySettings()
        assert settings.port == 9999
        assert settings.maxconns == 2500
        assert settings.connect_read_timeout == 4.5
        assert settings.log_level == "DEBUG"

    def test_port_validation(self):
        with pytest.raises(ValidationError):
            ProxySettings(port=-1)
        with pytest.raises(ValidationError):
            ProxySettings(port=70000)

    def test_buffer_and_request_size_validation(self):
        with pytest.raises(ValidationError):
            ProxySettings(bufmaxsz=0)
        with pytest.raises(ValidationError):
            ProxySettings(maxreqsz=0)

    def test_timeout_validation(self):
        with pytest.raises(ValidationError):
            ProxySettings(connect_read_timeout=0.0)
        with pytest.raises(ValidationError):
            ProxySettings(pipe_idle_timeout=-1.0)

    def test_maxconns_validation(self):
        with pytest.raises(ValidationError):
            ProxySettings(maxconns=0)

    def test_log_level_validation(self):
        settings = ProxySettings(log_level="warn")
        assert settings.log_level == "WARN"

        with pytest.raises(ValidationError):
            ProxySettings(log_level="INVALID_LEVEL")


class TestCacheSettings:
    """Tests for CacheSettings model."""

    def test_defaults(self):
        settings = CacheSettings()
        assert settings.cache_url == "mem://"
        assert settings.cache_enabled is True
        assert settings.cache_namespace == "trio_http_proxy"
        assert settings.dns_cache_ttl == "0s"
        assert settings.cache_failure_policy == "fail-open"
        assert settings.probe_interval == 30.0
        assert settings.startup_timeout == 5.0

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("CACHE_URL", "redis://localhost:6379/1")
        monkeypatch.setenv("CACHE_ENABLED", "0")
        monkeypatch.setenv("CACHE_NAMESPACE", "custom_proxy")
        monkeypatch.setenv("CACHE_FAILURE_POLICY", "FAIL-CLOSED")

        settings = CacheSettings()
        assert settings.cache_url == "redis://localhost:6379/1"
        assert settings.cache_enabled is False
        assert settings.cache_namespace == "custom_proxy"
        assert settings.cache_failure_policy == "fail-closed"

    def test_namespace_validation(self):
        with pytest.raises(ValidationError):
            CacheSettings(cache_namespace="")
        with pytest.raises(ValidationError):
            CacheSettings(cache_namespace="   ")
        with pytest.raises(ValidationError):
            CacheSettings(cache_namespace="invalid namespace with spaces")
        with pytest.raises(ValidationError):
            CacheSettings(cache_namespace="invalid$char!")

        # Valid characters: alphanumeric, underscore, hyphen
        settings = CacheSettings(cache_namespace=" valid-ns_123 ")
        assert settings.cache_namespace == "valid-ns_123"

    def test_dns_cache_ttl_validation(self):
        settings = CacheSettings(dns_cache_ttl="5m")
        assert settings.dns_cache_ttl == "5m"

        with pytest.raises(ValidationError):
            CacheSettings(dns_cache_ttl="invalid_duration")

    def test_failure_policy_validation(self):
        from typing import Any, cast

        settings = CacheSettings(cache_failure_policy=cast(Any, " FAIL-OPEN "))
        assert settings.cache_failure_policy == "fail-open"

        with pytest.raises(ValidationError):
            CacheSettings(cache_failure_policy=cast(Any, "fail-silent"))


class TestCircuitBreakerSettings:
    """Tests for CircuitBreakerSettings model."""

    def test_defaults(self):
        settings = CircuitBreakerSettings()
        assert settings.circuit_breaker_enabled is True
        assert settings.circuit_breaker_errors_rate == 50
        assert settings.circuit_breaker_period == "60s"
        assert settings.circuit_breaker_ttl == "30s"
        assert settings.circuit_breaker_min_calls == 3
        assert settings.circuit_breaker_max_destinations == 10000

    def test_errors_rate_validation(self):
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_errors_rate=0)
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_errors_rate=100)

    def test_period_and_ttl_validation(self):
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_period="0s")
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_ttl="0s")
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_period="not_a_ttl")


class TestRateLimitSettings:
    """Tests for RateLimitSettings model."""

    def test_defaults(self):
        settings = RateLimitSettings()
        assert settings.rate_limit_enabled is False
        assert settings.rate_limit == 0
        assert settings.rate_limit_period == "60s"
        assert settings.rate_limit_ttl is None

    def test_validation(self):
        with pytest.raises(ValidationError):
            RateLimitSettings(rate_limit=-1)
        with pytest.raises(ValidationError):
            RateLimitSettings(rate_limit_period="0s")
        with pytest.raises(ValidationError):
            RateLimitSettings(rate_limit_ttl="0s")


class TestCompositeSettings:
    """Tests for unified Settings model."""

    def test_defaults(self):
        settings = Settings()
        assert settings.port == 8080
        assert settings.cache_url == "mem://"
        assert settings.circuit_breaker_enabled is True
        assert settings.rate_limit_enabled is False
        assert settings.circuit_breaker_period == "60s"

    def test_kwarg_distribution(self):
        settings = Settings(
            port=9191,
            cache_url="redis://localhost",
            circuit_breaker_errors_rate=75,
            rate_limit=100,
        )
        assert settings.port == 9191
        assert settings.proxy.port == 9191
        assert settings.cache.cache_url == "redis://localhost"
        assert settings.circuit_breaker.circuit_breaker_errors_rate == 75
        assert settings.rate_limit.rate_limit == 100

    def test_default_settings_singleton(self):
        assert isinstance(DEFAULT_SETTINGS, Settings)
        assert DEFAULT_SETTINGS.port == 8080


class TestIntegration:
    """Tests for settings integration with ProxyCache and CircuitBreakerManager."""

    def test_circuit_breaker_manager_accepts_settings(self):
        cb_settings = CircuitBreakerSettings(
            circuit_breaker_errors_rate=30,
            circuit_breaker_period="120s",
            circuit_breaker_ttl="45s",
            circuit_breaker_min_calls=5,
            circuit_breaker_max_destinations=500,
        )
        manager = CircuitBreakerManager(settings=cb_settings)
        assert manager.errors_rate == 30
        assert manager.period == 120.0
        assert manager.ttl == 45.0
        assert manager.min_calls == 5
        assert manager.max_destinations == 500

    def test_proxy_cache_accepts_settings(self):
        custom_settings = Settings(
            cache_url="mem://custom",
            cache_namespace="custom_ns",
            circuit_breaker_errors_rate=60,
            rate_limit=50,
            rate_limit_period="30s",
        )
        cache = ProxyCache(settings=custom_settings)
        assert cache.cache_url == "mem://custom"
        assert cache.namespace == "custom_ns"
        assert cache.circuit_breaker_errors_rate == 60
        assert cache.rate_limit == 50
        assert cache.rate_limit_period_sec == 30.0

    def test_proxy_cache_accepts_cache_settings(self):
        cache_settings = CacheSettings(
            cache_url="mem://test",
            cache_namespace="test_ns",
            cache_failure_policy="fail-closed",
        )
        cache = ProxyCache(settings=cache_settings)
        assert cache.cache_url == "mem://test"
        assert cache.namespace == "test_ns"
        assert cache.failure_policy == "fail-closed"
