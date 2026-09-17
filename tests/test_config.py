"""Unit and integration tests for pydantic-settings configuration."""

import pytest
from pydantic import ValidationError

from trio_http_proxy.circuit_breaker import CircuitBreakerManager
from trio_http_proxy.config import (
    CacheSettings,
    CircuitBreakerSettings,
    ProxySettings,
    RateLimitSettings,
    Settings,
    get_settings,
    reset_settings_cache,
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
        # "0s"/"" normalize to None (disabled) rather than raising
        assert RateLimitSettings(rate_limit_ttl="0s").rate_limit_ttl is None


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

    def test_get_settings_cached(self):
        reset_settings_cache()
        settings = get_settings()
        assert isinstance(settings, Settings)
        assert settings.port == 8080
        assert get_settings() is settings
        reset_settings_cache()
        assert get_settings() is not settings
        reset_settings_cache()

    def test_prefixed_env_wins_over_legacy(self, monkeypatch):
        monkeypatch.setenv("TRIO_HTTP_PROXY_PORT", "2222")
        monkeypatch.setenv("PORT", "1111")
        assert ProxySettings().port == 2222
        assert Settings().port == 2222

    def test_env_file_forwarded_to_groups(self, tmp_path, monkeypatch):
        env_file = tmp_path / "test.env"
        env_file.write_text("MAXREQSZ=424242\nUNRELATED_VAR=zzz\n")
        monkeypatch.delenv("MAXREQSZ", raising=False)
        settings = Settings(_env_file=str(env_file))
        assert settings.proxy.maxreqsz == 424242
        assert settings.cache.cache_url == "mem://"

    def test_env_file_none_disables_dotenv(self, tmp_path, monkeypatch):
        env_file = tmp_path / "override.env"
        env_file.write_text("MAXREQSZ=424242\n")
        monkeypatch.delenv("MAXREQSZ", raising=False)
        assert Settings(_env_file=None).proxy.maxreqsz == 16384

    def test_settings_are_frozen(self):
        settings = Settings()
        with pytest.raises(ValidationError):
            settings.proxy.port = 1

    def test_unknown_kwarg_rejected_with_hint(self):
        with pytest.raises(ValueError, match="Unknown settings fields.*potr"):
            Settings(potr=1)

    def test_flat_kwarg_and_group_collision_rejected(self):
        with pytest.raises(ValueError, match="Conflicting settings for group 'proxy'"):
            Settings(port=1, proxy=ProxySettings(port=2))

    def test_stray_group_env_vars_do_not_crash_composite(self, monkeypatch):
        # PROXY/CACHE name no settings field, so the env source ignores them.
        monkeypatch.setenv("PROXY", "not-a-settings-object")
        monkeypatch.setenv("CACHE", "banana")
        monkeypatch.setenv("RATE_LIMIT", "7")
        settings = Settings()
        assert settings.port == 8080
        assert settings.rate_limit.rate_limit == 7
        # an invalid value for a real field fails fast at construction, but
        # importing the package (module import time) never reads the env.
        monkeypatch.setenv("RATE_LIMIT", "banana")
        with pytest.raises(ValidationError):
            Settings()

    def test_import_with_invalid_env_does_not_crash(self):
        import subprocess
        import sys

        result = subprocess.run(
            [sys.executable, "-c", "import trio_http_proxy"],
            capture_output=True,
            text=True,
            check=False,
            env={"PORT": "abc", "PATH": "/usr/bin"},
        )
        assert result.returncode == 0, result.stderr

    def test_infinite_and_nan_timeouts_rejected(self):
        with pytest.raises(ValidationError):
            ProxySettings(pipe_idle_timeout=float("inf"))
        with pytest.raises(ValidationError):
            ProxySettings(connect_read_timeout=float("nan"))


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


class TestDurationSemantics:
    """Uniform duration-string handling across settings groups."""

    def test_dns_cache_ttl_zero_and_empty_mean_disabled(self):
        assert CacheSettings(dns_cache_ttl="0s").dns_cache_ttl_seconds == 0.0
        assert CacheSettings(dns_cache_ttl="").dns_cache_ttl == "0s"
        assert CacheSettings(dns_cache_ttl="").dns_cache_ttl_seconds == 0.0
        assert CacheSettings(dns_cache_ttl=" 5m ").dns_cache_ttl_seconds == 300.0

    def test_rate_limit_ttl_none_empty_zero_mean_disabled(self):
        assert RateLimitSettings().rate_limit_ttl is None
        assert RateLimitSettings(rate_limit_ttl="").rate_limit_ttl is None
        assert RateLimitSettings(rate_limit_ttl="0s").rate_limit_ttl is None
        assert RateLimitSettings(rate_limit_ttl=None).rate_limit_ttl_seconds is None
        assert RateLimitSettings(rate_limit_ttl="90s").rate_limit_ttl_seconds == 90.0

    def test_positive_periods_parsed_once_into_seconds(self):
        cb = CircuitBreakerSettings(
            circuit_breaker_period="2m", circuit_breaker_ttl="1h"
        )
        assert cb.circuit_breaker_period_seconds == 120.0
        assert cb.circuit_breaker_ttl_seconds == 3600.0
        rl = RateLimitSettings(rate_limit_period="5m")
        assert rl.rate_limit_period_seconds == 300.0

    def test_empty_period_rejected(self):
        with pytest.raises(ValidationError):
            CircuitBreakerSettings(circuit_breaker_period="   ")
        with pytest.raises(ValidationError):
            RateLimitSettings(rate_limit_period="")

    def test_invalid_durations_rejected_with_field_names(self):
        for bad in ("-5s", "0.5s", "1w", "forever"):
            with pytest.raises(ValidationError, match="circuit_breaker_ttl"):
                CircuitBreakerSettings(circuit_breaker_ttl=bad)


class TestProxyCacheDurationIntegration:
    """ProxyCache consumes uniform duration semantics."""

    def test_rate_limit_ttl_falls_back_to_period(self):
        cache = ProxyCache(rate_limit=10, rate_limit_period="30s", rate_limit_ttl=None)
        assert cache.rate_limit_ttl_sec == 30.0

    def test_rate_limit_ttl_disabled_values_fall_back(self):
        for ttl in ("", "0s"):
            cache = ProxyCache(
                rate_limit=10, rate_limit_period="45s", rate_limit_ttl=ttl
            )
            assert cache.rate_limit_ttl_sec == 45.0

    def test_proxy_cache_rejects_invalid_durations(self):
        with pytest.raises(ValueError, match="rate_limit_period"):
            ProxyCache(rate_limit_period="1w")
        with pytest.raises(ValueError, match="dns_cache_ttl"):
            ProxyCache(dns_cache_ttl="soon")
