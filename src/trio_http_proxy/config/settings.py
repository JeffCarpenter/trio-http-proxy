"""Composite Settings model uniting all proxy sub-configurations."""

from __future__ import annotations

from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .cache_settings import CacheSettings
from .circuit_breaker_settings import CircuitBreakerSettings
from .proxy_settings import ProxySettings
from .rate_limit_settings import RateLimitSettings


class Settings(BaseSettings):
    """Unified application settings composing proxy, cache, breaker, and rate limiters."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    proxy: ProxySettings = Field(default_factory=ProxySettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    circuit_breaker: CircuitBreakerSettings = Field(
        default_factory=CircuitBreakerSettings
    )
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)

    def __init__(self, **values: Any) -> None:
        proxy_kwargs: dict[str, Any] = {}
        cache_kwargs: dict[str, Any] = {}
        cb_kwargs: dict[str, Any] = {}
        rl_kwargs: dict[str, Any] = {}

        for k, v in list(values.items()):
            if k in ProxySettings.model_fields:
                proxy_kwargs[k] = values.pop(k)
            elif k in CacheSettings.model_fields:
                cache_kwargs[k] = values.pop(k)
            elif k in CircuitBreakerSettings.model_fields:
                cb_kwargs[k] = values.pop(k)
            elif k in RateLimitSettings.model_fields:
                rl_kwargs[k] = values.pop(k)

        if proxy_kwargs and "proxy" not in values:
            values["proxy"] = ProxySettings(**proxy_kwargs)
        if cache_kwargs and "cache" not in values:
            values["cache"] = CacheSettings(**cache_kwargs)
        if cb_kwargs and "circuit_breaker" not in values:
            values["circuit_breaker"] = CircuitBreakerSettings(**cb_kwargs)
        if rl_kwargs and "rate_limit" not in values:
            values["rate_limit"] = RateLimitSettings(**rl_kwargs)

        super().__init__(**values)

    # Convenience direct accessors delegating to sub-models
    @property
    def port(self) -> int:
        return self.proxy.port

    @property
    def bufmaxsz(self) -> int:
        return self.proxy.bufmaxsz

    @property
    def maxreqsz(self) -> int:
        return self.proxy.maxreqsz

    @property
    def connect_read_timeout(self) -> float:
        return self.proxy.connect_read_timeout

    @property
    def dest_connect_timeout(self) -> float:
        return self.proxy.dest_connect_timeout

    @property
    def pipe_idle_timeout(self) -> float:
        return self.proxy.pipe_idle_timeout

    @property
    def pipe_send_timeout(self) -> float:
        return self.proxy.pipe_send_timeout

    @property
    def maxconns(self) -> int:
        return self.proxy.maxconns

    @property
    def log_level(self) -> str:
        return self.proxy.log_level

    @property
    def cache_url(self) -> str:
        return self.cache.cache_url

    @property
    def cache_enabled(self) -> bool:
        return self.cache.cache_enabled

    @property
    def cache_namespace(self) -> str:
        return self.cache.cache_namespace

    @property
    def dns_cache_ttl(self) -> str:
        return self.cache.dns_cache_ttl

    @property
    def cache_failure_policy(self) -> str:
        return self.cache.cache_failure_policy

    @property
    def probe_interval(self) -> float:
        return self.cache.probe_interval

    @property
    def circuit_breaker_enabled(self) -> bool:
        return self.circuit_breaker.circuit_breaker_enabled

    @property
    def circuit_breaker_errors_rate(self) -> int:
        return self.circuit_breaker.circuit_breaker_errors_rate

    @property
    def circuit_breaker_period(self) -> str:
        return self.circuit_breaker.circuit_breaker_period

    @property
    def circuit_breaker_ttl(self) -> str:
        return self.circuit_breaker.circuit_breaker_ttl

    @property
    def circuit_breaker_min_calls(self) -> int:
        return self.circuit_breaker.circuit_breaker_min_calls

    @property
    def circuit_breaker_max_destinations(self) -> int:
        return self.circuit_breaker.circuit_breaker_max_destinations

    @property
    def rate_limit_enabled(self) -> bool:
        return self.rate_limit.rate_limit_enabled

    @property
    def rate_limit_count(self) -> int:
        return self.rate_limit.rate_limit

    @property
    def rate_limit_period(self) -> str:
        return self.rate_limit.rate_limit_period

    @property
    def rate_limit_ttl(self) -> str | None:
        return self.rate_limit.rate_limit_ttl


DEFAULT_SETTINGS = Settings()

__all__ = ["DEFAULT_SETTINGS", "Settings"]
