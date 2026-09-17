"""Composite Settings model uniting all proxy sub-configurations."""

from __future__ import annotations

import difflib
from functools import cache
from typing import Any

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from .cache_settings import CacheSettings
from .circuit_breaker_settings import CircuitBreakerSettings
from .proxy_settings import ProxySettings
from .rate_limit_settings import RateLimitSettings

_UNSET = object()  # sentinel: "_env_file not explicitly given"

_GROUPS: tuple[tuple[str, type[BaseSettings]], ...] = (
    ("proxy", ProxySettings),
    ("cache", CacheSettings),
    ("circuit_breaker", CircuitBreakerSettings),
    ("rate_limit", RateLimitSettings),
)


class Settings(BaseSettings):
    """Unified application settings composing proxy, cache, breaker, and rate limiters.

    A pure init-only container: the env-aware sub-settings groups own all
    environment/.env reading themselves, so the composite never scans the
    process environment for its own (group) fields. Flat kwargs are dispatched
    to the matching group (``Settings(port=1)`` == ``Settings(proxy=ProxySettings(port=1))``).

    Pass ``_env_file``/``_env_file_encoding`` to redirect every group to a
    specific dotenv file (None disables dotenv entirely).
    """

    model_config = SettingsConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        case_sensitive=False,
    )

    proxy: ProxySettings = Field(default_factory=ProxySettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    circuit_breaker: CircuitBreakerSettings = Field(
        default_factory=CircuitBreakerSettings
    )
    rate_limit: RateLimitSettings = Field(default_factory=RateLimitSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Composite reads only init kwargs; sub-settings read env/dotenv.
        return (init_settings,)

    def __init__(
        self,
        *,
        proxy: ProxySettings | None = None,
        cache: CacheSettings | None = None,
        circuit_breaker: CircuitBreakerSettings | None = None,
        rate_limit: RateLimitSettings | int | None = None,
        _env_file: Any = _UNSET,
        _env_file_encoding: Any = _UNSET,
        **flat: Any,
    ) -> None:
        forwarded: dict[str, Any] = {}
        if _env_file is not _UNSET:
            forwarded["_env_file"] = _env_file
        if _env_file_encoding is not _UNSET:
            forwarded["_env_file_encoding"] = _env_file_encoding

        # A named parameter can shadow a same-named group field (e.g. the
        # `rate_limit` group has a `rate_limit` field): instance -> group,
        # anything else -> re-inject as a flat field kwarg.
        _raw_groups = {
            "proxy": proxy,
            "cache": cache,
            "circuit_breaker": circuit_breaker,
            "rate_limit": rate_limit,
        }
        resolved: dict[str, Any] = {}
        for group_name, group_cls in _GROUPS:
            explicit = _raw_groups[group_name]
            if explicit is not None and not isinstance(explicit, group_cls):
                flat.setdefault(group_name, explicit)
                explicit = None
            kwargs = {k: flat.pop(k) for k in list(flat) if k in group_cls.model_fields}
            if explicit is not None and kwargs:
                raise ValueError(
                    f"Conflicting settings for group {group_name!r}: got both an explicit "
                    f"{group_name}= instance and flat kwargs {sorted(kwargs)}. Pass one or the other."
                )
            if explicit is not None:
                resolved[group_name] = explicit
            else:
                resolved[group_name] = group_cls(**kwargs, **forwarded)

        if flat:
            known = [f for _, cls in _GROUPS for f in cls.model_fields] + [
                name for name, _ in _GROUPS
            ]
            unknown = ", ".join(sorted(flat))
            hints = ", ".join(
                difflib.get_close_matches(next(iter(flat)), known, n=3) or []
            )
            raise ValueError(
                f"Unknown settings fields: {unknown}. Known fields: {known}"
                + (f". Did you mean: {hints}?" if hints else "")
            )

        super().__init__(**resolved)

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
    def dns_cache_ttl_seconds(self) -> float:
        return self.cache.dns_cache_ttl_seconds

    @property
    def cache_failure_policy(self) -> str:
        return self.cache.cache_failure_policy

    @property
    def probe_interval(self) -> float:
        return self.cache.probe_interval

    @property
    def startup_timeout(self) -> float:
        return self.cache.startup_timeout

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
    def circuit_breaker_period_seconds(self) -> float:
        return self.circuit_breaker.circuit_breaker_period_seconds

    @property
    def circuit_breaker_ttl(self) -> str:
        return self.circuit_breaker.circuit_breaker_ttl

    @property
    def circuit_breaker_ttl_seconds(self) -> float:
        return self.circuit_breaker.circuit_breaker_ttl_seconds

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
    def rate_limit_period_seconds(self) -> float:
        return self.rate_limit.rate_limit_period_seconds

    @property
    def rate_limit_ttl(self) -> str | None:
        return self.rate_limit.rate_limit_ttl

    @property
    def rate_limit_ttl_seconds(self) -> float | None:
        return self.rate_limit.rate_limit_ttl_seconds


@cache
def get_settings() -> Settings:
    """Lazily build and cache process-wide Settings (no import-time env reads).

    Call ``reset_settings_cache()`` (e.g. in tests) after changing the
    environment to rebuild.
    """
    return Settings()


reset_settings_cache = get_settings.cache_clear


__all__ = ["Settings", "get_settings", "reset_settings_cache"]
