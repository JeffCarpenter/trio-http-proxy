"""Unified Settings model consolidating proxy networking, cache, breaker, and rate limiters."""

from __future__ import annotations

from functools import cache, cached_property
from io import DEFAULT_BUFFER_SIZE
from typing import Any, ClassVar, Literal

from pydantic import Field, FiniteFloat, ValidationInfo, field_validator

from ._base import (
    EnvSettings,
    normalize_failure_policy,
    normalize_optional_duration,
    parse_duration_seconds,
    validate_duration_string,
    validate_namespace,
)

_UNSET = object()


class Settings(EnvSettings):
    """Unified application settings consolidating proxy, cache, breaker, and rate limiters.

    Provides direct field access to all proxy configurations without sub-models.
    Configured via canonical ``TRIO_HTTP_PROXY_<FIELD>`` environment variables
    or direct keyword arguments.
    """

    VALID_LOG_LEVELS: ClassVar[frozenset[str]] = frozenset(
        {"CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"}
    )

    # Proxy Networking & Server
    port: int = Field(
        default=8080,
        ge=0,
        le=65535,
        description="Port to listen on for incoming proxy connections.",
    )
    bufmaxsz: int = Field(
        default=DEFAULT_BUFFER_SIZE,
        ge=1,
        description="Maximum buffer size for data transfer.",
    )
    maxreqsz: int = Field(
        default=16384,
        ge=1,
        description="Maximum allowed request size before rejecting as RequestTooLargeError.",
    )
    connect_read_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for reading client CONNECT request.",
    )
    dest_connect_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for connecting to target destination.",
    )
    pipe_idle_timeout: FiniteFloat = Field(
        default=300.0,
        gt=0.0,
        description="Timeout in seconds when bidirectional pipe is completely idle.",
    )
    pipe_send_timeout: FiniteFloat = Field(
        default=30.0,
        gt=0.0,
        description="Timeout in seconds when sending data through pipe stalls.",
    )
    maxconns: int = Field(
        default=1000,
        ge=1,
        description="Maximum concurrent active proxy connections.",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level for proxy events.",
    )

    # Cache Backend & Namespace
    cache_url: str = Field(
        default="mem://",
        description="URL for cache backend (e.g. 'mem://', 'redis://...').",
    )
    cache_enabled: bool = Field(
        default=True,
        description="Whether cache operations and backend connectivity are enabled.",
    )
    cache_namespace: str = Field(
        default="trio_http_proxy",
        description="Key namespace prefix isolating proxy cache entries.",
    )
    dns_cache_ttl: str = Field(
        default="0s",
        description="TTL duration string for DNS caching; '0s' disables caching.",
    )
    cache_failure_policy: Literal["fail-open", "fail-closed"] = Field(
        default="fail-open",
        description="Policy on cache backend failure: 'fail-open' or 'fail-closed'.",
    )
    probe_interval: FiniteFloat = Field(
        default=30.0,
        gt=0.0,
        description="Interval in seconds for periodic health probing of degraded backend.",
    )
    startup_timeout: FiniteFloat = Field(
        default=5.0,
        gt=0.0,
        description="Timeout in seconds for cache startup ping in fail-closed mode.",
    )

    # Circuit Breaker
    circuit_breaker_enabled: bool = Field(
        default=True,
        description="Whether destination circuit breaking is active.",
    )
    circuit_breaker_errors_rate: int = Field(
        default=50,
        ge=1,
        le=99,
        description="Percentage error rate threshold (1-99) tripping breaker to OPEN.",
    )
    circuit_breaker_period: str = Field(
        default="60s",
        description="Duration window string over which errors are counted.",
    )
    circuit_breaker_ttl: str = Field(
        default="30s",
        description="Cool-down duration string before probing in HALF_OPEN.",
    )
    circuit_breaker_min_calls: int = Field(
        default=3,
        ge=1,
        description="Minimum calls required in window before tripping.",
    )
    circuit_breaker_max_destinations: int = Field(
        default=10000,
        ge=1,
        description="Maximum destinations tracked concurrently in LRU cache.",
    )

    # Rate Limiting
    rate_limit_enabled: bool = Field(
        default=False,
        description="Whether rate limiting per client IP is enabled.",
    )
    rate_limit: int = Field(
        default=0,
        ge=0,
        description="Maximum requests allowed within rate_limit_period (0 means unlimited).",
    )
    rate_limit_period: str = Field(
        default="60s",
        description="Window duration string over which requests are counted.",
    )
    rate_limit_ttl: str | None = Field(
        default=None,
        description="Ban TTL duration string applied once rate limit is exceeded ('0s'/'': disabled).",
    )

    # Validators
    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        norm = value.strip().upper()
        if norm not in cls.VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid log_level: {value!r}. Must be one of {sorted(cls.VALID_LOG_LEVELS)}"
            )
        return norm

    @field_validator("cache_namespace")
    @classmethod
    def _validate_cache_namespace(cls, value: str) -> str:
        return validate_namespace(value)

    @field_validator("dns_cache_ttl")
    @classmethod
    def _validate_dns_cache_ttl(cls, value: str) -> str:
        return validate_duration_string(value, field="dns_cache_ttl", allow_zero=True)

    @field_validator("cache_failure_policy", mode="before")
    @classmethod
    def _normalize_failure_policy(cls, value: Any) -> Any:
        if isinstance(value, str):
            return normalize_failure_policy(value)
        return value

    @field_validator(
        "circuit_breaker_period", "circuit_breaker_ttl", "rate_limit_period"
    )
    @classmethod
    def _validate_duration_fields(cls, value: str, info: ValidationInfo) -> str:
        field = info.field_name or "duration"
        return validate_duration_string(value, field=field)

    @field_validator("rate_limit_ttl")
    @classmethod
    def _validate_rate_limit_ttl(cls, value: str | None) -> str | None:
        return normalize_optional_duration(value, field="rate_limit_ttl")

    # Cached properties for parsed durations
    @cached_property
    def dns_cache_ttl_seconds(self) -> float:
        """DNS cache TTL in seconds; ``0.0`` means disabled (pass-through)."""
        return parse_duration_seconds(
            self.dns_cache_ttl, field="dns_cache_ttl", allow_zero=True
        )

    @cached_property
    def circuit_breaker_period_seconds(self) -> float:
        """Error-counting window in seconds."""
        return parse_duration_seconds(
            self.circuit_breaker_period, field="circuit_breaker_period"
        )

    @cached_property
    def circuit_breaker_ttl_seconds(self) -> float:
        """Open-circuit cool-down in seconds."""
        return parse_duration_seconds(
            self.circuit_breaker_ttl, field="circuit_breaker_ttl"
        )

    @cached_property
    def rate_limit_period_seconds(self) -> float:
        """Rate-limit window in seconds."""
        return parse_duration_seconds(self.rate_limit_period, field="rate_limit_period")

    @cached_property
    def rate_limit_ttl_seconds(self) -> float | None:
        """Ban TTL in seconds; ``None`` means disabled."""
        if self.rate_limit_ttl is None:
            return None
        return parse_duration_seconds(self.rate_limit_ttl, field="rate_limit_ttl")

    @property
    def rate_limit_count(self) -> int:
        """Alias for rate_limit count for backward compatibility."""
        return self.rate_limit

    def __init__(
        self,
        *args: Any,
        _env_file: Any = _UNSET,
        _env_file_encoding: Any = _UNSET,
        **values: Any,
    ) -> None:
        if args:
            raise TypeError(
                f"Settings takes no positional arguments, got {len(args)}"
            )
        for key in ("proxy", "cache", "circuit_breaker", "rate_limit"):
            if key in values and isinstance(values[key], dict):
                sub = values.pop(key)
                values.update(sub)
            elif key in values and hasattr(values[key], "model_dump"):
                sub = values.pop(key)
                values.update(sub.model_dump())
        kwargs: dict[str, Any] = {}
        if _env_file is not _UNSET:
            kwargs["_env_file"] = _env_file
        if _env_file_encoding is not _UNSET:
            kwargs["_env_file_encoding"] = _env_file_encoding
        super().__init__(**kwargs, **values)


@cache
def get_settings() -> Settings:
    """Lazily build and cache process-wide Settings (no import-time env reads).

    Call ``reset_settings_cache()`` (e.g. in tests) after changing the
    environment to rebuild.
    """
    return Settings()


reset_settings_cache = get_settings.cache_clear


def resolve_settings(settings: Settings | None, context: Any = None) -> Settings:
    """Resolve the effective Settings for a call: explicit ``settings=`` wins,
    then a ``context.settings`` (if truthy), then the process-wide default.

    Centralizes the precedence duplicated across http_proxy/tunnel/pipe/
    pipe_with_timeout/process_as_connect_request so it only needs fixing
    in one place.
    """
    if settings is not None:
        return settings
    context_settings = getattr(context, "settings", None)
    if context_settings:
        return context_settings
    return get_settings()


__all__ = [
    "Settings",
    "get_settings",
    "reset_settings_cache",
    "resolve_settings",
]
