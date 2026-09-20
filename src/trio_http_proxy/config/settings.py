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
    prefixed,
    validate_duration_string,
    validate_namespace,
)

_UNSET = object()


class Settings(EnvSettings):
    """Unified application settings consolidating proxy, cache, breaker, and rate limiters.

    Provides direct field access to all proxy configurations without sub-models.
    Every field accepts its canonical ``TRIO_HTTP_PROXY_<FIELD>`` env var (wins)
    plus its legacy unprefixed name for backward compatibility.
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
        validation_alias=prefixed("port"),
    )
    bufmaxsz: int = Field(
        default=DEFAULT_BUFFER_SIZE,
        ge=1,
        description="Maximum buffer size for data transfer.",
        validation_alias=prefixed("bufmaxsz"),
    )
    maxreqsz: int = Field(
        default=16384,
        ge=1,
        description="Maximum allowed request size before rejecting as RequestTooLargeError.",
        validation_alias=prefixed("maxreqsz"),
    )
    connect_read_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for reading client CONNECT request.",
        validation_alias=prefixed("connect_read_timeout"),
    )
    dest_connect_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for connecting to target destination.",
        validation_alias=prefixed("dest_connect_timeout"),
    )
    pipe_idle_timeout: FiniteFloat = Field(
        default=300.0,
        gt=0.0,
        description="Timeout in seconds when bidirectional pipe is completely idle.",
        validation_alias=prefixed("pipe_idle_timeout"),
    )
    pipe_send_timeout: FiniteFloat = Field(
        default=30.0,
        gt=0.0,
        description="Timeout in seconds when sending data through pipe stalls.",
        validation_alias=prefixed("pipe_send_timeout"),
    )
    maxconns: int = Field(
        default=1000,
        ge=1,
        description="Maximum concurrent active proxy connections.",
        validation_alias=prefixed("maxconns"),
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level for proxy events.",
        validation_alias=prefixed("log_level"),
    )

    # Cache Backend & Namespace
    cache_url: str = Field(
        default="mem://",
        description="URL for cache backend (e.g. 'mem://', 'redis://...').",
        validation_alias=prefixed("cache_url"),
    )
    cache_enabled: bool = Field(
        default=True,
        description="Whether cache operations and backend connectivity are enabled.",
        validation_alias=prefixed("cache_enabled"),
    )
    cache_namespace: str = Field(
        default="trio_http_proxy",
        description="Key namespace prefix isolating proxy cache entries.",
        validation_alias=prefixed("cache_namespace"),
    )
    dns_cache_ttl: str = Field(
        default="0s",
        description="TTL duration string for DNS caching; '0s' disables caching.",
        validation_alias=prefixed("dns_cache_ttl"),
    )
    cache_failure_policy: Literal["fail-open", "fail-closed"] = Field(
        default="fail-open",
        description="Policy on cache backend failure: 'fail-open' or 'fail-closed'.",
        validation_alias=prefixed("cache_failure_policy"),
    )
    probe_interval: FiniteFloat = Field(
        default=30.0,
        gt=0.0,
        description="Interval in seconds for periodic health probing of degraded backend.",
        validation_alias=prefixed("probe_interval"),
    )
    startup_timeout: FiniteFloat = Field(
        default=5.0,
        description="Timeout in seconds for cache startup ping in fail-closed mode.",
        validation_alias=prefixed("startup_timeout"),
    )

    # Circuit Breaker
    circuit_breaker_enabled: bool = Field(
        default=True,
        description="Whether destination circuit breaking is active.",
        validation_alias=prefixed("circuit_breaker_enabled"),
    )
    circuit_breaker_errors_rate: int = Field(
        default=50,
        description="Percentage error rate threshold (1-99) tripping breaker to OPEN.",
        validation_alias=prefixed("circuit_breaker_errors_rate"),
    )
    circuit_breaker_period: str = Field(
        default="60s",
        description="Duration window string over which errors are counted.",
        validation_alias=prefixed("circuit_breaker_period"),
    )
    circuit_breaker_ttl: str = Field(
        default="30s",
        description="Cool-down duration string before probing in HALF_OPEN.",
        validation_alias=prefixed("circuit_breaker_ttl"),
    )
    circuit_breaker_min_calls: int = Field(
        default=3,
        description="Minimum calls required in window before tripping.",
        validation_alias=prefixed("circuit_breaker_min_calls"),
    )
    circuit_breaker_max_destinations: int = Field(
        default=10000,
        ge=1,
        description="Maximum destinations tracked concurrently in LRU cache.",
        validation_alias=prefixed("circuit_breaker_max_destinations"),
    )

    # Rate Limiting
    rate_limit_enabled: bool = Field(
        default=False,
        description="Whether rate limiting per client IP is enabled.",
        validation_alias=prefixed("rate_limit_enabled"),
    )
    rate_limit: int = Field(
        default=0,
        description="Maximum requests allowed within rate_limit_period (0 means unlimited).",
        validation_alias=prefixed("rate_limit"),
    )
    rate_limit_period: str = Field(
        default="60s",
        description="Window duration string over which requests are counted.",
        validation_alias=prefixed("rate_limit_period"),
    )
    rate_limit_ttl: str | None = Field(
        default=None,
        description="Ban TTL duration string applied once rate limit is exceeded ('0s'/'': disabled).",
        validation_alias=prefixed("rate_limit_ttl"),
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

    @field_validator("circuit_breaker_errors_rate")
    @classmethod
    def _validate_circuit_breaker_errors_rate(cls, value: int) -> int:
        if not 1 <= value <= 99:
            raise ValueError(
                f"circuit_breaker_errors_rate must be between 1 and 99, got {value}"
            )
        return value

    @field_validator("circuit_breaker_period", "circuit_breaker_ttl")
    @classmethod
    def _validate_duration_fields(cls, value: str, info: ValidationInfo) -> str:
        field = info.field_name or "duration"
        return validate_duration_string(value, field=field)

    @field_validator("rate_limit_period")
    @classmethod
    def _validate_rate_limit_period(cls, value: str) -> str:
        return validate_duration_string(value, field="rate_limit_period")

    @field_validator("rate_limit_ttl")
    @classmethod
    def _validate_rate_limit_ttl(cls, value: str | None) -> str | None:
        return normalize_optional_duration(value, field="rate_limit_ttl")

    @field_validator("startup_timeout")
    @classmethod
    def _validate_startup_timeout(cls, value: float) -> float:
        if value <= 0:
            raise ValueError(f"startup_timeout must be > 0, got {value}")
        return value

    @field_validator("circuit_breaker_min_calls")
    @classmethod
    def _validate_circuit_breaker_min_calls(cls, value: int) -> int:
        if value < 1:
            raise ValueError(f"circuit_breaker_min_calls must be >= 1, got {value}")
        return value

    @field_validator("rate_limit")
    @classmethod
    def _validate_rate_limit(cls, value: int) -> int:
        if value < 0:
            raise ValueError(f"rate_limit must be >= 0, got {value}")
        return value

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

    # Backward compatibility properties for sub-group attribute access
    @property
    def proxy(self) -> Settings:
        return self

    @property
    def cache(self) -> Settings:
        return self

    @property
    def circuit_breaker(self) -> Settings:
        return self

    def __init__(
        self,
        *args: Any,
        _env_file: Any = _UNSET,
        _env_file_encoding: Any = _UNSET,
        **values: Any,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if _env_file is not _UNSET:
            kwargs["_env_file"] = _env_file
        if _env_file_encoding is not _UNSET:
            kwargs["_env_file_encoding"] = _env_file_encoding
        super().__init__(*args, **kwargs, **values)


# Type aliases for eliminated sub-models
ProxySettings = Settings
CacheSettings = Settings
CircuitBreakerSettings = Settings
RateLimitSettings = Settings


@cache
def get_settings() -> Settings:
    """Lazily build and cache process-wide Settings (no import-time env reads).

    Call ``reset_settings_cache()`` (e.g. in tests) after changing the
    environment to rebuild.
    """
    return Settings()


reset_settings_cache = get_settings.cache_clear


__all__ = [
    "CacheSettings",
    "CircuitBreakerSettings",
    "ProxySettings",
    "RateLimitSettings",
    "Settings",
    "get_settings",
    "reset_settings_cache",
]
