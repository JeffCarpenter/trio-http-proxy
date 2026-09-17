"""Cache backend and namespace configuration using pydantic-settings."""

from __future__ import annotations

import re
from typing import Literal

from cashews.ttl import ttl_to_seconds
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class CacheSettings(BaseSettings):
    """Configuration settings for cashews cache backend, namespace, and probing."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

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
        description="TTL duration string for DNS caching (e.g. '60s', '5m').",
    )
    cache_failure_policy: Literal["fail-open", "fail-closed"] = Field(
        default="fail-open",
        description="Policy on cache backend failure: 'fail-open' or 'fail-closed'.",
    )
    probe_interval: float = Field(
        default=30.0,
        gt=0.0,
        description="Interval in seconds for periodic health probing of degraded backend.",
    )
    startup_timeout: float = Field(
        default=5.0,
        gt=0.0,
        description="Timeout in seconds for cache startup ping in fail-closed mode.",
    )

    @field_validator("cache_namespace")
    @classmethod
    def _validate_cache_namespace(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Cache namespace must be a non-empty string")
        stripped = value.strip()
        if not NAMESPACE_PATTERN.match(stripped):
            raise ValueError(
                f"Cache namespace must match '^[a-zA-Z0-9_-]+$', got {value!r}"
            )
        return stripped

    @field_validator("dns_cache_ttl")
    @classmethod
    def _validate_dns_cache_ttl(cls, value: str) -> str:
        try:
            parsed = ttl_to_seconds(value)
        except (ValueError, TypeError) as err:
            raise ValueError(f"Invalid duration for dns_cache_ttl: {value!r}") from err
        if parsed is None or parsed < 0:
            raise ValueError(
                f"dns_cache_ttl must evaluate to >= 0 seconds, got {value!r}"
            )
        return value

    @field_validator("cache_failure_policy", mode="before")
    @classmethod
    def _normalize_failure_policy(cls, value: str) -> str:
        if isinstance(value, str):
            val = value.strip().lower()
            if val not in {"fail-open", "fail-closed"}:
                raise ValueError(
                    "CACHE_FAILURE_POLICY must be either 'fail-open' or 'fail-closed'"
                )
            return val
        return value


__all__ = ["NAMESPACE_PATTERN", "CacheSettings"]
