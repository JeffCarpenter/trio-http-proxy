"""Cache backend and namespace configuration using pydantic-settings."""

from __future__ import annotations

import re
from functools import cached_property
from typing import Any, Literal

from pydantic import Field, FiniteFloat, field_validator

from ._base import (
    EnvSettings,
    parse_duration_seconds,
    prefixed,
    validate_duration_string,
)

NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

_FAILURE_POLICIES = frozenset({"fail-open", "fail-closed"})


def validate_namespace(value: str) -> str:
    """Strip and validate a cache namespace; single source of truth for this rule."""
    stripped = value.strip()
    if not stripped:
        raise ValueError("Cache namespace must be a non-empty string")
    if not NAMESPACE_PATTERN.match(stripped):
        raise ValueError(
            f"Cache namespace must match '^[a-zA-Z0-9_-]+$', got {value!r}"
        )
    return stripped


def normalize_failure_policy(value: str) -> str:
    """Normalize a failure policy string; single source of truth for this rule."""
    norm = value.strip().lower()
    if norm not in _FAILURE_POLICIES:
        raise ValueError(
            "CACHE_FAILURE_POLICY must be either 'fail-open' or 'fail-closed'"
        )
    return norm


class CacheSettings(EnvSettings):
    """Configuration settings for cashews cache backend, namespace, and probing.

    Duration fields use cashews TTL syntax (integer amount + s/m/h/d suffix).
    ``dns_cache_ttl`` of ``0s`` (or empty) disables DNS caching (pass-through).
    """

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
        gt=0.0,
        description="Timeout in seconds for cache startup ping in fail-closed mode.",
        validation_alias=prefixed("startup_timeout"),
    )

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

    @cached_property
    def dns_cache_ttl_seconds(self) -> float:
        """DNS cache TTL in seconds; ``0.0`` means disabled (pass-through)."""
        return parse_duration_seconds(
            self.dns_cache_ttl, field="dns_cache_ttl", allow_zero=True
        )


__all__ = [
    "NAMESPACE_PATTERN",
    "CacheSettings",
    "normalize_failure_policy",
    "validate_namespace",
]

