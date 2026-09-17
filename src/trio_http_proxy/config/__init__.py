"""Configuration package for trio_http_proxy based on pydantic-settings.

Exports strongly typed, environment- and .env-aware configuration models:
- ProxySettings: Networking, buffer sizes, timeouts, concurrency, logging.
- CacheSettings: Cache backend URL, enabling, namespace, failure policy, probe interval.
- CircuitBreakerSettings: Error rate thresholds, periods, TTLs, min calls, destination caps.
- RateLimitSettings: Enabling, rate limit thresholds, periods, TTLs.
- Settings: Unified root settings composing all sub-configurations.
- get_settings()/reset_settings_cache(): lazy process-wide settings access.

Every field accepts the canonical ``TRIO_HTTP_PROXY_<FIELD>`` env var (wins)
plus its legacy unprefixed name for backward compatibility.
"""

from ._base import (
    ENV_PREFIX,
    EnvSettings,
    normalize_optional_duration,
    parse_duration_seconds,
    prefixed,
)
from .cache_settings import (
    NAMESPACE_PATTERN,
    CacheSettings,
    normalize_failure_policy,
    validate_namespace,
)
from .circuit_breaker_settings import CircuitBreakerSettings
from .proxy_settings import ProxySettings
from .rate_limit_settings import RateLimitSettings
from .settings import Settings, get_settings, reset_settings_cache

__all__ = [
    "ENV_PREFIX",
    "NAMESPACE_PATTERN",
    "CacheSettings",
    "CircuitBreakerSettings",
    "EnvSettings",
    "ProxySettings",
    "RateLimitSettings",
    "Settings",
    "get_settings",
    "normalize_failure_policy",
    "normalize_optional_duration",
    "parse_duration_seconds",
    "prefixed",
    "reset_settings_cache",
    "validate_namespace",
]
