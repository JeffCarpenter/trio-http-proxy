"""Configuration package for trio_http_proxy based on pydantic-settings.

Exports strongly typed, environment- and .env-aware configuration models:
- Settings: Unified root settings model with direct field access.
- ProxySettings, CacheSettings, CircuitBreakerSettings, RateLimitSettings:
  backward-compatible aliases to Settings.
- get_settings()/reset_settings_cache(): lazy process-wide settings access.

Every field accepts the canonical ``TRIO_HTTP_PROXY_<FIELD>`` env var (wins)
plus its legacy unprefixed name for backward compatibility.
"""

from ._base import (
    ENV_PREFIX,
    EnvSettings,
    normalize_failure_policy,
    normalize_optional_duration,
    parse_duration_seconds,
    validate_duration_string,
    validate_namespace,
)
from .builder import ProxyCacheBuilder, SettingsBuilderMixin
from .settings import (
    Settings,
    get_settings,
    reset_settings_cache,
    resolve_settings,
)

__all__ = [
    "ENV_PREFIX",
    "EnvSettings",
    "ProxyCacheBuilder",
    "Settings",
    "SettingsBuilderMixin",
    "get_settings",
    "normalize_failure_policy",
    "normalize_optional_duration",
    "parse_duration_seconds",
    "reset_settings_cache",
    "resolve_settings",
    "validate_duration_string",
    "validate_namespace",
]
