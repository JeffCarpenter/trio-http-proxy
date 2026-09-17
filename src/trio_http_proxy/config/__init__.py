"""Configuration package for trio_http_proxy based on pydantic-settings.

Exports strongly typed, environment- and .env-aware configuration models:
- ProxySettings: Networking, buffer sizes, timeouts, concurrency, logging.
- CacheSettings: Cache backend URL, enabling, namespace, failure policy, probe interval.
- CircuitBreakerSettings: Error rate thresholds, periods, TTLs, min calls, destination caps.
- RateLimitSettings: Enabling, rate limit thresholds, periods, TTLs.
- Settings: Unified root settings composing all sub-configurations.
"""

from .cache_settings import NAMESPACE_PATTERN, CacheSettings
from .circuit_breaker_settings import CircuitBreakerSettings
from .proxy_settings import ProxySettings
from .rate_limit_settings import RateLimitSettings
from .settings import DEFAULT_SETTINGS, Settings

__all__ = [
    "DEFAULT_SETTINGS",
    "NAMESPACE_PATTERN",
    "CacheSettings",
    "CircuitBreakerSettings",
    "ProxySettings",
    "RateLimitSettings",
    "Settings",
]
