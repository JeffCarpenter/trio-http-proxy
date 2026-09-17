"""Caching, circuit breaking, and rate limiting module for trio_http_proxy.

This module re-exports components extracted into dedicated modules for
backward compatibility.
"""

from cashews import CircuitBreakerOpen

from ..circuit_breaker import (
    DEFAULT_MAX_DESTINATIONS,
    CircuitBreakerManager,
    CircuitPermit,
    CircuitState,
    DestinationCircuitBreaker,
    canonicalize_destination,
)
from ..errors import CacheBackendUnavailable, RateLimitError
from ..proxy_cache import (
    BACKEND_EXCEPTIONS,
    CACHE_FAILURE_POLICY,
    DEFAULT_CACHE_ENABLED,
    DEFAULT_CACHE_FAILURE_POLICY,
    DEFAULT_CACHE_NAMESPACE,
    DEFAULT_CACHE_SETTINGS,
    DEFAULT_CACHE_URL,
    DEFAULT_CB_SETTINGS,
    DEFAULT_CIRCUIT_BREAKER_ENABLED,
    DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE,
    DEFAULT_CIRCUIT_BREAKER_MIN_CALLS,
    DEFAULT_CIRCUIT_BREAKER_PERIOD,
    DEFAULT_CIRCUIT_BREAKER_TTL,
    DEFAULT_DNS_CACHE_TTL,
    DEFAULT_PROBE_INTERVAL,
    DEFAULT_PROXY_CACHE,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RATE_LIMIT_ENABLED,
    DEFAULT_RATE_LIMIT_PERIOD,
    DEFAULT_RATE_LIMIT_TTL,
    DEFAULT_RL_SETTINGS,
    NAMESPACE_PATTERN,
    CacheSettings,
    CircuitBreakerSettings,
    ProxyCache,
    RateLimitSettings,
    Settings,
    get_client_key,
)
from .lifecycle_state import LifecycleState

__all__ = [
    "BACKEND_EXCEPTIONS",
    "CACHE_FAILURE_POLICY",
    "DEFAULT_CACHE_ENABLED",
    "DEFAULT_CACHE_FAILURE_POLICY",
    "DEFAULT_CACHE_NAMESPACE",
    "DEFAULT_CACHE_SETTINGS",
    "DEFAULT_CACHE_URL",
    "DEFAULT_CB_SETTINGS",
    "DEFAULT_CIRCUIT_BREAKER_ENABLED",
    "DEFAULT_CIRCUIT_BREAKER_ERRORS_RATE",
    "DEFAULT_CIRCUIT_BREAKER_MIN_CALLS",
    "DEFAULT_CIRCUIT_BREAKER_PERIOD",
    "DEFAULT_CIRCUIT_BREAKER_TTL",
    "DEFAULT_DNS_CACHE_TTL",
    "DEFAULT_MAX_DESTINATIONS",
    "DEFAULT_PROBE_INTERVAL",
    "DEFAULT_PROXY_CACHE",
    "DEFAULT_RATE_LIMIT",
    "DEFAULT_RATE_LIMIT_ENABLED",
    "DEFAULT_RATE_LIMIT_PERIOD",
    "DEFAULT_RATE_LIMIT_TTL",
    "DEFAULT_RL_SETTINGS",
    "NAMESPACE_PATTERN",
    "CacheBackendUnavailable",
    "CacheSettings",
    "CircuitBreakerManager",
    "CircuitBreakerOpen",
    "CircuitBreakerSettings",
    "CircuitPermit",
    "CircuitState",
    "DestinationCircuitBreaker",
    "LifecycleState",
    "ProxyCache",
    "RateLimitError",
    "RateLimitSettings",
    "Settings",
    "canonicalize_destination",
    "get_client_key",
]
