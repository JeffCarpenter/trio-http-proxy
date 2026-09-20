"""Caching, circuit breaking, and rate limiting module for trio_http_proxy.

Re-exports components from dedicated modules. Configuration lives in the
typed settings models (see ``..config``); ``get_settings()`` is the lazy
accessor, ``get_default_proxy_cache()`` the lazy default-cache factory.
"""

from __future__ import annotations

from cashews import CircuitBreakerOpen

from ..circuit_breaker import (
    CircuitBreakerManager,
    CircuitPermit,
    CircuitState,
    DestinationCircuitBreaker,
    canonicalize_destination,
)
from ..config import (
    NAMESPACE_PATTERN,
    CacheSettings,
    CircuitBreakerSettings,
    RateLimitSettings,
    Settings,
    get_settings,
    normalize_failure_policy,
    validate_namespace,
)
from ..errors import CacheBackendUnavailable, RateLimitError
from ..proxy_cache import (
    BACKEND_EXCEPTIONS,
    LifecycleState,
    ProxyCache,
    ProxyCacheBuilder,
    ProxyCacheProtocol,
    get_client_key,
    get_default_proxy_cache,
)

__all__ = [
    "BACKEND_EXCEPTIONS",
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
    "ProxyCacheBuilder",
    "ProxyCacheProtocol",
    "RateLimitError",
    "RateLimitSettings",
    "Settings",
    "canonicalize_destination",
    "get_client_key",
    "get_default_proxy_cache",
    "get_settings",
    "normalize_failure_policy",
    "validate_namespace",
]
