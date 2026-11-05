"""Caching and lifecycle management module for trio_http_proxy."""

from __future__ import annotations

from ..dependencies import CacheBackend, HostResolver, ThreadFactory, ThreadHandle
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
    "CacheBackend",
    "HostResolver",
    "LifecycleState",
    "ProxyCache",
    "ProxyCacheBuilder",
    "ProxyCacheProtocol",
    "ThreadFactory",
    "ThreadHandle",
    "get_client_key",
    "get_default_proxy_cache",
]
