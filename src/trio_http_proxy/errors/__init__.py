"""Exception subclasses and error definitions for trio_http_proxy."""

from cashews import CircuitBreakerOpen

from .cache_backend_unavailable import CacheBackendUnavailable
from .proxy_protocol_error import ProxyProtocolError
from .rate_limit_error import RateLimitError
from .request_too_large_error import RequestTooLargeError

__all__ = [
    "CacheBackendUnavailable",
    "CircuitBreakerOpen",
    "ProxyProtocolError",
    "RateLimitError",
    "RequestTooLargeError",
]
