"""Exception subclasses and error definitions for trio_http_proxy."""

from __future__ import annotations

from typing import Any

import cashews
from cashews import CircuitBreakerOpen


class CacheBackendUnavailable(RuntimeError):
    """The configured cache backend could not service an operation."""


class ProxyProtocolError(ValueError):
    """The client's CONNECT request is malformed or violates the
    authority-form contract (RFC 7231 section 4.3.6). Raised in place of
    `assert` so validation survives `python -O` (asserts are stripped under
    optimization, silently turning every one of these checks into a no-op)."""


class RequestTooLargeError(ProxyProtocolError):
    """The CONNECT request exceeded maxreqsz before a complete request
    arrived. A subclass of ProxyProtocolError so it's still caught wherever
    that is, but distinguishable so tunnel() can reply 414 instead of 400."""


class RateLimitError(cashews.RateLimitError):
    """Rate-limit rejection with an optional HTTP Retry-After value."""

    def __init__(
        self,
        *args: Any,
        retry_after: int | None = None,
        **kwargs: Any,
    ) -> None:
        try:
            super().__init__(*args, **kwargs)
        except TypeError:
            super().__init__(*args)
        self.retry_after = retry_after

    @property
    def retry_after_seconds(self) -> float | None:
        if self.retry_after is None:
            return None
        try:
            return float(self.retry_after)
        except (TypeError, ValueError):
            return None


__all__ = [
    "CacheBackendUnavailable",
    "CircuitBreakerOpen",
    "ProxyProtocolError",
    "RateLimitError",
    "RequestTooLargeError",
]
