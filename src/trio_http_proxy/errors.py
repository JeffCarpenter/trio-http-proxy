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

    retry_after_seconds: float = 1.0
    retry_after: int | None = None

    def __init__(
        self,
        *args: Any,
        retry_after_seconds: float = 1.0,
        retry_after: float | int | None = None,
        **kwargs: Any,
    ) -> None:
        # cashews.RateLimitError inherits Exception.__init__(self, *args),
        # which accepts no keyword arguments at all -- so **kwargs here only
        # exists to let callers pass (and this class discard) unrecognized
        # legacy kwargs without raising, not to actually forward them.
        # A blanket `except TypeError` around the super().__init__() call
        # would also mask a genuine TypeError unrelated to kwargs, so just
        # drop kwargs explicitly instead of relying on the exception to
        # tell us they weren't accepted.
        del kwargs
        super().__init__(*args)
        if retry_after is not None:
            try:
                self.retry_after_seconds = float(retry_after)
            except (TypeError, ValueError):
                self.retry_after_seconds = float(retry_after_seconds)
            self.retry_after = (
                int(retry_after) if isinstance(retry_after, (int, float)) else None
            )
        else:
            try:
                self.retry_after_seconds = float(retry_after_seconds)
            except (TypeError, ValueError):
                self.retry_after_seconds = 1.0
            self.retry_after = (
                int(self.retry_after_seconds)
                if isinstance(retry_after_seconds, (int, float))
                else None
            )


__all__ = [
    "CacheBackendUnavailable",
    "CircuitBreakerOpen",
    "ProxyProtocolError",
    "RateLimitError",
    "RequestTooLargeError",
]
