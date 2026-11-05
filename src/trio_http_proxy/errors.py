"""Exception subclasses and error definitions for the minimal Trio HTTP proxy."""

from __future__ import annotations


class ProxyError(Exception):
    """Base exception for all proxy errors."""


class ProxyProtocolError(ProxyError, ValueError):
    """The client's CONNECT request is malformed or violates the
    authority-form contract (RFC 7231 §4.3.6)."""


class RequestTooLargeError(ProxyProtocolError):
    """The CONNECT request exceeded maxreqsz before a complete request arrived.
    Distinguishable so the proxy replies 414 URI Too Long instead of 400 Bad Request."""


__all__ = [
    "ProxyError",
    "ProxyProtocolError",
    "RequestTooLargeError",
]
