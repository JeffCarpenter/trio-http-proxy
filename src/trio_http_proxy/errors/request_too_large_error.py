"""RequestTooLargeError exception for CONNECT requests exceeding size limits."""

from .proxy_protocol_error import ProxyProtocolError


class RequestTooLargeError(ProxyProtocolError):
    """The CONNECT request exceeded maxreqsz before a complete request
    arrived. A subclass of ProxyProtocolError so it's still caught wherever
    that is, but distinguishable so tunnel() can reply 414 instead of 400."""


__all__ = ["RequestTooLargeError"]
