"""Minimal HTTP CONNECT proxy built strictly on Trio structured concurrency."""

from . import errors, http, protocol, proxy, streaming
from .errors import ProxyError, ProxyProtocolError, RequestTooLargeError
from .protocol import parse_connect_target, process_as_connect_request
from .proxy import (
    http_proxy,
    main,
    start_server,
    tunnel,
)
from .streaming import pipe, pipe_with_timeout

__all__ = [
    "ProxyError",
    "ProxyProtocolError",
    "RequestTooLargeError",
    "errors",
    "http",
    "http_proxy",
    "main",
    "parse_connect_target",
    "pipe",
    "pipe_with_timeout",
    "process_as_connect_request",
    "protocol",
    "proxy",
    "start_server",
    "streaming",
    "tunnel",
]
