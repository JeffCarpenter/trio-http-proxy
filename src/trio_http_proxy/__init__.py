"""Minimal HTTP CONNECT proxy built strictly on Trio structured concurrency."""

from . import config, errors, http, protocol, proxy, streaming
from .config import ProxyConfig
from .errors import ProxyError, ProxyProtocolError, RequestTooLargeError
from .http import (
    http_200_connection_established,
    http_400_bad_request,
    http_408_request_timeout,
    http_414_uri_too_long,
    http_502_bad_gateway,
    http_504_gateway_timeout,
    http_method_connect,
)
from .protocol import parse_connect_target, process_as_connect_request
from .proxy import (
    get_conn_limiter,
    http_proxy,
    main,
    reset_conn_limiter,
    start_server,
    tunnel,
)
from .streaming import pipe, pipe_with_timeout

__all__ = [
    "ProxyConfig",
    "ProxyError",
    "ProxyProtocolError",
    "RequestTooLargeError",
    "config",
    "errors",
    "get_conn_limiter",
    "http",
    "http_200_connection_established",
    "http_400_bad_request",
    "http_408_request_timeout",
    "http_414_uri_too_long",
    "http_502_bad_gateway",
    "http_504_gateway_timeout",
    "http_method_connect",
    "http_proxy",
    "main",
    "parse_connect_target",
    "pipe",
    "pipe_with_timeout",
    "process_as_connect_request",
    "protocol",
    "proxy",
    "reset_conn_limiter",
    "start_server",
    "streaming",
    "tunnel",
]
