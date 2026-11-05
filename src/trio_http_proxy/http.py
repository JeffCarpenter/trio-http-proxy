"""HTTP wire-format constants for the minimal Trio HTTP proxy."""

from __future__ import annotations

HTTP_METHOD_CONNECT: bytes = b"CONNECT"
http_method_connect: bytes = HTTP_METHOD_CONNECT

HTTP_200_CONNECTION_ESTABLISHED: bytes = b"HTTP/1.1 200 Connection established\r\n\r\n"
http_200_connection_established: bytes = HTTP_200_CONNECTION_ESTABLISHED

HTTP_400_BAD_REQUEST: bytes = (
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_400_bad_request: bytes = HTTP_400_BAD_REQUEST

HTTP_408_REQUEST_TIMEOUT: bytes = (
    b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_408_request_timeout: bytes = HTTP_408_REQUEST_TIMEOUT

HTTP_414_URI_TOO_LONG: bytes = (
    b"HTTP/1.1 414 URI Too Long\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_414_uri_too_long: bytes = HTTP_414_URI_TOO_LONG

HTTP_502_BAD_GATEWAY: bytes = (
    b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_502_bad_gateway: bytes = HTTP_502_BAD_GATEWAY

HTTP_504_GATEWAY_TIMEOUT: bytes = (
    b"HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_504_gateway_timeout: bytes = HTTP_504_GATEWAY_TIMEOUT

__all__ = [
    "HTTP_200_CONNECTION_ESTABLISHED",
    "HTTP_400_BAD_REQUEST",
    "HTTP_408_REQUEST_TIMEOUT",
    "HTTP_414_URI_TOO_LONG",
    "HTTP_502_BAD_GATEWAY",
    "HTTP_504_GATEWAY_TIMEOUT",
    "HTTP_METHOD_CONNECT",
    "http_200_connection_established",
    "http_400_bad_request",
    "http_408_request_timeout",
    "http_414_uri_too_long",
    "http_502_bad_gateway",
    "http_504_gateway_timeout",
    "http_method_connect",
]
