"""HTTP wire-format constants for the minimal Trio HTTP proxy."""

from __future__ import annotations

HTTP_METHOD_CONNECT: bytes = b"CONNECT"

HTTP_200_CONNECTION_ESTABLISHED: bytes = b"HTTP/1.1 200 Connection established\r\n\r\n"

HTTP_400_BAD_REQUEST: bytes = (
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

HTTP_408_REQUEST_TIMEOUT: bytes = (
    b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

HTTP_414_URI_TOO_LONG: bytes = (
    b"HTTP/1.1 414 URI Too Long\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

HTTP_502_BAD_GATEWAY: bytes = (
    b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

HTTP_504_GATEWAY_TIMEOUT: bytes = (
    b"HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)

__all__ = [
    "HTTP_200_CONNECTION_ESTABLISHED",
    "HTTP_400_BAD_REQUEST",
    "HTTP_408_REQUEST_TIMEOUT",
    "HTTP_414_URI_TOO_LONG",
    "HTTP_502_BAD_GATEWAY",
    "HTTP_504_GATEWAY_TIMEOUT",
    "HTTP_METHOD_CONNECT",
]
