"""Sans-io HTTP CONNECT protocol: no sockets, clocks or event loop.

Feed bytes in with ConnectParser.receive_data(), pull the parsed request out
with next_request(). Callers own all I/O and timeouts.
"""

from __future__ import annotations

from collections.abc import Buffer
from typing import NamedTuple
from urllib.parse import urlsplit

import h11

from . import http
from .errors import ProxyProtocolError, RequestTooLargeError


class ConnectRequest(NamedTuple):
    """A validated CONNECT request plus any bytes that followed its headers."""

    host: str
    port: int
    trailing_data: bytes


def parse_connect_target(target: bytes) -> tuple[str, int]:
    """Parse and validate an RFC 7231 §4.3.6 authority-form CONNECT request-target.

    Must be host:port only. Rejects userinfo, path, query, and fragment delimiters.
    Correctly parses bracketed IPv6 literals (e.g. "[::1]:443").
    Raises ProxyProtocolError on invalid target.
    """
    try:
        decoded_target = target.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ProxyProtocolError(f"{target!r} contains invalid octets") from exc

    if any(ord(c) <= 0x20 or ord(c) >= 0x7F for c in decoded_target):
        raise ProxyProtocolError(f"{target!r} contains control or whitespace octets")

    if any(c in decoded_target for c in ("@", "?", "#", "/", "\\", "%")):
        raise ProxyProtocolError(f"{target!r} is not authority-form (RFC 7231 §4.3.6)")

    try:
        parsed = urlsplit(f"//{decoded_target}")
    except ValueError as exc:
        raise ProxyProtocolError(f"{target!r} is not a valid authority") from exc

    try:
        desthost, destport = parsed.hostname, parsed.port
    except ValueError as e:
        raise ProxyProtocolError(f"{target!r} has an invalid port") from e

    if destport is None or not (1 <= destport <= 65535):
        raise ProxyProtocolError(f"{target!r} has an invalid port range")
    if not desthost:
        raise ProxyProtocolError(f"{target!r} missing host")
    if not decoded_target.startswith("["):
        try:
            desthost.encode("idna")
        except UnicodeError as exc:
            raise ProxyProtocolError(f"{target!r} has an invalid host") from exc

    return desthost, destport


class ConnectParser:
    """Incremental, size-bounded parser for one CONNECT request.

    Call next_request() first and after every receive_data()/receive_eof().
    It returns None while more bytes are needed, then the ConnectRequest.
    Raises RequestTooLargeError once maxreqsz bytes arrive without a complete
    header block, and ProxyProtocolError for any other malformed input.

    The parser is finished once next_request() returns a request or raises.
    From then on next_request() repeats that same result. receive_data()
    raises RuntimeError, because any later bytes belong to the tunnel and must
    be forwarded, not parsed or dropped. receive_data() after receive_eof()
    also raises RuntimeError. receive_eof() is always safe to call.
    """

    def __init__(self, *, maxreqsz: int = 16384) -> None:
        if maxreqsz < 1:
            raise ValueError(f"maxreqsz must be >= 1, got {maxreqsz}")
        self._maxreqsz = maxreqsz
        self._conn = h11.Connection(
            our_role=h11.SERVER,
            max_incomplete_event_size=maxreqsz,
        )
        self._received = 0
        self._eof = False
        self._outcome: ConnectRequest | ProxyProtocolError | None = None

    def receive_data(self, data: Buffer) -> None:
        """Buffer bytes from the peer. Empty data is ignored; use receive_eof()."""
        chunk = bytes(data)
        if not chunk:
            return
        if self._outcome is not None:
            raise RuntimeError(
                "ConnectParser is finished; forward further bytes to the tunnel"
            )
        if self._eof:
            raise RuntimeError("ConnectParser got data after receive_eof()")
        self._received += len(chunk)
        self._conn.receive_data(chunk)

    def receive_eof(self) -> None:
        """Record that the peer closed its sending side."""
        self._eof = True
        self._conn.receive_data(b"")

    def next_request(self) -> ConnectRequest | None:
        """Return the request, None if more bytes are needed, or raise."""
        if isinstance(self._outcome, ProxyProtocolError):
            raise self._outcome
        if self._outcome is not None:
            return self._outcome
        try:
            request = self._poll()
        except ProxyProtocolError as exc:
            self._outcome = exc
            raise
        self._outcome = request
        return request

    def _poll(self) -> ConnectRequest | None:
        try:
            event = self._conn.next_event()
        except h11.RemoteProtocolError as exc:
            if exc.error_status_hint == 431:
                raise RequestTooLargeError(
                    f"Request exceeded {self._maxreqsz} bytes"
                ) from exc
            raise ProxyProtocolError(str(exc)) from exc

        if event is h11.NEED_DATA:
            if self._received >= self._maxreqsz:
                raise RequestTooLargeError(f"Request exceeded {self._maxreqsz} bytes")
            return None

        if not isinstance(event, h11.Request):
            raise ProxyProtocolError(f"Expected h11.Request, got {event!r}")

        trailing_data, _ = self._conn.trailing_data
        if self._received - len(trailing_data) > self._maxreqsz:
            raise RequestTooLargeError(f"Request exceeded {self._maxreqsz} bytes")

        if event.method != http.HTTP_METHOD_CONNECT:
            raise ProxyProtocolError(f"Expected CONNECT method, got {event.method!r}")

        if any(
            name in (b"content-length", b"transfer-encoding")
            for name, _ in event.headers
        ):
            raise ProxyProtocolError("CONNECT request must not contain framing headers")

        host, port = parse_connect_target(event.target)
        return ConnectRequest(host, port, trailing_data)


def error_response(exc: ProxyProtocolError) -> bytes:
    """Return the HTTP error response for a failed CONNECT parse."""
    if isinstance(exc, RequestTooLargeError):
        return http.HTTP_414_URI_TOO_LONG
    return http.HTTP_400_BAD_REQUEST


__all__ = [
    "ConnectParser",
    "ConnectRequest",
    "error_response",
    "parse_connect_target",
]
