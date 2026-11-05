"""HTTP CONNECT protocol parsing and RFC 7231 §4.3.6 validation."""

from __future__ import annotations

import logging
from urllib.parse import urlsplit

import h11
import trio

from . import http
from .config import ProxyConfig
from .errors import ProxyProtocolError, RequestTooLargeError

logger = logging.getLogger(__name__)


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

    if any(c in decoded_target for c in ("@", "?", "#", "/")):
        raise ProxyProtocolError(f"{target!r} is not authority-form (RFC 7231 §4.3.6)")

    try:
        parsed = urlsplit(f"//{decoded_target}")
    except ValueError as exc:
        raise ProxyProtocolError(f"{target!r} is not a valid authority") from exc

    if (
        parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ProxyProtocolError(f"{target!r} is not authority-form (RFC 7231 §4.3.6)")

    try:
        desthost, destport = parsed.hostname, parsed.port
    except ValueError as e:
        raise ProxyProtocolError(f"{target!r} has an invalid port") from e

    if destport is None or not (1 <= destport <= 65535):
        raise ProxyProtocolError(f"{target!r} has an invalid port range")
    if not desthost or not desthost.strip():
        raise ProxyProtocolError(f"{target!r} missing host")

    return desthost, destport


async def process_as_connect_request(
    stream: trio.abc.ReceiveStream,
    *,
    config: ProxyConfig | None = None,
    bufmaxsz: int | None = None,
    maxreqsz: int | None = None,
    read_timeout: float | None = None,
) -> tuple[str, int, bytes]:
    """Read a stream expected to contain a valid HTTP CONNECT request.

    Parses incrementally via h11 without raw byte searching.
    Returns (destination_host, destination_port, trailing_bytes).
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout.
    """
    cfg = config or ProxyConfig()
    req_bufmaxsz = bufmaxsz if bufmaxsz is not None else cfg.bufmaxsz
    req_maxreqsz = maxreqsz if maxreqsz is not None else cfg.maxreqsz
    req_read_timeout = (
        read_timeout if read_timeout is not None else cfg.connect_read_timeout
    )
    logger.debug("Reading CONNECT request...")

    h11_conn = h11.Connection(
        our_role=h11.SERVER,
        max_incomplete_event_size=req_maxreqsz,
    )
    total_received = 0

    with trio.fail_after(req_read_timeout):
        while True:
            try:
                event = h11_conn.next_event()
            except h11.RemoteProtocolError as exc:
                if exc.error_status_hint == 431:
                    raise RequestTooLargeError(
                        f"Request exceeded {req_maxreqsz} bytes"
                    ) from exc
                raise ProxyProtocolError(str(exc)) from exc

            if event is h11.NEED_DATA:
                if total_received >= req_maxreqsz:
                    raise RequestTooLargeError(
                        f"Request exceeded {req_maxreqsz} bytes"
                    )
                chunk = await stream.receive_some(req_bufmaxsz)
                if not chunk:
                    h11_conn.receive_data(b"")
                else:
                    total_received += len(chunk)
                    h11_conn.receive_data(bytes(chunk))
                continue
            break

    if not isinstance(event, h11.Request):
        raise ProxyProtocolError(f"Expected h11.Request, got {event!r}")

    trailing_data, _ = h11_conn.trailing_data
    header_len = total_received - len(trailing_data)
    if header_len > req_maxreqsz:
        raise RequestTooLargeError(f"Request exceeded {req_maxreqsz} bytes")

    if event.method != http.HTTP_METHOD_CONNECT:
        raise ProxyProtocolError(f"Expected CONNECT method, got {event.method!r}")

    desthost, destport = parse_connect_target(event.target)
    return desthost, destport, trailing_data


__all__ = [
    "parse_connect_target",
    "process_as_connect_request",
]
