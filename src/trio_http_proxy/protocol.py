# Copyright 2018-2019 Joshua Bronson.    All Rights Reserved.
# Copyright 2026      Jeffrey Carpenter.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import annotations

import logging
from urllib.parse import urlsplit

import h11
import trio

from . import http
from .config import get_settings
from .connection import ConnectionFilter, ConnectionLoggerAdapter
from .errors import ProxyProtocolError, RequestTooLargeError

LOGGER_NAME = "trio_http_proxy"
_raw_logger = logging.getLogger(LOGGER_NAME)
_raw_logger.addFilter(ConnectionFilter())
logger = ConnectionLoggerAdapter(_raw_logger, {})


async def process_as_connect_request(
    stream: trio.abc.ReceiveStream,
    bufmaxsz: int | None = None,
    maxreqsz: int | None = None,
    read_timeout: float | None = None,
) -> tuple[str, int, bytes]:
    """Read a stream expected to contain a valid HTTP CONNECT request to desthost:destport.

    Parses incrementally via h11 without raw byte searching. Returns
    (destination host, port, trailing_bytes).
    See https://tools.ietf.org/html/rfc7231#section-4.3.6 for the CONNECT spec.
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout
    seconds.
    """
    _proxy_cfg = get_settings()
    bufmaxsz = bufmaxsz if bufmaxsz is not None else _proxy_cfg.bufmaxsz
    maxreqsz = maxreqsz if maxreqsz is not None else _proxy_cfg.maxreqsz
    read_timeout = (
        read_timeout if read_timeout is not None else _proxy_cfg.connect_read_timeout
    )
    logger.debug("Reading CONNECT request...")

    h11_conn = h11.Connection(
        our_role=h11.SERVER,
        max_incomplete_event_size=maxreqsz,
    )
    total_received = 0

    with trio.fail_after(read_timeout):
        while True:
            try:
                event = h11_conn.next_event()
            except h11.RemoteProtocolError as exc:
                if exc.error_status_hint == 431:
                    raise RequestTooLargeError(
                        f"Request did not fit in {maxreqsz} bytes"
                    ) from exc
                raise

            if event is h11.NEED_DATA:
                if total_received >= maxreqsz:
                    raise RequestTooLargeError(
                        f"Request did not fit in {maxreqsz} bytes"
                    )
                chunk = await stream.receive_some(bufmaxsz)
                if not chunk:
                    h11_conn.receive_data(b"")
                else:
                    total_received += len(chunk)
                    h11_conn.receive_data(bytes(chunk))
                continue
            break

    if not isinstance(event, h11.Request):
        raise ProxyProtocolError(f"{event=} is not a h11.Request")

    trailing_data, _ = h11_conn.trailing_data
    header_len = total_received - len(trailing_data)
    if header_len > maxreqsz:
        raise RequestTooLargeError(f"Request did not fit in {maxreqsz} bytes")
    if event.method != http.http_method_connect:
        raise ProxyProtocolError(f"{event.method=} != CONNECT")

    target = event.target
    try:
        decoded_target = target.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ProxyProtocolError(f"{target=} contains invalid octets") from exc

    try:
        parsed = urlsplit(f"//{decoded_target}")
    except ValueError as exc:
        raise ProxyProtocolError(f"{target=} is not a valid authority") from exc

    if (
        parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise ProxyProtocolError(f"{target=} is not authority-form (RFC 7231 4.3.6)")

    try:
        desthost, destport = parsed.hostname, parsed.port
    except ValueError as e:
        raise ProxyProtocolError(f"{target=} has an invalid port") from e

    if destport is None or not (1 <= destport <= 65535):
        raise ProxyProtocolError(f"{target=} has an invalid port range")
    if not desthost or not desthost.strip():
        raise ProxyProtocolError(f"{target=} missing host")

    trailing_data, _ = h11_conn.trailing_data
    return desthost, destport, trailing_data


__all__ = [
    "process_as_connect_request",
]
