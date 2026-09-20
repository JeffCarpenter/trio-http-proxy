# Copyright 2018-2019 Joshua Bronson.    All Rights Reserved.
# Copyright 2026      Jeffrey Carpenter.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import annotations

import logging
import re
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
    stream,
    bufmaxsz: int | None = None,
    maxreqsz: int | None = None,
    read_timeout: float | None = None,
):
    """Read a stream expected to contain a valid HTTP CONNECT request to desthost:destport.
    Parse and return (destination host, port, trailing_bytes). Validate (lightly) and raise if request invalid.
    See https://tools.ietf.org/html/rfc7231#section-4.3.6 for the CONNECT spec.
    Raises trio.TooSlowError if the complete request doesn't arrive within read_timeout
    seconds -- guards against a slowloris-style client trickling bytes forever.
    """
    _proxy_cfg = get_settings()
    bufmaxsz = bufmaxsz if bufmaxsz is not None else _proxy_cfg.bufmaxsz
    maxreqsz = maxreqsz if maxreqsz is not None else _proxy_cfg.maxreqsz
    read_timeout = (
        read_timeout if read_timeout is not None else _proxy_cfg.connect_read_timeout
    )
    logger.debug("Reading...")
    raw_buf = bytearray()
    with trio.fail_after(read_timeout):
        while True:
            match = re.search(rb"\n\r?\n", raw_buf)
            if match is not None:
                header_len = match.end()
                if header_len > maxreqsz:
                    raise RequestTooLargeError(
                        f"Request did not fit in {maxreqsz} bytes"
                    )
                break
            if len(raw_buf) >= maxreqsz:
                raise RequestTooLargeError(f"Request did not fit in {maxreqsz} bytes")

            chunk = await stream.receive_some(bufmaxsz)
            if not chunk:
                break
            raw_buf.extend(chunk)

    h11_conn = h11.Connection(
        our_role=h11.SERVER, max_incomplete_event_size=max(maxreqsz, len(raw_buf))
    )
    h11_conn.receive_data(bytes(raw_buf))
    if not raw_buf or not re.search(rb"\n\r?\n", raw_buf):
        h11_conn.receive_data(b"")
    h11_nextevt = h11_conn.next_event()

    if not isinstance(h11_nextevt, h11.Request):
        raise ProxyProtocolError(f"{h11_nextevt=} is not a h11.Request")
    if h11_nextevt.method != http.http_method_connect:
        raise ProxyProtocolError(f"{h11_nextevt.method=} != CONNECT")
    target = h11_nextevt.target
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
    if destport is None:
        raise ProxyProtocolError(f"{target=} missing port")
    if desthost is None:
        raise ProxyProtocolError(f"{target=} missing host")

    trailing_data, _ = h11_conn.trailing_data

    return desthost, destport, trailing_data


__all__ = [
    "ProxyProtocolError",
    "RequestTooLargeError",
    "process_as_connect_request",
]
