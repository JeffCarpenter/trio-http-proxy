#!/usr/bin/env python3

# Copyright 2018-2019 Joshua Bronson. All Rights Reserved.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from contextvars import ContextVar
from functools import partial
from io import DEFAULT_BUFFER_SIZE
from itertools import count
from os import getenv
from textwrap import indent
from traceback import format_exc
from urllib.parse import urlsplit

import h11
import trio


def _int_env(name, default, _getenv=getenv):
    return int(_getenv(name, default))


DEFAULT_PORT = 8080
PORT = _int_env("PORT", DEFAULT_PORT)

BUFMAXSZ = _int_env("BUFMAXSZ", DEFAULT_BUFFER_SIZE)
DEFAULT_MAXREQSZ = 16384
MAXREQSZ = _int_env("MAXREQSZ", DEFAULT_MAXREQSZ)

prn = partial(print, end="")
indented = partial(indent, prefix="  ")

CV_CLIENT_STREAM: ContextVar[trio.SocketStream | None] = ContextVar(
    "client_stream", default=None
)
CV_DEST_STREAM: ContextVar[trio.SocketStream | None] = ContextVar(
    "dest_stream", default=None
)
CV_PIPE_FROM: ContextVar[trio.SocketStream | None] = ContextVar(
    "pipe_from", default=None
)
CV_DESTHOST: ContextVar[str | None] = ContextVar("desthost", default=None)
CV_CONN_ID: ContextVar[int | None] = ContextVar("conn_id", default=None)

_conn_ids = count(1)


async def http_proxy(
    client_stream, _nextid=_conn_ids.__next__, _connect=trio.open_tcp_stream
):
    CV_CONN_ID.set(_nextid())
    CV_CLIENT_STREAM.set(client_stream)
    async with client_stream:
        try:
            dest_stream = await tunnel(client_stream, _connect=_connect)
        except AssertionError, ValueError, h11.ProtocolError, OSError:
            return  # tunnel() already logged and notified the client
        try:
            async with dest_stream, trio.open_nursery() as nursery:
                nursery.start_soon(pipe, client_stream, dest_stream)
                nursery.start_soon(pipe, dest_stream, client_stream)
        except* trio.BrokenResourceError, trio.ClosedResourceError:
            log("Peer disconnected")
        except* Exception:  # noqa: BLE001 -- unexpected bug; isolate so trio.serve_tcp keeps serving other connections
            log(f"\n{indented(format_exc())}")


async def start_server(server=http_proxy, port=PORT, _serve=trio.serve_tcp):
    print(f"* Starting {server.__name__} on port {port or '(OS-selected port)'}...")
    try:
        await _serve(server, port)
    except KeyboardInterrupt:
        print("\nGoodbye for now.")


async def tunnel(client_stream, _connect=trio.open_tcp_stream):
    """Given a stream from a client containing an HTTP CONNECT request,
    open a connection to the destination server specified in the CONNECT request,
    and notify the client when the end-to-end connection has been established.
    Return the destination stream and the corresponding host.
    """
    try:
        desthost, destport = await process_as_connect_request(client_stream)
    except AssertionError, ValueError, h11.ProtocolError:
        log(f"Bad CONNECT request, sending 400 to client...\n{indented(format_exc())}")
        await client_stream.send_all(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        raise
    log(f"Got CONNECT request for {desthost}:{destport}, connecting...")
    try:
        dest_stream = await _connect(desthost, destport)
    except OSError:
        log(
            f"Could not connect to {desthost}:{destport}, sending 502 to client...\n{indented(format_exc())}"
        )
        await client_stream.send_all(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
        raise
    CV_DEST_STREAM.set(dest_stream)
    CV_DESTHOST.set(desthost)
    log(f"Connected to {desthost}, sending 200 to client...")
    await client_stream.send_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
    log("Sent 200 to client, tunnel established.")
    return dest_stream


async def process_as_connect_request(stream, bufmaxsz=BUFMAXSZ, maxreqsz=MAXREQSZ):
    """Read a stream expected to contain a valid HTTP CONNECT request to desthost:destport.
    Parse and return the destination host. Validate (lightly) and raise if request invalid.
    See https://tools.ietf.org/html/rfc7231#section-4.3.6 for the CONNECT spec.
    """
    log("Reading...")
    h11_conn = h11.Connection(our_role=h11.SERVER)
    total_bytes_read = 0
    while (h11_nextevt := h11_conn.next_event()) == h11.NEED_DATA:
        bytes_read = await stream.receive_some(bufmaxsz)
        total_bytes_read += len(bytes_read)
        assert total_bytes_read < maxreqsz, f"Request did not fit in {maxreqsz} bytes"
        h11_conn.receive_data(bytes_read)
    assert isinstance(h11_nextevt, h11.Request), f"{h11_nextevt=} is not a h11.Request"
    assert h11_nextevt.method == b"CONNECT", f"{h11_nextevt.method=} != CONNECT"
    # urlsplit handles bracketed IPv6 authorities (e.g. "[::1]:443") correctly;
    # a naive split on the first ":" tears such addresses apart.
    parsed = urlsplit(f"//{h11_nextevt.target.decode()}")
    desthost, destport = parsed.hostname, parsed.port
    if destport is None:
        raise ValueError(f"{h11_nextevt.target=} missing port")
    assert desthost is not None, f"{h11_nextevt.target=} missing host"
    return desthost, destport


async def pipe(from_stream, to_stream, bufmaxsz=BUFMAXSZ):
    CV_PIPE_FROM.set(from_stream)
    while chunk := await from_stream.receive_some(bufmaxsz):
        await to_stream.send_all(chunk)
        log(f"Forwarded {len(chunk)} bytes")
    log("Pipe finished")


def log(*args, **kw):
    client_stream = CV_CLIENT_STREAM.get()
    if client_stream:
        prn(f"[conn{CV_CONN_ID.get()}")
        dest_stream = CV_DEST_STREAM.get()
        if dest_stream:
            direction = "<>"
            pipe_from = CV_PIPE_FROM.get()
            if pipe_from:
                direction = "->" if pipe_from is client_stream else "<-"
            prn(f" {direction} {CV_DESTHOST.get()}")
        prn("] ")
    print(*args, **kw)


if __name__ == "__main__":
    trio.run(start_server)
