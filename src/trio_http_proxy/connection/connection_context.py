"""Connection context variables and formatting helpers."""

from __future__ import annotations

from contextvars import ContextVar
from itertools import count
from typing import Any

import trio

CV_CLIENT_STREAM: ContextVar[trio.abc.Stream | None] = ContextVar(
    "client_stream", default=None
)
CV_DEST_STREAM: ContextVar[trio.abc.Stream | None] = ContextVar(
    "dest_stream", default=None
)
CV_PIPE_FROM: ContextVar[trio.abc.ReceiveStream | None] = ContextVar(
    "pipe_from", default=None
)
CV_DESTHOST: ContextVar[str | None] = ContextVar("desthost", default=None)
CV_CONN_ID: ContextVar[int | None] = ContextVar("conn_id", default=None)
_DEFAULT_PROXY_CACHE_SENTINEL = object()
CV_PROXY_CACHE: ContextVar[Any] = ContextVar(
    "proxy_cache", default=_DEFAULT_PROXY_CACHE_SENTINEL
)

_conn_ids = count(1)


def _connection_context() -> dict[str, Any]:
    conn_id = CV_CONN_ID.get()
    client_stream = CV_CLIENT_STREAM.get()
    dest_stream = CV_DEST_STREAM.get()
    desthost = CV_DESTHOST.get()
    pipe_from = CV_PIPE_FROM.get()
    direction = None
    prefix = ""

    if client_stream is not None:
        if dest_stream is not None:
            direction = "<>"
            if pipe_from is not None:
                direction = "->" if pipe_from is client_stream else "<-"
            prefix = f"[conn{conn_id} {direction} {desthost}]"
        else:
            prefix = f"[conn{conn_id}]"

    return {
        "conn_id": conn_id,
        "desthost": desthost,
        "direction": direction,
        "conn_prefix": prefix,
    }


def format_conn_prefix() -> str:
    return str(_connection_context()["conn_prefix"])


__all__ = [
    "CV_CLIENT_STREAM",
    "CV_CONN_ID",
    "CV_DESTHOST",
    "CV_DEST_STREAM",
    "CV_PIPE_FROM",
    "CV_PROXY_CACHE",
    "_DEFAULT_PROXY_CACHE_SENTINEL",
    "_conn_ids",
    "_connection_context",
    "format_conn_prefix",
]
