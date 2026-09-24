"""Connection context variables and formatting helpers."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from itertools import count
from typing import Any, Iterator

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


class ConnectionContext:
    """Encapsulates isolated state for a single proxy connection.

    In a shared-nothing architecture, each connection owns its identity,
    streams, target destination, configuration, and logging context.
    """

    def __init__(
        self,
        conn_id: int | None = None,
        client_stream: trio.abc.Stream | None = None,
        settings: Any = None,
        proxy_cache: Any = _DEFAULT_PROXY_CACHE_SENTINEL,
    ) -> None:
        self.conn_id: int = conn_id if conn_id is not None else next(_conn_ids)
        self.client_stream: trio.abc.Stream | None = client_stream
        self.dest_stream: trio.abc.Stream | None = None
        self.desthost: str | None = None
        self.destport: int | None = None
        self.pipe_from: trio.abc.ReceiveStream | None = None
        self.settings: Any = settings
        self.proxy_cache: Any = proxy_cache

    @property
    def direction(self) -> str | None:
        """Data flow direction ('->', '<-', '<>', or None)."""
        if self.client_stream is not None and self.dest_stream is not None:
            if self.pipe_from is not None:
                return "->" if self.pipe_from is self.client_stream else "<-"
            return "<>"
        return None

    @property
    def conn_prefix(self) -> str:
        """Formatted connection prefix string for logging."""
        if self.client_stream is not None:
            if self.dest_stream is not None and self.desthost:
                direction = self.direction or "<>"
                return f"[conn{self.conn_id} {direction} {self.desthost}]"
            return f"[conn{self.conn_id}]"
        return ""

    def set_client_stream(self, stream: trio.abc.Stream | None) -> None:
        self.client_stream = stream
        CV_CLIENT_STREAM.set(stream)

    def set_desthost(self, desthost: str | None, destport: int | None = None) -> None:
        self.desthost = desthost
        if destport is not None:
            self.destport = destport
        CV_DESTHOST.set(desthost)

    def set_dest_stream(self, stream: trio.abc.Stream | None) -> None:
        self.dest_stream = stream
        CV_DEST_STREAM.set(stream)

    def set_pipe_from(self, stream: trio.abc.ReceiveStream | None) -> None:
        self.pipe_from = stream
        CV_PIPE_FROM.set(stream)

    def to_dict(self) -> dict[str, Any]:
        return {
            "conn_id": self.conn_id,
            "desthost": self.desthost,
            "direction": self.direction,
            "conn_prefix": self.conn_prefix,
        }

    @contextmanager
    def activate(self) -> Iterator[ConnectionContext]:
        """Activate this context in task-local ContextVars for backward compatibility."""
        tokens: list[tuple[ContextVar[Any], Token[Any]]] = [
            (CV_CURRENT_CONTEXT, CV_CURRENT_CONTEXT.set(self)),
            (CV_CONN_ID, CV_CONN_ID.set(self.conn_id)),
            (CV_CLIENT_STREAM, CV_CLIENT_STREAM.set(self.client_stream)),
            (CV_DEST_STREAM, CV_DEST_STREAM.set(self.dest_stream)),
            (CV_DESTHOST, CV_DESTHOST.set(self.desthost)),
            (CV_PIPE_FROM, CV_PIPE_FROM.set(self.pipe_from)),
        ]
        if self.proxy_cache is not _DEFAULT_PROXY_CACHE_SENTINEL:
            tokens.append((CV_PROXY_CACHE, CV_PROXY_CACHE.set(self.proxy_cache)))
        try:
            yield self
        finally:
            for var, tok in reversed(tokens):
                var.reset(tok)

    @classmethod
    def current(cls) -> ConnectionContext | None:
        """Return the active ConnectionContext for the current task, if any."""
        return CV_CURRENT_CONTEXT.get()


CV_CURRENT_CONTEXT: ContextVar[ConnectionContext | None] = ContextVar(
    "current_connection_context", default=None
)


def _connection_context() -> dict[str, Any]:
    ctx = CV_CURRENT_CONTEXT.get()
    if ctx is not None:
        conn_id = CV_CONN_ID.get()
        if conn_id is None:
            conn_id = ctx.conn_id
        client_stream = CV_CLIENT_STREAM.get() or ctx.client_stream
        dest_stream = CV_DEST_STREAM.get() or ctx.dest_stream
        desthost = CV_DESTHOST.get() or ctx.desthost
        pipe_from = CV_PIPE_FROM.get() or ctx.pipe_from
    else:
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
    "CV_CURRENT_CONTEXT",
    "CV_DESTHOST",
    "CV_DEST_STREAM",
    "CV_PIPE_FROM",
    "CV_PROXY_CACHE",
    "ConnectionContext",
    "_DEFAULT_PROXY_CACHE_SENTINEL",
    "_conn_ids",
    "_connection_context",
    "format_conn_prefix",
]
