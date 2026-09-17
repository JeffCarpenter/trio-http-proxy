"""ConnectionFilter logging filter for connection context injection."""

from __future__ import annotations

import logging

from .connection_context import _connection_context


class ConnectionFilter(logging.Filter):
    """Ensure connection attributes (conn_id, desthost, direction, conn_prefix)
    are always populated on LogRecord instances."""

    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _connection_context()
        for k, v in ctx.items():
            if not hasattr(record, k):
                setattr(record, k, v)
        return True


__all__ = ["ConnectionFilter"]
