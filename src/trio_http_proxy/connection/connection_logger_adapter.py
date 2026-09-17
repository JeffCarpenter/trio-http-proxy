"""ConnectionLoggerAdapter logging adapter for connection context formatting."""

from __future__ import annotations

import logging
from collections.abc import MutableMapping
from typing import Any

from .connection_context import _connection_context


class ConnectionLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that prepends connection context to log messages and injects
    structured attributes into LogRecord extra."""

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        ctx = _connection_context()
        prefix = ctx["conn_prefix"]
        if prefix:
            escaped_prefix = prefix.replace("%", "%%")
            msg = f"{escaped_prefix} {msg}"
        extra = kwargs.setdefault("extra", {})
        for k, v in ctx.items():
            extra.setdefault(k, v)
        return msg, kwargs

    def addHandler(self, hdlr: logging.Handler) -> None:
        self.logger.addHandler(hdlr)

    def removeHandler(self, hdlr: logging.Handler) -> None:
        self.logger.removeHandler(hdlr)

    def addFilter(self, fltr: logging.Filter | Any) -> None:
        self.logger.addFilter(fltr)

    def removeFilter(self, fltr: logging.Filter | Any) -> None:
        self.logger.removeFilter(fltr)


__all__ = ["ConnectionLoggerAdapter"]
