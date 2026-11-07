"""Minimal HTTP CONNECT proxy built strictly on Trio structured concurrency.

Public names are exported lazily (PEP 562). Importing the package, or one of the
sans-io modules (``protocol``, ``timeouts``, ``errors``, ``http``), does not import
Trio; the Trio-backed modules (``proxy``, ``streaming``) load on first access to
one of their names.
"""

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Static view for type checkers only; at runtime these names load lazily.
    from . import errors, http, protocol, proxy, streaming, timeouts
    from .errors import ProxyError, ProxyProtocolError, RequestTooLargeError
    from .protocol import (
        ConnectParser,
        ConnectRequest,
        error_response,
        parse_connect_target,
    )
    from .proxy import (
        http_proxy,
        main,
        process_as_connect_request,
        start_server,
        tunnel,
    )
    from .streaming import pipe, pipe_with_timeout
    from .timeouts import PipeTimeouts

__all__ = [
    "ConnectParser",
    "ConnectRequest",
    "PipeTimeouts",
    "ProxyError",
    "ProxyProtocolError",
    "RequestTooLargeError",
    "error_response",
    "errors",
    "http",
    "http_proxy",
    "main",
    "parse_connect_target",
    "pipe",
    "pipe_with_timeout",
    "process_as_connect_request",
    "protocol",
    "proxy",
    "start_server",
    "streaming",
    "timeouts",
    "tunnel",
]

# Submodules exposed as attributes of the package.
_SUBMODULES = frozenset(
    {"errors", "http", "protocol", "proxy", "streaming", "timeouts"}
)

# Re-exported name -> submodule that defines it.
_EXPORTS = {
    "ConnectParser": "protocol",
    "ConnectRequest": "protocol",
    "PipeTimeouts": "timeouts",
    "ProxyError": "errors",
    "ProxyProtocolError": "errors",
    "RequestTooLargeError": "errors",
    "error_response": "protocol",
    "http_proxy": "proxy",
    "main": "proxy",
    "parse_connect_target": "protocol",
    "pipe": "streaming",
    "pipe_with_timeout": "streaming",
    "process_as_connect_request": "proxy",
    "start_server": "proxy",
    "tunnel": "proxy",
}


if not TYPE_CHECKING:
    # Hidden from type checkers so a misspelled attribute is still an error there;
    # the TYPE_CHECKING imports above give them the real names.
    def __getattr__(name: str) -> object:
        if name in _SUBMODULES:
            value = import_module(f"{__name__}.{name}")
        elif name in _EXPORTS:
            value = getattr(import_module(f"{__name__}.{_EXPORTS[name]}"), name)
        else:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        globals()[name] = value  # cache: later lookups skip __getattr__
        return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
