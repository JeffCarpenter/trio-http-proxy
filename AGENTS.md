# Project Guide

## Scope

The `nano` branch is a small HTTP CONNECT proxy built on Trio and h11.
It requires Python 3.14+ and supports CONNECT tunneling only, not plain HTTP
proxy requests. Do not restore removed optional layers without an explicit
request.

There is no cache, circuit breaker, rate limiter, connection limiter,
destination policy, settings object, or custom signal handling. It binds
to `127.0.0.1:8080` by default. It has no authentication or SSRF protection.

## Commands

```bash
uv sync
uv run trio_http_proxy
uv run trio_http_proxy --port 9090
ruff format
ruff check
uv run --locked pyright
uv run --locked pytest
```

Run Pyright and pytest before committing. `just ci` runs lint, Pyright and
pytest. The local interpreter is `.venv/bin/python`.

## Architecture

Source lives in `src/trio_http_proxy/`:

- `protocol.py`: Sans-io core, no trio import. `ConnectParser` does bounded
  incremental h11 parsing (feed bytes, pull a `ConnectRequest` of host, port
  and early tunnel bytes); also target validation and `error_response()`.
- `timeouts.py`: Sans-io `PipeTimeouts` idle/send deadline policy; the caller
  supplies the clock. Keep `protocol.py`, `timeouts.py`, `errors.py` and
  `http.py` free of trio imports. `tests/test_sansio.py` checks this with a
  static import scan and a subprocess import with trio blocked; it does not
  catch an import hidden inside a function body.
- `proxy.py`: `process_as_connect_request()` drives `ConnectParser` over a trio
  stream; `tunnel()` performs the handshake; `http_proxy()` owns the client
  and upstream streams and runs both pipes in a nursery. `start_server()`
  delegates to `trio.serve_tcp`; `main()` parses only host and port.
- `streaming.py`: `pipe()` forwards bytes and propagates half-close;
  `pipe_with_timeout()` applies `PipeTimeouts` deadlines to a cancel scope.
- `http.py`: Uppercase wire-format response constants.
- `errors.py`: Proxy exception hierarchy.
- `__init__.py`: Lazy (PEP 562) public exports; importing the package must not
  load trio. A new public name needs the `TYPE_CHECKING` import, `__all__`
  and `_EXPORTS` (or `_SUBMODULES`) entries.
- `__main__.py`: Module entry point.

Keep all tasks inside nurseries. Preserve early tunnel bytes, half-close
semantics, bounded handshakes, sibling cancellation on pipe failure, and
cleanup on cancellation. A successful `tunnel()` transfers upstream ownership
to its caller; a failed handshake must close the upstream itself.

## Runtime reference

[docs/configuration.md](docs/configuration.md) documents CLI options and safety
limits. Update it when changing defaults or behavior. Environment variables and
`.env` files are not read.
