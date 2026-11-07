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

- `protocol.py`: Pure authority-form target validation and bounded incremental
  h11 request parsing. Returns host, port, and any early tunnel bytes.
- `proxy.py`: `tunnel()` performs the handshake; `http_proxy()` owns the client
  and upstream streams and runs both pipes in a nursery. `start_server()`
  delegates to `trio.serve_tcp`; `main()` parses only host and port.
- `streaming.py`: `pipe()` forwards bytes and propagates half-close;
  `pipe_with_timeout()` updates idle/send deadlines on progress.
- `http.py`: Uppercase wire-format response constants.
- `errors.py`: Proxy exception hierarchy.
- `__init__.py`: Public function and exception exports.
- `__main__.py`: Module entry point.

Keep all tasks inside nurseries. Preserve early tunnel bytes, half-close
semantics, bounded handshakes, sibling cancellation on pipe failure, and
cleanup on cancellation. A successful `tunnel()` transfers upstream ownership
to its caller; a failed handshake must close the upstream itself.

## Runtime reference

[docs/configuration.md](docs/configuration.md) documents CLI options and safety
limits. Update it when changing defaults or behavior. Environment variables and
`.env` files are not read.
