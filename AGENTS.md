# Project Guide

This file provides guidance for working with code in this repository.

## What this is

Simple HTTP CONNECT proxy built on Trio (structured-concurrency async I/O), h11 (HTTP parsing), and cashews (caching, circuit breaking, and rate limiting). It only handles `CONNECT` requests (i.e., tunneling HTTPS); plain HTTP requests are not supported.

## Commands

```bash
uv sync                          # install dependencies into .venv
uv run trio_http_proxy           # or: uv run python -m trio_http_proxy
TRIO_HTTP_PROXY_PORT=9090 uv run trio_http_proxy # override the default port (8080)

pytest                           # run test suite
ruff format                      # format
ruff check --fix                 # lint
ty check --fix                   # typecheck
pyright                          # typecheck
```

Before committing, run `pyright` and `pytest`.

To manually verify the proxy works end-to-end:

```bash
uv run trio_http_proxy & sleep 1; curl -x http://127.0.0.1:8080 https://canhazip.com; kill %1
```

## Architecture

Everything lives in `src/trio_http_proxy/`:
- `config/`: Frozen Pydantic `Settings`, loaded in order from explicit init kwargs, `TRIO_HTTP_PROXY_*` environment variables, `.env`, then defaults. Use the cached `get_settings()` at call time (never read config at import time); `reset_settings_cache()` clears it for tests. No module-level config constants: consumers accept `None`-defaulted parameters and resolve settings when called. Duration parsing and validation helpers live here. The default shared connection limiter is `get_conn_limiter()` (memoized; `reset_conn_limiter()` for tests), and the default cache is the lazy `get_default_proxy_cache()` singleton. `CV_PROXY_CACHE` overrides the default cache for a call tree; set it to `None` to disable caching.
- `proxy.py`: Core HTTP CONNECT protocol handling, socket tunneling, bidirectional piping, connection-aware logging, and server lifecycle.
- `proxy_cache.py` + `cache/`: `ProxyCache` bridging `cashews` async operations to Trio via a dedicated background asyncio thread runner, providing destination DNS resolution caching, circuit breaker fail-fast protection for unhealthy upstream hosts, and client rate limiting. `circuit_breaker_manager.py` holds the per-destination breaker registry.
- `__init__.py`: Re-exports public symbols from `.config`, `.proxy`, `.cache`, and `.errors`.

- `start_server` calls `trio.serve_tcp(http_proxy, port)` and manages the `proxy_cache` lifecycle. Each inbound TCP connection spawns `http_proxy` as its own Trio task.
- `http_proxy(client_stream, proxy_cache=...)` is the per-connection entry point: it checks client rate limits, calls `tunnel()` to complete the CONNECT handshake, then runs two `pipe_with_timeout()` tasks concurrently in a nursery — one for each direction of the tunnel — so data flows client→dest and dest→client simultaneously until either side closes or stalls.
- `tunnel(client_stream, proxy_cache=...)` calls `process_as_connect_request()` (parses the CONNECT line via `h11`, in `h11.SERVER` role) to get the destination host/port, probes the destination circuit breaker, optionally resolves/caches destination IP via cashews, opens a TCP stream to it with `trio.open_tcp_stream`, records connection outcomes to the circuit breaker, and replies `200 Connection established` to the client. If circuit breaker is tripped, fast-fails with `502 Bad Gateway`.
- `pipe(from_stream, to_stream)` forwards chunks until EOF, and sends EOF on destination if supported.
- `pipe_with_timeout(from_stream, to_stream, ...)` wraps `pipe()` with idle and send timeouts.
- Logging (`logger`) uses the standard library `logging` module. It is connection-aware via three `ContextVar`s (`CV_CLIENT_STREAM`, `CV_DEST_STREAM`, `CV_PIPE_FROM`) so that concurrent connections' log records are automatically prefixed with `[connN ...]` and enriched with structured attributes (`conn_id`, `desthost`, `direction`, `conn_prefix`). Each `client_stream` gets a sequential `.id` for this purpose (via the `_nextid` counter default arg on `http_proxy`).
- Requires Python 3.14+ (see `pyproject.toml`).

## Configuration reference

The [configuration reference](docs/configuration.md) is the source of truth for supported environment-variable names, defaults, value formats, and runtime behavior. Use `TRIO_HTTP_PROXY_<SETTING>` names, and update the reference when changing settings in `src/trio_http_proxy/config/settings.py`.
