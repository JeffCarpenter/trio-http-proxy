# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Simple HTTP CONNECT proxy built on Trio (structured-concurrency async I/O), h11 (HTTP parsing), and cashews (caching, circuit breaking, and rate limiting). It only handles `CONNECT` requests (i.e., tunneling HTTPS); plain HTTP requests are not supported.

## Commands

```bash
uv sync                          # install dependencies into .venv
uv run trio_http_proxy           # or: uv run python -m trio_http_proxy
PORT=9090 uv run trio_http_proxy # override the default port (8080)

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
- `config/`: Typed configuration via pydantic-settings. `Settings` is a frozen Pydantic composite with four required, explicit groups: `ProxySettings`, `CacheSettings`, `CircuitBreakerSettings`, and `RateLimitSettings`. It has no flat kwargs or forwarding properties; construct overrides as typed groups and access values through the group (`settings.proxy.port`, `settings.rate_limit.rate_limit`). `load_settings()` loads all groups with the same environment/dotenv sources. Resolution inside each group is explicit group kwargs > `TRIO_HTTP_PROXY_*` env vars > `.env` file > defaults (legacy unprefixed names like `PORT`/`BUFMAXSZ` remain aliases). `get_settings()` caches `load_settings()`; `reset_settings_cache()` clears it. Configuration is never read at import time. Helpers: `parse_duration_seconds`, `normalize_failure_policy`, `validate_namespace`. Parsed-once `*_seconds` properties are exposed as pydantic PrivateAttrs on the group models. There are **no module-level config constants** (no `PORT`, `LOG_LEVEL`, `DEFAULT_*`); consumers take `None`-defaulted params and resolve `get_settings()` at call time. The default shared connection limiter is `get_conn_limiter()` (memoized; `reset_conn_limiter()` for tests), and the default cache is the lazy `get_default_proxy_cache()` singleton. The `CV_PROXY_CACHE` ContextVar overrides the default cache for a whole call tree (set `None` to disable caching).
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

## Configuration (Environment Variables)

All variables may be written with the canonical prefix (`TRIO_HTTP_PROXY_PORT`, ...) or unprefixed legacy names (`PORT`, ...). Duration values accept `30`, `30s`, `5m`, `1h`; `0s` or empty string means **disabled** (falls back or bypasses the feature).

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | Proxy listen port |
| `BUFMAXSZ` | `131072` | Maximum chunk read buffer size (bytes) |
| `MAXREQSZ` | `16384` | Maximum CONNECT request header size (bytes) |
| `CONNECT_READ_TIMEOUT` | `10.0` | Maximum time (seconds) to wait for complete CONNECT request |
| `DEST_CONNECT_TIMEOUT` | `10.0` | Maximum time (seconds) to wait for destination TCP connect |
| `PIPE_IDLE_TIMEOUT` | `300.0` | Inactivity timeout (seconds) closing idle tunnels |
| `PIPE_SEND_TIMEOUT` | `30.0` | Stalled send timeout (seconds) closing blocked writes |
| `MAXCONNS` | `1000` | Connection limiter capacity cap for concurrent tunnels |
| `LOG_LEVEL` | `INFO` | Minimum log level (DEBUG, INFO, WARNING, ERROR) |
| `CACHE_URL` | `mem://` | Cashews cache backend (`mem://`, `disk://...`, `redis://...`) |
| `CACHE_ENABLED` | `true` | Enable or disable cashews caching entirely |
| `CACHE_NAMESPACE` | `trio_http_proxy` | Key namespace for all cache entries |
| `DNS_CACHE_TTL` | `0s` | TTL for destination DNS resolution caching (`0s` = disabled, pass through to trio) |
| `CACHE_FAILURE_POLICY` | `fail-open` | Backend-outage behavior (`fail-open` / `fail-closed`) |
| `PROBE_INTERVAL` | `30.0` | Seconds between backend health probes while degraded |
| `STARTUP_TIMEOUT` | `5.0` | Seconds to wait for cache backend startup ping |
| `CIRCUIT_BREAKER_ENABLED` | `true` | Enable circuit breaking for destination hosts |
| `CIRCUIT_BREAKER_ERRORS_RATE`| `50` | Error percentage threshold to trip circuit |
| `CIRCUIT_BREAKER_PERIOD` | `60s` | Circuit breaker sliding window period |
| `CIRCUIT_BREAKER_TTL` | `30s` | Circuit open cooldown duration |
| `CIRCUIT_BREAKER_MIN_CALLS` | `3` | Minimum connection attempts before circuit can trip |
| `CIRCUIT_BREAKER_MAX_DESTINATIONS` | `10000` | Cap on tracked per-destination breakers |
| `RATE_LIMIT_ENABLED` | `false`| Enable client connection rate limiting |
| `RATE_LIMIT` | `0` | Requests allowed per client IP within `RATE_LIMIT_PERIOD` (0 = unlimited) |
| `RATE_LIMIT_PERIOD` | `60s` | Window for client rate limiting |
| `RATE_LIMIT_TTL` | unset | Ban TTL after exceeding the limit (unset or `0s` = same as `RATE_LIMIT_PERIOD`) |
