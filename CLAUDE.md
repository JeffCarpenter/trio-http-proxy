# Project Guide

This file provides guidance for working with code in this repository.

## What this is

`trio-http-proxy` is a minimal HTTP CONNECT proxy built strictly on Trio (structured-concurrency async I/O) and h11 (HTTP parsing). It handles only HTTP `CONNECT` requests for tunneling HTTPS traffic. It does not handle plain HTTP requests.

The minimal proxy contains no background threads, no Cashews cache, no circuit breaker, and no Pydantic settings.

## Commands

```bash
uv sync                          # install dependencies into .venv
uv run trio_http_proxy           # run the proxy on port 8080
TRIO_HTTP_PROXY_PORT=9090 uv run trio_http_proxy # override port

pytest                           # run test suite
ruff format                      # format code
ruff check --fix                 # lint code
ty check --fix                   # typecheck with ty
pyright                          # typecheck with pyright
```

Before committing, run `pyright` and `pytest`.

To verify the proxy end-to-end:

```bash
uv run trio_http_proxy & sleep 1; curl -x http://127.0.0.1:8080 https://canhazip.com; kill %1
```

## Architecture

All library code lives in `src/trio_http_proxy/`:

- `config.py`: Immutable dataclass `ProxyConfig` and `ProxyConfig.from_env()`. Loads configuration from canonical `TRIO_HTTP_PROXY_*` variables, legacy aliases (`PORT`, `BUFMAXSZ`, etc.), or keyword arguments.
- `errors.py`: Exception hierarchy: `ProxyError`, `ProxyProtocolError`, and `RequestTooLargeError`.
- `http.py`: Wire-format HTTP byte constants (`200`, `400`, `408`, `414`, `502`, `504`).
- `protocol.py`: RFC 7231 §4.3.6 authority target parser (`parse_connect_target`) and incremental `h11` stream consumer (`process_as_connect_request`).
- `streaming.py`: Bidirectional `pipe()` with `send_eof()` half-close support, and `pipe_with_timeout()` wrapping cancel scopes that update deadlines during I/O.
- `proxy.py`: Core lifecycle: `start_server()` manages the TCP listener; `http_proxy()` serves each client connection; `tunnel()` connects to the upstream destination.
- `__init__.py`: Public exports.
- `__main__.py`: CLI entry point.
- `py.typed`: PEP 561 type marker.

## Concurrency Invariants

The proxy enforces strict structured concurrency:

1. **Bounded Lifetimes:** Every task runs inside a nursery. When a function returns, all child tasks have finished. No background threads or orphan tasks exist.
2. **Half-Close Support:** Sockets support full duplex. When one direction finishes reading, `pipe()` calls `await to_stream.send_eof()`. The other direction keeps transferring data until reaching its own EOF.
3. **Timeouts and Cancellation:** Cancel scopes wrap all socket operations. When one direction times out or fails, the nursery cancels the opposite direction immediately.
4. **Guaranteed Cleanup:** Socket streams close inside `try...finally: await stream.aclose()` blocks. No file descriptors leak on normal completion, errors, or cancellation.
5. **Backpressure:** A shared `trio.CapacityLimiter` limits concurrent accepted connections.

## Configuration Reference

See [docs/configuration.md](docs/configuration.md) for the complete list of settings, environment variables, defaults, and usage examples.
