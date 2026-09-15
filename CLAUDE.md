# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Simple HTTP CONNECT proxy built on Trio (structured-concurrency async I/O) and h11 (HTTP parsing). The entire implementation is one file: `src/trio_http_proxy/proxy.py`. It only handles `CONNECT` requests (i.e., tunneling HTTPS); plain HTTP requests are not supported.

## Commands

```bash
uv sync                          # install dependencies into .venv
uv run trio_http_proxy           # or: uv run python -m trio_http_proxy
PORT=9090 uv run trio_http_proxy # override the default port (8080)

ruff format                      # format
ruff check --fix                 # lint
ty check --fix                   # typecheck
```

Before committing, run `pyright`

No test suite exists. To manually verify the proxy works end-to-end:

```bash
uv run trio_http_proxy & sleep 1; curl -x http://127.0.0.1:8080 https://canhazip.com; kill %1
```

## Architecture

Everything lives in `proxy.py`; `__init__.py` just re-exports it (`from .proxy import *`).

- `start_server` calls `trio.serve_tcp(http_proxy, port)`. Each inbound TCP connection spawns `http_proxy` as its own Trio task.
- `http_proxy(client_stream)` is the per-connection entry point: it calls `tunnel()` to complete the CONNECT handshake, then runs two `pipe()` tasks concurrently in a nursery — one for each direction of the tunnel — so data flows client→dest and dest→client simultaneously until either side closes.
- `tunnel()` calls `process_as_connect_request()` (parses the CONNECT line via `h11`, in `h11.SERVER` role) to get the destination host/port, opens a TCP stream to it with `trio.open_tcp_stream`, and replies `200 Connection established` to the client. From this point the connection is a raw byte tunnel — no further HTTP parsing occurs.
- `pipe(from_stream, to_stream)` just loops `async for chunk in from_stream: await to_stream.send_all(chunk)`.
- Logging (`logger`) uses the standard library `logging` module. It is connection-aware via three `ContextVar`s (`CV_CLIENT_STREAM`, `CV_DEST_STREAM`, `CV_PIPE_FROM`) so that concurrent connections' log records are automatically prefixed with `[connN ...]` and enriched with structured attributes (`conn_id`, `desthost`, `direction`, `conn_prefix`). Each `client_stream` gets a sequential `.id` for this purpose (via the `_nextid` counter default arg on `http_proxy`).
- Requires Python 3.14+ (see `pyproject.toml`), primarily for the walrus operator usage and modern async idioms already in use — check `requires-python` before assuming older syntax is off-limits.

## Notes

- `requirements.in` / `requirements-lock.txt` are legacy pip-tools artifacts from before the move to uv (see `pyproject.toml` / `uv.lock`); prefer `uv add` for dependency changes.
- `.pylintrc` disables `line-too-long` and `missing-docstring` — the codebase intentionally omits docstrings/line-length enforcement in favor of ruff.
