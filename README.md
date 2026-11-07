# trio-http-proxy — micro

A small HTTP CONNECT proxy built on [Trio](https://trio.readthedocs.io/) and
[h11](https://h11.readthedocs.io/). Requires Python 3.14 or newer.

This branch keeps CONNECT tunneling and its safety checks. It has no cache,
circuit breaker, rate limiter, connection limiter, destination policy, settings
object, or custom signal handling. Plain HTTP proxy requests are not supported.

## Run

```bash
uv sync
uv run trio_http_proxy                  # 127.0.0.1:8080
uv run trio_http_proxy --port 9090
uv run python -m trio_http_proxy --host ::1 --port 8080
```

Both `trio_http_proxy` and `trio-http-proxy` invoke the same CLI. Only
`--host` and `--port` are configurable from the command line; environment
variables and `.env` files are not read.

In another terminal:

```bash
curl -x http://127.0.0.1:8080 https://canhazip.com
```

**Keep it local or behind trusted network controls.** There is no authentication
or destination filtering. Clients can connect to any address the proxy can
reach, including private networks. Binding to `0.0.0.0` or `::` exposes that
access to other machines.

## What remains

- Incremental CONNECT parsing with authority-form validation, including IPv6.
- A 16 KiB header limit and a 10-second request deadline.
- A 10-second deadline covering DNS resolution and TCP connection.
- Preservation of tunnel bytes sent alongside the CONNECT headers.
- Bidirectional forwarding with half-closes, idle and send timeouts.
- Stream cleanup on success, failure, timeout, and task cancellation.

Trio handles DNS, connection racing, listening sockets, and task lifetimes.
Ctrl-C cancels the server and its active tunnels; there is no drain period or
SIGTERM handler.

Micro builds on `nano`, keeping the same protocol checks and safety limits.
Forwarding uses one `pipe()` with per-operation deadlines instead of callbacks
and a separate timeout wrapper.

See [runtime options and limits](docs/configuration.md) for details.

## Embedding

```python
from functools import partial

import trio
from trio_http_proxy import start_server

trio.run(partial(start_server, 8080, host="127.0.0.1"))
```

Use `await nursery.start(partial(start_server, 0))` to listen on an OS-assigned
port and receive the listener list. Cancelling the owning nursery closes the
server and its connections.

## Development

```bash
ruff check
ruff format --check
uv run --locked pyright
uv run --locked pytest
```

Tests cover protocol properties, memory-stream invariants, handshake failures,
timeouts, cancellation, and live loopback TCP tunnels.
