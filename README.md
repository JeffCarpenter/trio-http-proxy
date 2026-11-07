# trio-http-proxy

A minimal HTTP CONNECT proxy built strictly on Trio and h11.

It tunnels HTTPS traffic with structured concurrency. It does not handle plain HTTP requests.

## Features

- **Structured Concurrency:** Every task runs inside a nursery. No background threads, leaked tasks, or dangling sockets.
- **RFC 7231 Compliance:** Parses authority-form targets (`host:port`), including bracketed IPv6 literals.
- **Half-Close Support:** Sockets send EOF when one direction finishes writing while continuing to receive data.
- **Cooperative Cancellation:** Idle and send timeouts nudge cancel scopes during data transfer. When one direction fails or times out, the nursery cancels the other immediately.
- **Backpressure:** A shared `trio.CapacityLimiter` bounds total concurrent connections.
- **Graceful Shutdown:** SIGTERM or SIGINT stops accepting connections and allows existing tunnels to drain. A second signal forces cancellation.
- **Destination Policy:** Exact host/port allowlists and denylists, with public-address checks on IP literals and DNS answers to guard against SSRF and DNS rebinding.
- **Minimal Dependencies:** Built solely on `trio` and `h11`. No background worker threads, no cache backends, and no complex configuration frameworks.

## Requirements

Python 3.14 or newer.

## Installation

```bash
uv sync
```

Or install with pip:

```bash
pip install .
```

## Running the Proxy

Start the proxy on the default port (8080):

```bash
uv run trio_http_proxy
```

Override the listen port:

```bash
TRIO_HTTP_PROXY_PORT=9090 uv run trio_http_proxy
```

Legacy environment variables also work:

```bash
PORT=9090 uv run trio_http_proxy
```

Private and loopback destinations are blocked by default. For trusted local use,
set `TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS=true`. See the
[configuration reference](docs/configuration.md#destination-policy-and-ssrf-protection)
for allowlist syntax, DNS checks, and security limits. Restrict access to trusted
clients; this proxy does not provide authentication.

## Testing the Proxy

Make an HTTPS request through the proxy:

```bash
curl -x http://127.0.0.1:8080 https://canhazip.com
```

Run a one-liner smoke test in a single shell session:

```bash
uv run trio_http_proxy & sleep 1; curl -x http://127.0.0.1:8080 https://canhazip.com; kill %1
```

## Programmatic Usage

Embed the proxy into a Trio application:

```python
import trio
from trio_http_proxy import ProxyConfig, start_server


async def main() -> None:
    config = ProxyConfig(port=8080, maxconns=500)
    await start_server(config)


trio.run(main)
```

## Configuration

Settings load from `ProxyConfig` fields or environment variables. Canonical prefixed variables take precedence over legacy aliases.

| Setting | Canonical Variable | Legacy Variable | Default | Description |
|---|---|---|---:|---|
| `port` | `TRIO_HTTP_PROXY_PORT` | `PORT` | `8080` | Proxy TCP listen port. |
| `bufmaxsz` | `TRIO_HTTP_PROXY_BUFMAXSZ` | `BUFMAXSZ` | `8192` | Chunk read buffer size in bytes (`io.DEFAULT_BUFFER_SIZE`). |
| `maxreqsz` | `TRIO_HTTP_PROXY_MAXREQSZ` | `MAXREQSZ` | `16384` | Maximum CONNECT request header size in bytes. |
| `connect_read_timeout` | `TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT` | `CONNECT_READ_TIMEOUT` | `10.0` | Timeout in seconds to read the client CONNECT request. |
| `dest_connect_timeout` | `TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT` | `DEST_CONNECT_TIMEOUT` | `10.0` | Total timeout in seconds for DNS and destination connection attempts. |
| `pipe_idle_timeout` | `TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT` | `PIPE_IDLE_TIMEOUT` | `300.0` | Inactivity timeout in seconds closing idle tunnels. |
| `pipe_send_timeout` | `TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT` | `PIPE_SEND_TIMEOUT` | `30.0` | Timeout in seconds closing tunnels blocked on socket writes. |
| `shutdown_timeout` | `TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT` | `PROXY_SHUTDOWN_TIMEOUT`, `SHUTDOWN_TIMEOUT` | `15.0` | Grace period in seconds for tunnels to drain after SIGTERM/SIGINT. |
| `allow_private_networks` | `TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS` | `PROXY_ALLOW_PRIVATE_NETWORKS` | `false` | Permit non-public destinations; allowlists and denylists still apply. |
| `allowed_targets` | `TRIO_HTTP_PROXY_ALLOWED_TARGETS` | `PROXY_ALLOWED_TARGETS` | empty | Comma-separated exact hosts or `host:port` entries to allow. |
| `denied_targets` | `TRIO_HTTP_PROXY_DENIED_TARGETS` | `PROXY_DENIED_TARGETS` | empty | Comma-separated exact hosts or `host:port` entries to deny. |
| `maxconns` | `TRIO_HTTP_PROXY_MAXCONNS` | `MAXCONNS` | `1000` | Maximum concurrent client connections admitted. |
| `log_level` | `TRIO_HTTP_PROXY_LOG_LEVEL` | `LOG_LEVEL` | `INFO` | Minimum logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

See [docs/configuration.md](docs/configuration.md) for full details.

## Development

Run tests and code checks:

```bash
pytest                 # run test suite
ruff check             # lint
ruff format --check    # check formatting
pyright                # typecheck
```

## License

See [LICENSE](LICENSE) for details.
