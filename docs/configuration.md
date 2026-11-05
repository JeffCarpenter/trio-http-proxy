# Configuration Reference

`trio-http-proxy` reads configuration from `ProxyConfig` instances or environment variables.

## Resolution Precedence

When initializing settings, `ProxyConfig.from_env()` evaluates values in this order:

1. Explicit arguments passed to `ProxyConfig(...)`
2. Canonical prefixed environment variables (`TRIO_HTTP_PROXY_*`)
3. Unprefixed legacy environment variables (`PORT`, `BUFMAXSZ`, etc.)
4. Built-in defaults

## Settings

| Field | Canonical Variable | Legacy Variable | Default | Description |
|---|---|---|---:|---|
| `port` | `TRIO_HTTP_PROXY_PORT` | `PORT` | `8080` | Proxy TCP listen port. |
| `bufmaxsz` | `TRIO_HTTP_PROXY_BUFMAXSZ` | `BUFMAXSZ` | `8192` | Maximum chunk read buffer size in bytes (`io.DEFAULT_BUFFER_SIZE`). |
| `maxreqsz` | `TRIO_HTTP_PROXY_MAXREQSZ` | `MAXREQSZ` | `16384` | Maximum CONNECT request header size in bytes. |
| `connect_read_timeout` | `TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT` | `CONNECT_READ_TIMEOUT` | `10.0` | Timeout in seconds to read the complete client CONNECT request. |
| `dest_connect_timeout` | `TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT` | `DEST_CONNECT_TIMEOUT` | `10.0` | Timeout in seconds to establish the destination TCP connection. |
| `pipe_idle_timeout` | `TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT` | `PIPE_IDLE_TIMEOUT` | `300.0` | Inactivity timeout in seconds closing idle tunnels. |
| `pipe_send_timeout` | `TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT` | `PIPE_SEND_TIMEOUT` | `30.0` | Timeout in seconds closing tunnels blocked on socket writes. |
| `maxconns` | `TRIO_HTTP_PROXY_MAXCONNS` | `MAXCONNS` | `1000` | Maximum concurrent client connections admitted by `trio.CapacityLimiter`. |
| `log_level` | `TRIO_HTTP_PROXY_LOG_LEVEL` | `LOG_LEVEL` | `INFO` | Minimum logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |

## Command-Line Usage

Run the proxy with custom environment variables:

```bash
TRIO_HTTP_PROXY_PORT=9090 TRIO_HTTP_PROXY_MAXCONNS=500 uv run trio_http_proxy
```

Legacy unprefixed variable names also work:

```bash
PORT=9090 MAXCONNS=500 uv run trio_http_proxy
```

## Programmatic Usage

Pass an explicit `ProxyConfig` instance to `start_server()`:

```python
import trio
from trio_http_proxy import ProxyConfig, start_server

config = ProxyConfig(
    port=9090,
    maxconns=500,
    pipe_idle_timeout=120.0,
)

trio.run(start_server, config)
```
