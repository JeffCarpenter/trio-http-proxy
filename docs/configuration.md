# Configuration reference

`trio-http-proxy` reads settings from environment variables and an optional
`.env` file in the current working directory. The precedence is:

1. Explicit `Settings(...)` keyword arguments
2. Environment variables
3. `.env` values
4. Defaults

Environment variable names use the `TRIO_HTTP_PROXY_` prefix followed by the
setting name in uppercase (for example, `TRIO_HTTP_PROXY_PORT`). Names are
case-insensitive. Use the prefixed names: unprefixed names such as `PORT` are
not loaded by the current settings model.

## Settings

| Setting suffix | Environment variable | Default | Description |
|---|---|---:|---|
| `PORT` | `TRIO_HTTP_PROXY_PORT` | `8080` | Proxy listen port. |
| `BUFMAXSZ` | `TRIO_HTTP_PROXY_BUFMAXSZ` | `131072` | Maximum bytes read into each proxy data chunk. |
| `MAXREQSZ` | `TRIO_HTTP_PROXY_MAXREQSZ` | `16384` | Maximum CONNECT request-header size in bytes. The limit includes the complete header terminator; bytes after it do not count. |
| `CONNECT_READ_TIMEOUT` | `TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT` | `10.0` | Maximum seconds to read a complete CONNECT request. |
| `DEST_CONNECT_TIMEOUT` | `TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT` | `10.0` | Maximum seconds to connect to the destination. |
| `PIPE_IDLE_TIMEOUT` | `TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT` | `300.0` | Maximum tunnel inactivity in seconds. |
| `PIPE_SEND_TIMEOUT` | `TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT` | `30.0` | Maximum seconds to wait for a stalled write. |
| `MAXCONNS` | `TRIO_HTTP_PROXY_MAXCONNS` | `1000` | Maximum concurrent accepted proxy connections. Additional clients wait before being accepted. |
| `LOG_LEVEL` | `TRIO_HTTP_PROXY_LOG_LEVEL` | `INFO` | Minimum logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, and standard Python levels). |
| `CACHE_URL` | `TRIO_HTTP_PROXY_CACHE_URL` | `mem://` | Cashews backend URL, such as `mem://`, `disk://...`, or `redis://...`. |
| `CACHE_ENABLED` | `TRIO_HTTP_PROXY_CACHE_ENABLED` | `true` | Enable or disable cache backend features. |
| `CACHE_NAMESPACE` | `TRIO_HTTP_PROXY_CACHE_NAMESPACE` | `trio_http_proxy` | Namespace prefix for cache entries. |
| `DNS_CACHE_TTL` | `TRIO_HTTP_PROXY_DNS_CACHE_TTL` | `0s` | Destination DNS cache lifetime. `0s` disables DNS caching and uses Trio's resolver directly. |
| `CACHE_FAILURE_POLICY` | `TRIO_HTTP_PROXY_CACHE_FAILURE_POLICY` | `fail-open` | Backend outage policy: `fail-open` continues without cache features; `fail-closed` returns `503 Service Unavailable` for affected requests. |
| `PROBE_INTERVAL` | `TRIO_HTTP_PROXY_PROBE_INTERVAL` | `30.0` | Seconds between cache-backend health probes while degraded. |
| `STARTUP_TIMEOUT` | `TRIO_HTTP_PROXY_STARTUP_TIMEOUT` | `5.0` | Seconds to wait for cache bridge startup and startup health checks. |
| `CIRCUIT_BREAKER_ENABLED` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_ENABLED` | `true` | Enable circuit breaking for destination hosts. |
| `CIRCUIT_BREAKER_ERRORS_RATE` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_ERRORS_RATE` | `50` | Error percentage threshold for opening a destination circuit (1–99). |
| `CIRCUIT_BREAKER_PERIOD` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_PERIOD` | `60s` | Sliding-window duration for destination connection outcomes. |
| `CIRCUIT_BREAKER_TTL` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_TTL` | `30s` | Duration a circuit stays open before a probe can be attempted. |
| `CIRCUIT_BREAKER_MIN_CALLS` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_MIN_CALLS` | `3` | Minimum connection attempts in the window before a circuit can open. |
| `CIRCUIT_BREAKER_MAX_DESTINATIONS` | `TRIO_HTTP_PROXY_CIRCUIT_BREAKER_MAX_DESTINATIONS` | `10000` | Maximum number of destination breakers tracked at once. |
| `RATE_LIMIT_ENABLED` | `TRIO_HTTP_PROXY_RATE_LIMIT_ENABLED` | `false` | Enable per-client-IP connection rate limiting. |
| `RATE_LIMIT` | `TRIO_HTTP_PROXY_RATE_LIMIT` | `0` | Requests allowed per client IP in `RATE_LIMIT_PERIOD`; `0` means unlimited. |
| `RATE_LIMIT_PERIOD` | `TRIO_HTTP_PROXY_RATE_LIMIT_PERIOD` | `60s` | Rate-limit window duration. |
| `RATE_LIMIT_TTL` | `TRIO_HTTP_PROXY_RATE_LIMIT_TTL` | unset | Ban duration after a client exceeds the limit. When unset, empty, or `0s`, the period defaults to `RATE_LIMIT_PERIOD`. |

## Value formats and behavior

- Boolean settings accept normal Pydantic boolean values, including `true` and
  `false`.
- Timeout, probe, and startup values with numeric defaults are measured in
  seconds and must be positive.
- Duration-string settings accept integer durations with `s`, `m`, `h`, or `d`
  suffixes, such as `30s`, `5m`, `1h`, or `2d`. Durations must be positive,
  except `DNS_CACHE_TTL` and `RATE_LIMIT_TTL`, which allow zero/empty values as
  described above.
- `CACHE_FAILURE_POLICY` accepts `fail-open` or `fail-closed` (case-insensitive).
- When rate limiting rejects a request, `Retry-After` reflects the live
  remaining limit expiry when the backend provides it, with a minimum safe value
  of one second.

For example, run the proxy on port 9090 with:

```bash
TRIO_HTTP_PROXY_PORT=9090 uv run trio_http_proxy
```

Settings are defined in `src/trio_http_proxy/config/settings.py`; keep this
reference synchronized when adding or changing a setting.
