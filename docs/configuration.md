# Nano runtime options and limits

The nano branch has no settings object, environment-variable configuration,
`.env` support, destination policies, or connection admission limiter.

## Listen address

```bash
uv run trio_http_proxy --host 127.0.0.1 --port 8080
```

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Address to bind. Use `::1` for IPv6 loopback. |
| `--port` | `8080` | TCP port. |

The Python entry point is
`start_server(port=8080, *, host="127.0.0.1", task_status=...)`.
It delegates to `trio.serve_tcp`. A port of `0` requests an OS-assigned port;
`nursery.start()` returns the bound listeners.

Old variables such as `TRIO_HTTP_PROXY_PORT`, `PORT`, and
`TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS` have no effect. `ProxyConfig` and
the limiter helpers have been removed. HTTP constants are available as
uppercase names in `trio_http_proxy.http`; the old lowercase aliases are gone.

## Safety limits

These are library defaults, not CLI or environment settings.

| Limit | Default | Python function parameter |
|---|---:|---|
| CONNECT header size | 16384 bytes | `process_as_connect_request(maxreqsz=...)`, `tunnel(maxreqsz=...)` |
| CONNECT read deadline | 10 seconds | `process_as_connect_request(read_timeout=...)`, `tunnel(read_timeout=...)` |
| DNS and TCP connection deadline | 10 seconds | `tunnel(connect_timeout=...)` |
| Initial tunnel data and 200 response writes | 30 seconds total | Fixed |
| Error response write deadline | 5 seconds | Fixed |
| Per-direction idle timeout | 300 seconds | `http_proxy(idle_timeout=...)`, `pipe_with_timeout(idle_timeout=...)` |
| Per-direction send timeout | 30 seconds | `http_proxy(send_timeout=...)`, `pipe_with_timeout(send_timeout=...)` |
| Read chunk size | 8192 bytes | `process_as_connect_request(bufmaxsz=...)`, `pipe(bufmaxsz=...)`, `pipe_with_timeout(bufmaxsz=...)` |

A normal EOF half-closes the opposite stream and lets the other direction
finish. A forwarding error or timeout cancels the sibling pipe. Stream owners
close their sockets when they finish or are cancelled.

## Trust and lifecycle

There is no authentication or SSRF protection. All reachable destinations,
including local and private addresses, are allowed. Keep the loopback default
unless a firewall or another trusted boundary restricts access.

Ctrl-C uses Trio's normal cancellation and cleanup. There is no custom signal
receiver, graceful drain interval, second-signal behavior, or SIGTERM handler.
