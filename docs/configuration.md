# Configuration Reference

`trio-http-proxy` reads configuration from `ProxyConfig` instances or environment variables.

## Resolution Precedence

When initializing settings, `ProxyConfig.from_env()` evaluates values in this order:

1. Canonical prefixed environment variables (`TRIO_HTTP_PROXY_*`)
2. Legacy aliases listed below, in their listed order
3. Built-in defaults

Empty environment values are skipped. Constructing `ProxyConfig(...)` directly
uses its explicit fields and defaults without reading the environment.

## Settings

| Field | Canonical Variable | Legacy Variable | Default | Description |
|---|---|---|---:|---|
| `port` | `TRIO_HTTP_PROXY_PORT` | `PORT` | `8080` | Proxy TCP listen port. |
| `bufmaxsz` | `TRIO_HTTP_PROXY_BUFMAXSZ` | `BUFMAXSZ` | `8192` | Maximum chunk read buffer size in bytes (`io.DEFAULT_BUFFER_SIZE`). |
| `maxreqsz` | `TRIO_HTTP_PROXY_MAXREQSZ` | `MAXREQSZ` | `16384` | Maximum CONNECT request header size in bytes. |
| `connect_read_timeout` | `TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT` | `CONNECT_READ_TIMEOUT` | `10.0` | Timeout in seconds to read the complete client CONNECT request. |
| `dest_connect_timeout` | `TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT` | `DEST_CONNECT_TIMEOUT` | `10.0` | Total timeout in seconds for DNS resolution and destination TCP connection attempts. |
| `pipe_idle_timeout` | `TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT` | `PIPE_IDLE_TIMEOUT` | `300.0` | Inactivity timeout in seconds closing idle tunnels. |
| `pipe_send_timeout` | `TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT` | `PIPE_SEND_TIMEOUT` | `30.0` | Timeout in seconds closing tunnels blocked on socket writes. |
| `maxconns` | `TRIO_HTTP_PROXY_MAXCONNS` | `MAXCONNS` | `1000` | Maximum concurrent client connections admitted by `trio.CapacityLimiter`. |
| `shutdown_timeout` | `TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT` | `PROXY_SHUTDOWN_TIMEOUT`, `SHUTDOWN_TIMEOUT` | `15.0` | Grace period in seconds allowing active connections to drain on SIGTERM/SIGINT before forced cancellation. Accepts a floating-point number; `0` skips the grace period. |
| `log_level` | `TRIO_HTTP_PROXY_LOG_LEVEL` | `LOG_LEVEL` | `INFO` | Minimum logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `allow_private_networks` | `TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS` | `PROXY_ALLOW_PRIVATE_NETWORKS` | `false` | Opt out of the public-address guard. Does not override allowlists or denylists. |
| `allowed_targets` | `TRIO_HTTP_PROXY_ALLOWED_TARGETS` | `PROXY_ALLOWED_TARGETS` | empty | Comma-separated exact hosts or `host:port` entries; empty permits all targets that pass the other checks. |
| `denied_targets` | `TRIO_HTTP_PROXY_DENIED_TARGETS` | `PROXY_DENIED_TARGETS` | empty | Comma-separated exact hosts or `host:port` entries to block. Takes precedence over the allowlist. |

## Graceful Shutdown

`shutdown_timeout` defaults to `15.0` seconds. Environment names are checked in
order: `TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT`, `PROXY_SHUTDOWN_TIMEOUT`, then
`SHUTDOWN_TIMEOUT`. Empty values are skipped; values must be floating-point
numbers. Set `0` to skip the grace period.

With signal handling enabled (the default), the first `SIGTERM` or `SIGINT` closes
listening sockets and stops accepting connections. Existing connections can
continue transferring data for up to `shutdown_timeout` seconds. When that grace
period expires, or a second signal arrives, the server cancels remaining handlers
and waits for their cleanup. The timeout bounds the grace period, not cleanup time.

Cancellation from the calling Trio task cancels the server and its handlers
without waiting for the grace period. Listener cleanup also runs on startup or
runtime failures.

Pass `enable_signals=False` to `start_server()` when the caller manages its
lifecycle. If signal receiver setup is unavailable (for example, outside the main
thread), the server falls back to this no-signal mode. Errors during serving or
receiver teardown propagate; they do not trigger fallback.

## Destination Policy and SSRF Protection

The proxy rejects non-public destination addresses by default, including
loopback, private, link-local, shared carrier-grade NAT, unspecified, reserved,
and multicast addresses. It also blocks `localhost` and its subdomains. Policy
rejections return `403 Forbidden` before any connection to the denied target.

For hostnames, the default connector resolves DNS within `dest_connect_timeout`
and checks every returned IPv4/IPv6 address. It rejects the entire target if any
answer is restricted, even when another answer is public. It then connects only
to vetted numeric addresses, avoiding a second hostname lookup and DNS rebinding.
Attempts start 250 ms apart, or immediately after the previous attempt fails, so
an unreachable IPv6 address does not starve working IPv4 addresses.

Host entries match every port; port-qualified entries match only that port. Use
`[IPv6]:port` for IPv6 entries with ports. Matching ignores case, DNS trailing
dots, IPv6 spelling differences, and scope IDs. IPv4-mapped IPv6 and well-known
NAT64 (`64:ff9b::/96`) aliases share their embedded IPv4 policy identity. Embedded
IPv4 addresses in 6to4 and Teredo destinations must also pass address policy.
Entries are exact: wildcards, CIDRs, and hostname suffix matching are unsupported.
Hostname allowlist entries apply to the requested hostname; resolved addresses
still undergo the public-address guard and IP denylist checks.

For example, allow only two HTTPS destinations:

```bash
TRIO_HTTP_PROXY_ALLOWED_TARGETS=example.com:443,api.example.com:8443 uv run trio_http_proxy
```

To tunnel to local or private services, explicitly set
`TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS=true` or construct
`ProxyConfig(allow_private_networks=True)`. This restores private-network access
and should be used only with trusted clients or a restrictive allowlist. The
boolean parser accepts `1`, `true`, `yes`, and `on` (case-insensitive, with
surrounding whitespace ignored) as true; other nonempty values mean false. Lists
trim whitespace and ignore empty comma-separated entries. Programmatic lists use
immutable tuples, such as `allowed_targets=("example.com:443",)`.

This guard does not authenticate clients or replace an egress firewall. It cannot
inspect what a public upstream forwards to, arbitrary network-specific address
translation, or DNS queries themselves. Hostname denylists do not block every
alias for that hostname; use IP entries or network controls for that purpose.
Custom `_connect` callables passed to `tunnel()` or `http_proxy()` are trusted
transport overrides: they retain requested-target checks but must enforce their
own DNS/address safety.

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
