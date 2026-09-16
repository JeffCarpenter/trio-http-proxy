# Findings: code review of proxy.py and cache.py

Snapshot as of commit `aa00aa4` ("Integrate cashews caching, circuit breaker, rate
limiting, and DNS cache"), test suite green at 78 passed. Tier 1 items were reproduced
in-session and each quotes its actual output; Tier 2 items are read off the source and
were not run; Tier 3 is nits. No fixes applied — fix locations are named, not written.

## Tier 1 — Confirmed (reproduced in this session)

### C1. Pipelined post-CONNECT bytes are silently dropped (proxy.py:409-421)
h11 buffers everything received. Clients that send the TLS ClientHello in the same
segment as the CONNECT line have those bytes sitting in `h11_conn.trailing_data`,
which `process_as_connect_request` discards. Reproduced:

    event: Request(method=b'CONNECT', target=b'example.com:443', ...)
    trailing_data: (b'\x16\x03\x01EARLYTLS', False)

Effect: handshake stalls until PIPE_IDLE_TIMEOUT (300s), then 504-less close.
Fix location: `process_as_connect_request` must return `h11_conn.trailing_data[0]`
and `tunnel` must `send_all` it to dest before returning the stream.

### C2. Any exception escaping `http_proxy` kills the whole server
`trio.serve_tcp` propagates handler exceptions into the serve nursery. Proven:

    SERVER DIED: ExceptionGroup(... [BrokenResourceError('simulated ...')])

Two live escape paths:
- proxy.py:214-226 — `get_client_key`, `check_rate_limit`, and `send_all(429)` all
  run *outside* any try. A client that RSTs after CONNECT, or a bridge RuntimeError,
  takes the process down.
- proxy.py:231-237 — the catch tuple is `(ValueError, h11.ProtocolError, OSError,
  trio.TooSlowError, CircuitBreakerOpen)`. `trio.BrokenResourceError` /
  `ClosedResourceError` subclass `Exception`, not `OSError`, and every error path in
  `tunnel()` ends in `await client_stream.send_all(...)` to a client that may be gone.
  Reproduced: `ESCAPED: BrokenResourceError client gone`.
Fix location: wrap the whole `http_proxy` body (not just the nursery) in the
`except*` guard, or pass a `handler_nursery` to `serve_tcp`.

### C3. Circuit breaker threshold is effectively doubled (cache.py:228-289)
`check_circuit()` calls the decorated `_circuit_probe(host, port, None)`, which
*succeeds*. cashews (`decorators/circuit_breaker.py`) increments `:total` on every
call and `:fails` only on exception. So each real request produces 2 totals (probe +
record) and at most 1 fail. Trip condition `fails*100/total >= errors_rate` therefore
sees at most 50%.
- `CIRCUIT_BREAKER_ERRORS_RATE=50` (the default) trips only at 100% real failure.
- Any value > 50 can never trip.
- `min_calls=3` is reached after 1.5 real requests.
Measured (10 consecutive 100%-failing requests per rate, `min_calls=1`):

    errors_rate=50  -> tripped_at=2
    errors_rate=60  -> tripped_at=None
    errors_rate=80  -> tripped_at=None
    errors_rate=99  -> tripped_at=None

Fix location: `check_circuit` must read breaker state without registering a call
(inspect the `:open` lock key), not invoke the decorated function.

### C4. 504 is never sent when the circuit breaker is enabled (proxy.py:369-378, cache.py:291-301)
`record_connection_result` passes the exception into `_circuit_probe`, which re-raises
it; cashews re-raises after recording; the object crosses the bridge thread intact.
`except CircuitBreakerOpen, OSError, RuntimeError` does not match `trio.TooSlowError`,
so it propagates out of `record_connection_result` — before
`send_all(HTTP_504_GATEWAY_TIMEOUT)` on the next line. Reproduced with a real clock:

    RESULT tunnel raised: TooSlowError simulated dest connect timeout
    RESULT client received: None                       # circuit breaker enabled

Control, same request with `proxy_cache=None`:

    RESULT control (proxy_cache=None) client received: bytearray(b'HTTP/1.1 504 Gateway Timeout\r\n\r\n')

Fix location: `record_connection_result` must never propagate — catch `Exception`, or
better, do not use the re-raise-through-decorator trick to record results.

### C5. `except KeyboardInterrupt` in `start_server` is dead code (proxy.py:287)
`serve_listeners` opens an internal nursery, so KI arrives as a group. Run against the
real shape — `start_server(handler, PORT, trio.serve_tcp, None)` with a handler that
raises KI on a live connection:

    RESULT escaped: BaseExceptionGroup('Exceptions from Trio nursery',
                      [BaseExceptionGroup('Exceptions from Trio nursery', [KeyboardInterrupt(...)])])

"Goodbye for now." never prints. `finally` still runs. Fix: `except* KeyboardInterrupt`.

### C6. Both documented run commands fail
    $ python -m trio_http_proxy
    No module named trio_http_proxy.__main__; 'trio_http_proxy' is a package and cannot be directly executed
    $ uv run trio_http_proxy
    error: Failed to spawn: `trio_http_proxy`
No `__main__.py`, no `[project.scripts]`. `python src/trio_http_proxy/proxy.py` does
work (the fallback import at proxy.py:30 resolves against the installed package), but
that is not what CLAUDE.md documents.

## Tier 2 — Reasoned from source (not reproduced)

### R1. No half-close: `pipe` never calls `send_eof` (proxy.py:474-484)
Client sends FIN → client→dest pipe returns → dest never learns → the dest→client pipe
blocks for the full 300s idle timeout while both fds and a CONN_LIMITER slot stay held.
Client-closes-first is the common case for HTTP-over-CONNECT. With MAXCONNS=1000 this
fills with zombies. Fix location: `pipe` should `await to_stream.send_eof()` on EOF.

### R2. `ProxyCache.run()` burns an uncancellable worker thread per cache op (cache.py:186-197)
`trio.to_thread.run_sync` without `abandon_on_cancel` is uncancellable and draws on the
default 40-thread limiter. With rate limiting enabled, every connection takes a slot and
holds it for up to `timeout=10.0`. MAXCONNS=1000 is fiction during the handshake phase,
and shutdown blocks up to 10s per in-flight op. The bridge loop could be driven from
Trio via `trio_asyncio` (already a dependency, currently unused) instead.

### R3. `stop()` / `run_coroutine` race on `self._loop` (cache.py:143-184)
`stop()` holds `_lock`; `run_coroutine` does not. An in-flight task can read `self._loop`
mid-teardown (RuntimeError, or a future that never resolves → 10s stall), and
`_ensure_started` will happily *restart* the bridge thread after shutdown.

### R4. DNS cache defeats happy-eyeballs (cache.py:205-219, 262-279)
`_resolve_dns` returns `addr_infos[0][4][0]` — one address, no family preference, no
fallback. Handing a single literal to `trio.open_tcp_stream` disables its parallel
v4/v6 attempts, so a host whose first record is an unreachable AAAA becomes hard-down
for the TTL. Bounded: off by default (`DNS_CACHE_TTL=0s`).

### R5. `_is_mock_clock()` makes production behavior test-conditional (cache.py:167-172, 269/287/296/309)
Every cache entry point no-ops under an autojump clock. test_proxy.py uses exactly that
clock, so the whole default-cache path is unexercised there — which is why C4 survives a
78-test green suite. The mechanism belongs in test fixtures (pass `proxy_cache=None`),
not in the library.

### R6. `CONN_LIMITER` comment overclaims (proxy.py:70-76)
`async with client_stream, limiter` acquires the limiter *after* accept and *inside* the
stream context, with no acquisition timeout. Over-cap connections park in the wait queue
holding an open fd. The cap bounds concurrent tunnels; it does not bound file
descriptors, which is what the comment says it does.

## Tier 3 — Nits
- cache.py:183-184 — on `fut.result` timeout the asyncio task keeps running; `fut.cancel()` never called.
- cache.py:181-182 — the `RuntimeError` is raised after `coro` was constructed: "coroutine was never awaited" leak.
- Two different `_int_env`: proxy.py:40 is strict (`PORT=abc` → ValueError at import, before logging is configured), cache.py:18 is lenient (silently falls back). Pick one. `BUFMAXSZ=0` passes validation here and then fails per-connection inside trio
(`ValueError: max_bytes must be >= 1`) rather than at startup.
- cache.py:207 `assert self._loop is not None` contradicts `ProxyProtocolError`'s own docstring rationale for avoiding asserts under `python -O`.
- proxy.py:383-384 — `CV_DESTHOST` is set only on success, so every error-path log line loses the host in its prefix.
- proxy.py:349, 362 — `logger.exception` (ERROR + traceback) for client-controlled DNS misses and refused connections: a client can flood logs with random hostnames. `warning` without traceback fits.
- proxy.py:22-37 — `try: from .cache / except ImportError: from trio_http_proxy.cache` masks a genuine ImportError inside cache.py behind a confusing second failure.
- `__init__.py` does `from .proxy import *` and proxy.py has no `__all__`, so `trio_http_proxy.h11`, `.trio`, `.logging`, `.partial`, `.urlsplit`, `.getenv` are all public API.
- `tunnel()` docstring says it returns "the destination stream and the corresponding host"; it returns the stream only.
- CLAUDE.md env table omits BUFMAXSZ, MAXREQSZ, CONNECT_READ_TIMEOUT, DEST_CONNECT_TIMEOUT, PIPE_IDLE_TIMEOUT, PIPE_SEND_TIMEOUT, MAXCONNS, LOG_LEVEL, RATE_LIMIT_TTL; and the architecture section says `http_proxy` runs two `pipe()` tasks when it runs `pipe_with_timeout`.
- `HTTP_429` hardcodes `Retry-After: 60` regardless of `RATE_LIMIT_PERIOD`.
- Error responses carry no `Connection: close` / `Content-Length: 0`.
- `get_client_key` keys on the peer IP, which is the load balancer's address behind one.

## Deployment posture (not a bug)
Open relay: no proxy auth, no destination port restriction, no private/link-local
blocking. `CONNECT 169.254.169.254:80` and `CONNECT 127.0.0.1:<any>` both work. Fine
for a localhost dev proxy; it must not be exposed.
