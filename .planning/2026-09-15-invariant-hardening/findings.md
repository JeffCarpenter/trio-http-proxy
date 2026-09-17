# Findings: Proxy Invariant Hardening

## Evidence baseline

- Baseline commit before worktree setup: `463df1b`.
- `uv run pytest -q`: **93 passed in 0.53s**.
- `mcpls`/pyright diagnostics for both `proxy.py` and `cache.py`: no diagnostics.
- Main now has infrastructure-only commit `d464577`, which adds `.worktrees/` to `.gitignore`.

## Confirmed implementation findings

### Server and proxy

1. `start_server()` starts and stops its `proxy_cache` argument, but calls `_serve(server, port)` without binding that cache into the per-connection handler. A custom cache therefore owns lifecycle but not connection operations.
2. `CONN_LIMITER` is acquired inside `http_proxy()`, after `trio.serve_tcp()` has accepted the socket and spawned a handler task. It bounds active handler bodies/tunnels, not accepted descriptors or waiting tasks.
3. Trio 0.34's `serve_tcp()`/`serve_listeners()` expose no pre-accept capacity argument. Listener-level admission or an equivalent custom accept path is required; another handler semaphore cannot satisfy the FD/task invariant.
4. After destination connection succeeds, `tunnel()` sends the 200 response and any trailing bytes before returning the destination stream. If either client send fails, the caller never receives the destination stream and cannot enter its closing context. This is a concrete leak window.
5. CONNECT size enforcement compares total bytes received with `MAXREQSZ` before feeding the new bytes to h11. Correctness therefore depends on TCP read grouping: a small header plus coalesced tunnel bytes can be rejected.
6. Direct reproduction: a 35-byte CONNECT header plus 100 trailing bytes, with `maxreqsz=36`, raises `RequestTooLargeError` instead of accepting the header and preserving the trailing bytes.
7. h11 already exposes post-request bytes through `trailing_data`; the missing part is enforcing the limit against the parsed header boundary rather than the raw read size.
8. Source control flow uses `>= maxreqsz` before parsing, so a complete header delivered at exactly the configured limit is rejected. Proposed documented rule: complete headers of `<= MAXREQSZ` are accepted; incomplete-at-limit and larger headers are rejected.
9. Invalid request-target octets are already deterministic in the exercised path. `CONNECT \xff:443 ...` is rejected by h11 with `RemoteProtocolError`; `tunnel()` sends exactly `HTTP_400_BAD_REQUEST`. This invariant needs regression coverage but may not need a production fix.
10. Cached-address connection handling breaks on the first `trio.TooSlowError`; it does not try the remaining addresses. The final solution must still keep `DEST_CONNECT_TIMEOUT` as one overall bound rather than multiplying it by address count.

### Cache, rate limiting, and bridge lifecycle

11. Cache-backend failure behavior is currently inconsistent rather than configured: rate-limit backend exceptions are logged and bypassed, while DNS/circuit backend exceptions can escape and terminate the connection.
12. `HTTP_429_TOO_MANY_REQUESTS` is built once at import time from `DEFAULT_RATE_LIMIT_PERIOD`. A `ProxyCache` with custom period/TTL cannot affect that header.
13. Cashews' `RateLimitError` has no retry metadata, but the configured backend exposes `get_expire(key) -> int`. In a direct probe using period `10s` and TTL `7s`, the generated key was `rate_limit:rl:client` and its remaining expiry was `7`. Accurate response metadata is therefore available at rejection time.
14. `ProxyCache.stop()` can time out its thread join and then clear `_loop` and `_thread` regardless. That can make a still-live bridge unmanageable. Pending asyncio tasks are not explicitly cancelled and drained before loop closure.

## Design choices now assigned to RW tranches

- Complete CONNECT header boundary is inclusive: `header_size <= MAXREQSZ`.
- Destination connection attempts share one overall timeout.
- `CACHE_FAILURE_POLICY` supports `fail-open` and `fail-closed`; current tranche specification uses `fail-open` as the default. `fail-closed` raises a stable project exception for later HTTP 503 mapping.
- Rate-limit rejection should carry the backend's actual remaining expiry, not merely the configured period.
- Bridge shutdown must cancel/drain pending loop work and verify thread termination before discarding references.

## Coordination lessons

- The original ten invariants were user-supplied. The items above document confirmed causes, existing behavior, and library capabilities discovered during inspection.
- The initial design audit ran too long before delegation. Future repeated worker instructions are centralized in `RW-BOILERPLATE.txt`, with tranche prompts expressed as task-specific deltas.


## Difficult testing pitfalls

### RW1: proxy and server core

- A completed client-side TCP connect may still be queued in the kernel backlog. Admission tests must separately measure userspace-accepted descriptors and handler tasks; client connect success alone is not proof of acceptance.
- IPv4 and IPv6 may create multiple listener accept loops. Capacity reservations must remain globally bounded and be released when accept is cancelled or fails.
- Shutdown can race with a blocked accept or newly available limiter token. Repeated cancellation cycles are needed to reveal leaked reservations.
- Client disappearance can occur after destination connect, during the 200 response, or while replaying trailing bytes. Each path must close the destination without depending on the caller receiving it and without harmful double-close behavior.
- The terminating CRLF sequence can span reads. A complete exact-limit header and arbitrary trailing tunnel bytes can also arrive together. Tests must ensure h11's own incomplete-event bound does not preempt the intended application boundary semantics.
- Fake `TooSlowError` tests can overstate confidence in address fallback. Tests should prove a later address was attempted, all losing or late-success streams were closed, and all attempts shared one overall deadline.
- Binding the selected cache into a connection handler can break custom handlers or the injected `_serve` seam. Compatibility and exact object identity both require coverage.

### RW2: cache and bridge lifecycle

- Some backends configure successfully but connect lazily on the first operation. Both setup-time and runtime failures must exercise `fail-open` and `fail-closed` behavior.
- Backend transport failures must not be confused with normal destination DNS `OSError`, `RateLimitError`, Trio cancellation, or programming errors.
- A multi-step rate-limit operation can partially succeed, such as increment succeeding before expiry fails. Tests must not assume backend operations are atomic.
- Backend expiry values can be rounded and may use sentinel values such as `0` or `-1`. Real-time sleeps near one-second boundaries make `Retry-After` assertions flaky; controlled clocks or bounded assertions are safer.
- Concurrent requests can cross a rate threshold together while only one establishes the ban TTL. Each rejection still needs valid remaining-expiry metadata.
- `stop()` can race with coroutine submission or future completion. Tests must detect unscheduled coroutine warnings, tasks attached to a closed loop, callbacks using an invalid Trio token, and lock-order deadlocks.
- Cleared fields and daemon-thread status do not prove shutdown. Tests must retain and inspect the actual bridge thread and loop objects, then verify termination and closure.
- Backend `close()` itself may stall. Shutdown must remain bounded without discarding references to a live bridge.
- Repeated `start()`, `stop()`, context-manager exit, and failed-start cleanup sequences need coverage because lifecycle idempotence bugs may not appear in a single happy-path test.

### Dependent HTTP/cache integration

- Cache failure can arise during rate checking, circuit probing, DNS lookup, or connection-outcome recording. The selected HTTP behavior must be consistent at every request-visible stage.
- Sending a 429 or 503 can itself fail because the client disappeared. That secondary failure must not mask resource cleanup or leave bridge work running.
- HTTP `Retry-After` must use actual rejection metadata rather than the module-import default, including custom period/TTL configurations and near-expiry values.


## RW1 independent acceptance findings

- Native verification reproduced the worker's green state: **105 tests passed**, Pyright reported 0 diagnostics, scoped Ruff passed, and `git show --check` passed.
- A native Luna 5.6 diagnostics worker independently ran scoped Ruff and Pyright. Both were clean. The earlier mcpls test diagnostic (`start_server` had no `limiter` parameter) was a stale/wrong-workspace result because mcpls resolved the main-worktree source while analyzing a nested worktree test.
- Additional probes passed for actual non-ASCII target octets (`\xff` -> deterministic 400), admission turnover/token release, cancellation during the 200 response, and a truly hanging first address followed by a successful second address within the overall deadline.
- **Repair required:** `start_server()` sets `CV_PROXY_CACHE` but never resets it. After a finite injected server returns, a later defaulted `http_proxy()` or `tunnel()` in the same context can receive the already-stopped custom cache. The branch must reset the ContextVar token in `finally` or remove the unnecessary fallback before acceptance.
- Regression risk to consider during repair: the custom accept loop no longer includes Trio's transient `EMFILE`, `ENFILE`, `ENOBUFS`, and `ENOMEM` retry behavior. This should either be preserved or explicitly tested/documented.


## RW2 in-flight supervision findings

- The first cache tranche accumulated four pytest processes blocked in `futex_do_wait` for roughly 9–12 minutes. They were terminated individually while leaving the worker process alive. This is direct evidence that lifecycle tests can hang the test interpreter when a non-daemon bridge thread survives a failing test.
- The in-flight setup-failure tests use `invalidscheme://localhost` and assume `cashews.Cache.setup()` fails eagerly. Cashews backends can initialize lazily, so this is not a deterministic failure seam; if the assertion fails before cleanup, the newly non-daemon bridge thread prevents pytest from exiting. Acceptance must replace this with an explicit mocked setup failure and guaranteed cleanup.
- The current shutdown draft cancels loop tasks and joins the thread, but it still needs independent checks for drain timeouts, pending-task destruction, default-executor cleanup, and retryability after a failed join.
- RW2 remains unaccepted and uncommitted while these issues are resolved.


## Replacement cache tranche gate

- Native Luna replacement committed `b386ce3` on `invariant/cache-core`, based on integrated main `97c8c50`.
- Scope is clean: only `cache.py`, `__init__.py`, and `tests/test_cache_invariants.py`.
- Independent branch verification: **123 tests passed**, Pyright 0, scoped Ruff clean, and `git show --check` clean.
- The implementation has deterministic setup failure seams, fail-open/fail-closed runtime behavior, destination DNS error preservation, live expiry metadata, and actual bridge task/thread/loop shutdown checks.
- Static review caught and removed an immediate-callback/non-reentrant-lock deadlock before commit. It also restored daemon threads for failure-safe tests while retaining actual termination assertions, and made failed shutdown retryable without clearing live references.
- Integration remains gated on independent process/thread probes for pending work and stop-versus-submit behavior. Consider whether default-executor DNS work needs explicit shutdown coverage; the invariant primarily names the dedicated bridge thread/loop, but no live executor should be silently claimed as cleaned.


## Default-executor shutdown defect after `b386ce3`

- Dedicated bridge lifecycle probes passed for pending asyncio tasks, eight concurrent submitters racing stop, and a forced two-second timeout followed by successful retry with references retained.
- A controlled blocked `socket.getaddrinfo` probe found a stricter defect: `stop()` returned with the bridge thread dead and event loop closed while the captured `asyncio_0` default-executor thread remained alive.
- Root cause: canceling the asyncio wrapper task does not cancel its running executor callable, and `loop.close()` shuts down the executor with `wait=False`.
- Required repair: await `loop.shutdown_default_executor()` before `loop.stop()`. The outer two-second bridge-thread join remains the bounded failure gate; if executor work cannot drain, stop raises and retains live references.
