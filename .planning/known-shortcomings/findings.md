# Findings: known shortcomings in proxy.py and test_proxy.py

Snapshot as of commit `2ce50f7` (after the memory-stream test refactor documented in
`.planning/memory-stream-test-refactor/findings.md`). Not bugs found by testing — a
review of what the current implementation and test suite do *not* cover, so the gaps are
tracked rather than silently assumed away.

## Implementation (`src/trio_http_proxy/proxy.py`)

- ~~**No timeouts anywhere.**~~ **Fixed.** `process_as_connect_request` (via `tunnel`)
  is now bounded by `CONNECT_READ_TIMEOUT` (`trio.fail_after`, 408 on trip);
  `tunnel`'s `_connect` call is bounded by `DEST_CONNECT_TIMEOUT` (`trio.fail_after`,
  504 on trip); each `pipe()` direction is bounded by `PIPE_IDLE_TIMEOUT`
  (`trio.move_on_after` per `receive_some`), and cancels its sibling via a shared
  `nursery.cancel_scope` so an idle tunnel closes entirely rather than leaking the
  other direction's task; all four (below) are env-overridable (`_float_env`, same
  pattern as `PORT`/`BUFMAXSZ`).
  ~~`send_all` remains unbounded~~ **Also fixed.** Each `pipe()` direction's
  `send_all` is bounded by `PIPE_SEND_TIMEOUT` (`trio.move_on_after`); a stalled
  destination (e.g. a client that completes CONNECT and stops reading, filling
  the OS send buffer) trips it the same way an idle receive does, cancelling the
  sibling via the shared `nursery.cancel_scope`. Tested via
  `MemorySendStream.send_all_hook` set to `trio.sleep_forever` — the hook fires
  *after* `send_all()` has already buffered the chunk, so it simulates the call
  itself stalling (a filled real TCP send buffer), not data never arriving.
- **No connection limit or backpressure.** `trio.serve_tcp` spawns one task per inbound
  TCP connection with no cap, so a connection flood has no throttle.
- **No target allowlist/denylist.** The proxy will `CONNECT` to any `host:port`,
  including internal or loopback addresses (e.g. `127.0.0.1`, a cloud metadata
  endpoint). This is an SSRF-shaped risk for anything other than a fully trusted,
  localhost-only deployment.
- **IPv6 literal targets are parsed incorrectly.** `process_as_connect_request` does:

  ```python
  desthost, _, destport = h11_nextevt.target.partition(b":")
  ```

  For a target like `[::1]:443`, this splits on the *first* colon inside the address
  itself, not the host:port separator, producing garbage instead of a real host/port.
- **No graceful shutdown.** `start_server` only catches `KeyboardInterrupt`; there's no
  SIGTERM handling to drain in-flight tunnels before exit.
- ~~**Logging is bare `print()`.**~~ **Fixed.** Replaced bare `print()` with standard library `logging` (`logger` / `log()`), structured contextual logging (`ConnectionLoggerAdapter` / `ConnectionFilter`) tracking `conn_id`, `desthost`, `direction`, `conn_prefix`, standard log levels (`DEBUG`, `INFO`, `WARNING`, `ERROR`), proper exception formatting via `logger.exception` / `exc_info=True`, `LOG_LEVEL` env override, and non-intrusive `NullHandler` by default.
- **Dead code:** `decoded_and_indented` (line 25) is defined but never referenced
  anywhere in the module.
- **`maxreqsz`/`bufmaxsz` are hardcoded** function-default constants with no env-var
  override, unlike `PORT`.

## Tests (`tests/test_proxy.py`)

- **Zero real-socket coverage.** Every test goes through
  `trio.testing.memory_stream_pair()`. The actual `trio.serve_tcp` + real
  `SocketStream` code path is only exercised by the manual `curl`-based smoke test
  documented in `CLAUDE.md`, not by anything in the automated suite. OS-level partial
  read/write behavior is untested.
- **No IPv6 / malformed-target parsing tests.** Directly related to the parsing bug
  above — nothing in the suite would fail if that bug were introduced or reintroduced.
- **No concurrent-connections test.** Every `http_proxy`/`tunnel` test drives exactly
  one connection at a time. `_conn_ids` behavior and task isolation under several
  simultaneous tunnels is unverified.
- ~~**No large-payload / multi-chunk-boundary test through the full tunnel.**~~
  **Fixed.** `test_http_proxy_forwards_large_payload_across_chunk_boundaries` sends a
  `BUFMAXSZ * 3 + 777`-byte payload (non-boundary-aligned remainder) through both
  tunnel directions, asserts byte-for-byte integrity, and uses `receive_some_hook`
  counters to prove `pipe()`'s read loop actually iterated ≥4 times per direction
  rather than one buffered `receive_some()` call returning everything at once.
- **`start_server` tests still monkeypatch the global `trio.serve_tcp`** — the one
  place in the suite still coupled to a real trio API via `monkeypatch`, inconsistent
  with the `_connect`/`_nextid`-style dependency injection used everywhere else after
  the memory-stream refactor. Could take a `_serve` param for symmetry.
- **`test_http_proxy_mixed_exception_group_logs_both_handlers` encodes internal
  knowledge of `trio.testing.MemoryReceiveStream`'s two-checkpoint-then-hook timing**
  (via the `trio.Event` gate added during the memory-stream refactor). Correct today,
  but coupled to trio's testing-stream internals rather than only to `proxy.py`'s own
  behavior — worth re-checking if trio's memory-stream implementation ever changes.
- **Line 145 (`if __name__ == "__main__": trio.run(start_server)`) is uncovered** —
  the one gap in the otherwise 99% coverage. Acceptable but worth naming explicitly.
- **No fuzz/property-based testing of `process_as_connect_request`.** Coverage is a
  fixed set of hand-picked malformed inputs; broader h11 parsing edge cases are
  unchecked.
- **`test_process_as_connect_request_enforces_max_size` only overflows via target
  length**, not via an oversized header (e.g. a huge `Host:` value) — a more realistic
  overflow vector in practice.

## Architectural & Design Shortcomings (Perlis Epigram 9 & Separation of Concerns)

Documented in detail in `.planning/2026-09-19-code-review-perlis-epigram-9/findings.md`:

- **Severe Epigram 9 Violation (Data Structure Proliferation):** 14 bespoke data structures across `src/` (custom classes, dataclasses, enums, exceptions, and Pydantic models) with an average ratio of **1.21 functions per data structure** (e.g. `RateLimitSettings`, `CircuitBreakerSettings`, `CircuitState`, `LifecycleState`, and 4 single-exception classes).
- **Configuration Friction & Forwarding Overhead:** Segmenting configuration into 4 sub-models forced `Settings` to implement **31 manual forwarding properties** and a 56-line `_raw_groups` reconciliation loop, while `ProxyCache.__init__` accepts **18 unpacked parameters**.
- **Hot-Path Dynamic Reflection (`inspect.signature`):** `proxy._record_connection_result` inspects `proxy_cache.record_connection_result` at runtime via `inspect.signature` on every destination connection result instead of relying on a stable interface or `typing.Protocol`.
- **Covert Dependency Injection via `contextvars.ContextVar`:** 7 global context variables in `connection_context.py` are used as side-channels to pass application dependencies (like `CV_PROXY_CACHE`) across call frames.
- **Inverted Cohesion:** 4 single-exception files and 2 single-enum files exist in isolation, while `proxy.py` (777 lines, 5 distinct architectural responsibilities) and `proxy_cache.py` (843 lines) remain heavy monoliths.
