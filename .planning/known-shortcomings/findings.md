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
  other direction's task. All three are env-overridable (`_float_env`, same pattern
  as `PORT`/`BUFMAXSZ`). `send_all` remains unbounded — see below.
- **`send_all` is still unbounded.** A client that completes the CONNECT
  handshake and then simply stops reading can fill the OS send buffer and hold
  the task open forever — the same slowloris shape as the now-fixed receive-side
  gaps above, on the write side instead. Deferred: `memory_stream_pair()`
  buffers unboundedly, so exercising this needs a `send_all_hook` (or a real
  socket) to simulate backpressure rather than the trickle/idle patterns used
  for the receive-side timeout tests.
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
  There is no test that would catch this (see below).
- **No graceful shutdown.** `start_server` only catches `KeyboardInterrupt`; there's no
  SIGTERM handling to drain in-flight tunnels before exit.
- **Logging is bare `print()`.** No levels, no timestamps, no way to redirect or
  silence independently of stdout.
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
- **No large-payload / multi-chunk-boundary test through the full tunnel.**
  `test_http_proxy_full_round_trip` only sends small, single-shot payloads in each
  direction.
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
