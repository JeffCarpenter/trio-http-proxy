# Tier 1 — cheap + real bug/gap
1. IPv6 target parse bug (partition(b":") breaks [::1]:443) — small fix (rsplit last colon or urllib.parse), add regression test. Real correctness bug, near-zero effort. [x]
2. start_server test still monkeypatches trio.serve_tcp — swap for _serve DI param, matches rest of suite. Cheap, closes inconsistency. [x]
3. maxreqsz/bufmaxsz env override — copy PORT pattern. Trivial. [x]

# Tier 2 — moderate effort, high value if proxy ever leaves localhost
4. Timeouts (trio.fail_after/move_on_after on recv/connect/send) — real slowloris exposure, moderate effort. [x]
   - CONNECT_READ_TIMEOUT (fail_after, process_as_connect_request) -> 408 on trip
   - DEST_CONNECT_TIMEOUT (fail_after, tunnel's _connect call) -> 504 on trip
   - PIPE_IDLE_TIMEOUT (move_on_after per receive_some in pipe()) -> idle side
     cancels the sibling pipe() via a shared nursery.cancel_scope, so the whole
     tunnel closes, not just the idle direction
   - All three env-overridable via new `_float_env`, same pattern as PORT/BUFMAXSZ
   - Tested with trio.testing.MockClock(autojump_threshold=0) — slowaris-style
     tests simulate multi-second stalls that resolve in ~0.2s real time; every
     new test wrapped in its own trio.move_on_after() watchdog so a regression
     fails fast instead of hanging the suite
   - PIPE_SEND_TIMEOUT (move_on_after around send_all in pipe()) -> stalled
     destination (send_all_hook that never returns, i.e. a filled real TCP send
     buffer) also cancels the sibling via the shared nursery.cancel_scope. Was
     deferred as unbounded send_all; now closed the same way as the receive-side
     gaps, tested via MemorySendStream.send_all_hook = trio.sleep_forever.
5. Connection cap/backpressure (semaphore around serve_tcp handler) — pairs naturally with #4, moderate effort. [x]
   - MAXCONNS env-overridable (_int_env, default 1000)
   - CONN_LIMITER = trio.CapacityLimiter(MAXCONNS), module-level, shared
     across all http_proxy() calls via default arg (injectable for tests)
   - http_proxy() wraps its whole body in `async with client_stream, limiter:`
     -- over-cap connections block waiting for a slot (backpressure) rather
     than being rejected
   - Tested: 2nd conn parks on the limiter without even reaching _connect
     while 1st holds it; slot frees and 2nd proceeds once 1st completes
6. Target allowlist/denylist (SSRF risk) — only matters if deployment isn't fully trusted-localhost; ask before investing here — depends on threat model.

# Tier 3 — test coverage gaps, moderate effort
7. Concurrent-connections test — verifies _conn_ids/task isolation, meaningful, straightforward with memory streams. [x]
   - test_http_proxy_handles_concurrent_connections_without_cross_talk: 3
     http_proxy() tasks run truly concurrently in one nursery (not
     one-at-a-time), each sent a distinct payload in-flight simultaneously;
     asserts each payload lands only at its own dest stream, proving
     ContextVar/task isolation holds under real concurrency
   - Found + fixed a real race in the earlier limiter-release test
     (test_http_proxy_releases_limiter_slot_when_connection_completes):
     it fed CONNECT bytes to only one client before the other's task could
     win the CapacityLimiter race, so if scheduling order flipped, the
     "loser" task could sit in process_as_connect_request() waiting on its
     *real* (non-mock-clock) 10s CONNECT_READ_TIMEOUT. Fixed by sending
     both CONNECT requests before awaiting either reply.
8. Large-payload/multi-chunk-boundary test — cheap to add, catches chunking bugs.
9. Oversized-header overflow test (not just target-length) — more realistic attack surface than current test.

# Tier 4 — lower ROI
10. Real-socket integration test — valuable but flaky/slow, only 1 code path (serve_tcp+SocketStream) uncovered.
11. Graceful shutdown (SIGTERM drain) — real gap bufor a CLI tool.
12. Structured logging (replace print) — cosmetic, no bug risk. [x]
13. Fuzz/property testing on process_as_connect_reqain payoff given h11 already does parsing.
14. memory-stream internals coupling note, __main__ line coverage — doc-only / accept as-is, not worth fixing.

# Tier 5 — Architectural & Design Refactoring (Perlis Epigram 9)
15. Unify configuration into single model; eliminate 31 forwarding properties and 18-param signature.
16. Replace hot-path `inspect.signature` with `typing.Protocol` for `ProxyCache`.
17. Consolidate `errors/*.py` and standalone enums to restore module-level cohesion.
18. Modularize `proxy.py` into `protocol.py` (HTTP CONNECT wire parsing) and `streaming.py` (duplex piping).
