# Findings: Minimal Trio HTTP Proxy Enhancements

## Acceptance Summary

All findings below were resolved and verified before implementation was pushed
to `origin/mini`. Line references in the historical review notes describe the
reviewed versions, not the current source.

- Shutdown cleanup always notifies the supervisor; shielded drain waits no longer deadlock on parent cancellation. Signal fallback is limited to receiver setup.
- Fuzz tests use incremental receive streams and check exact size boundaries plus preservation of returned and unread tunnel bytes.
- Target policy checks requested hosts and all resolved addresses, rejects mixed public/private DNS answers, and connects only to vetted numeric addresses.
- Security review findings for scoped IPv6, NAT64 private-address aliases, upstream ownership during failed handshakes, and client-reset isolation were fixed with regressions.
- Final acceptance: 217 tests passed, 98% coverage, Ruff/Pyright/ty clean, and real CLI smoke tests passed.
- Private destinations require explicit `TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS=true`; the guard does not replace client authentication or network egress controls.

Detailed decisions and verification are retained in
`.planning/2026-11-07-resume-root-proxy-enhancements/`.

## Historical Hierarchical Delegation Review Findings (Tranches T1 & T2)

### Tranche T1 (Signal Shutdown) — Review by `pi` Reviewer (`lilo`)
- **[P1] Potential cancellation deadlock in `src/trio_http_proxy/proxy.py:238–239`**:
  - Once the drain timeout expires, the supervisor shielded its wait for `drained`.
  - If the enclosing nursery is cancelled while a connection handler finishes shielded cleanup, `conn_runner` exits via cancellation before reaching `drained.set()`.
  - The supervisor waits indefinitely, even though the handler finished.
  - **Remediation**: Wrap `conn_runner` in `try...finally: drained.set()` or join child tasks directly within the supervisor nursery. Add a regression test for external cancellation during timeout cleanup.
- **Documentation**:
  - `docs/configuration.md` is missing `shutdown_timeout` details.

### Tranche T2 (Hypothesis Fuzzing) — Review by `pi` Reviewer (`nemo`)
- **[P1] Chunk-splitting test in `tests/test_fuzz_protocol.py:336–352`**:
  - Sending all chunks into Trio's in-memory stream before parsing coalesces the chunks into a single read.
  - Use `ChunkedReceiveStream` with generated non-empty bodies.
  - Assert that `returned trailing bytes + unread stream bytes == original body`.
- **[P2] Oversize chunk variation in `tests/test_fuzz_protocol.py:433–444`**:
  - Pre-buffering bypasses incremental delivery.
  - Use `ChunkedReceiveStream` for incremental oversize testing.
  - Add exact-limit acceptance and one-byte-over rejection cases to catch off-by-one errors.
