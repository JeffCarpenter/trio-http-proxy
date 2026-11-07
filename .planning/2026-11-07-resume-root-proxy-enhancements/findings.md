# Findings: Minimal Trio HTTP Proxy Enhancements

## Hierarchical Delegation Review Findings (Tranches T1 & T2)

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

## Recovery findings (2026-11-07)
- Root plan is stale: shutdown and fuzz review fixes are already committed in their worktrees. Shutdown config and tests remain uncommitted; target-filter has substantial uncommitted implementation and tests.
- No active hcom workers; all existing edits must be preserved and accepted independently.
- Root uses minimal immutable ProxyConfig architecture, not the historical Pydantic/cache architecture in AGENTS.md.

## Security review decision
- Existing target-filter checks only requested literals; hostname DNS can still resolve to loopback/private/metadata endpoints. Must validate every resolved address and connect only vetted numeric addresses (no second hostname lookup).
- Threat model: untrusted CONNECT clients target internal services via literals, hostname aliases, mixed DNS answers, IPv4-mapped IPv6, and DNS rebinding. Default deny all non-global destinations, deny entries override allow entries, explicit private-network opt-out. Limits: DNS queries themselves and administrator-controlled egress/NAT are outside this guard.
- Keep pure matching/address policy in protocol; isolate Trio resolution and connection at proxy boundary. No new dependency needed.

- Trio docs confirm async getaddrinfo and numeric open_tcp_stream. Security will live in default connector; explicitly injected _connect is a trusted alternate transport and retains original hostname contract. Pure target matching remains before any connector.
- Address vetting will reject an entire DNS answer set if any answer is restricted, preventing fallback/rebinding bypass. Numeric connections avoid a second hostname lookup. Sequential address fallback is bounded by the existing total connection timeout (performance tradeoff versus Happy Eyeballs).

- Independently inspected fuzz artifact and ran suite: 11 passed in 0.79s. Worker removed coalescing blind spots and bounded deterministic generators.
- Address normalization no longer IDNA-encodes untrusted malformed ASCII names (avoids uncaught UnicodeError for repeated/oversized labels). IPv4-mapped aliases canonicalize for denylist comparisons; transition IPv6 addresses check embedded IPv4 policy.

## Phase 2 security review and performance
- Reviewer reproduced IPv6 scoped-address denylist bypass with real TCP listener when private-network opt-out enabled. Fixed policy normalization to ignore scope IDs; regression covers requested aliases and scoped DNS results.
- Added embedded IPv4 checks for IPv4-mapped, 6to4 and Teredo private destinations.
- Replaced sequential fallback with 250ms staggered vetted-address racing; preserves default Happy Eyeballs liveness while resolving only once. Tests verify stalled-first fallback, fast failure, simultaneous winner/loser cleanup and parent cancellation after stream acquisition.
- Final isolated target worktree checks: 150 tests pass in 1.00s; Ruff clean; pyright 0 errors; ty clean. A rejected regression-test edit used pre-format context; re-read local block and applied exact match.

## Final decisions and references
- Installed Trio API confirmed through mcpls hover and Context7 /python-trio/trio networking documentation. CLI pyright and ty and warmed published LSP diagnostics agree.
- Reviewer acceptance and independent root runs support merging without further edits. Embedded-address policy covers mapped IPv6, NAT64 well-known prefix, 6to4 and Teredo; arbitrary network-specific translation remains out of scope.
- Coverage 98%; remaining CLI/startup statements exercised by manual subprocess smoke but not counted by in-process coverage.
- Keep worker branches/worktrees; integrate reviewed artifacts without resetting uncommitted source. Root pre-existing planning edits remain user-owned.
