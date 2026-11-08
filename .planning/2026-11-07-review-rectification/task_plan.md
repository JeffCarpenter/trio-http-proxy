# Task Plan: Review Rectification

## Goal
Rectify all code-review findings across `src/trio_http_proxy/` while strictly preserving structured concurrency invariants, the sans-I/O boundary, and existing test contracts, backed by targeted regression tests.

## Next Step
All four backlog follow-ups and the approved monkeypatch-free testability migration are complete. Validation and known live-LSP limitations are recorded below and in progress.md. The user subsequently authorized a local commit after final verification; no push requested.

## Current Phase
Phase 14 (complete)

## Phases

### Phase 1: Requirements & Discovery
- [x] Perform rigorous, probing code review of all `src/trio_http_proxy/` modules using `mcpls` and repro probes
- [x] Verify existing test suite contracts across `tests/` so no intentional `nano` behavior is broken
- [x] Document findings and root causes in `findings.md`
- **Status:** complete

### Phase 2: Planning & Structure
- [x] Define exact changes for `protocol.py`, `timeouts.py`, `streaming.py`, `proxy.py`, and `docs/configuration.md`
- [x] Design regression tests for every rectified defect
- **Status:** complete

### Phase 3: Implementation
- [x] Update `src/trio_http_proxy/protocol.py` (`parse_connect_target` RFC/IDNA/control-octet hardening, `ConnectParser` `maxreqsz >= 1` guard, reject `Content-Length`/`Transfer-Encoding` on `CONNECT`)
- [x] Update `src/trio_http_proxy/timeouts.py` (`PipeTimeouts` non-negative finite timeout validation)
- [x] Update `src/trio_http_proxy/streaming.py` (`pipe()` EOF check before `on_receive()`, parenthesized exception tuple)
- [x] Update `src/trio_http_proxy/proxy.py` (`_reply_error` best-effort write error suppression, `UnicodeError` defense-in-depth, `bufmaxsz` forwarding in `tunnel`/`http_proxy`, `main()` `--port` range check)
- [x] Update `docs/configuration.md` (`bufmaxsz` parameter on `tunnel` and `http_proxy`)
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Add regression tests across `tests/test_protocol.py`, `tests/test_sansio.py`, `tests/test_tunnel_cleanup.py`, `tests/test_tunnel_forwarding.py`, `tests/test_pipe_deadlines.py`, `tests/test_cli.py`, and `tests/test_proxy_tcp_integration.py`
- [x] Run `mcpls` `get_diagnostics` on all modified files
- [x] Run `just ci` (`ruff format --check`, `ruff check`, `just sansio`, `pyright`, `pytest`)
- **Status:** complete

### Phase 5: Delivery
- [x] Summarize rectifications and verification results for the user
- **Status:** complete

### Phase 6: Coverage Audit
- [x] Run baseline branch coverage: 202 tests pass; total 97%
- [x] Exercise missing lazy-export and both module-entry modes; verify nursery misses were a Coverage.py bug fixed in >=7.16.2
- [x] Re-run coverage: 205 tests, 100% in every source module; document runtime-only TYPE_CHECKING arc
- **Status:** complete

### Phase 7: Configuration Hygiene
- [x] Keep pyproject.toml as the sole Pyright configuration, covering src/ and tests/, Python 3.14 and the project venv
- [x] Add ruff format --check to just lint-ci
- [x] Verify just ci: Ruff, sans-io scan, Pyright and 205 tests all pass
- **Status:** complete

### Phase 8: Mutation Verification
- [x] Run ten isolated scratch-copy guard mutants with bounded test runs
- [x] Add direct error-reply assertions; keep invalid-port runner inert to prevent timeout-only failure
- [x] Record verifier and mutation_report.md: 10/10 killed in 0.40–0.43 s each, no timeouts
- **Status:** complete

### Phase 9: Property Tests and Final Validation
- [x] Assert accepted targets are IPv6 or IDNA encodable, including explicit regression examples
- [x] Exercise arbitrary requests and six dial outcomes inside http_proxy nurseries; assert sibling survival
- [x] Run just ci and branch coverage: 209 tests, 100% source coverage, zero Pyright/mcpls diagnostics
- **Status:** complete

### Phase 10: Proposed Testability Follow-up
- [x] Inventory existing patch sites and propose minimal CLI/stream/connector seams
- [x] Document ordered migration and invariant/coverage/mutation gates in testability_plan.md
- [x] Check final working-tree state and leave implementation of the proposal for a later pass; no commit or push
- **Status:** complete

### Phase 11: CLI Dependency Seams
- [x] Add typed argv/runner inputs preserving main() defaults
- [x] Replace CLI/entry-point patches with explicit inputs and measured subprocesses
- [x] Verify 11 CLI cases, 212 total tests, 100% measured coverage and all ten mutants
- **Status:** complete

### Phase 12: Stream Interfaces and Cleanup
- [x] Replace stream-method patches with a shared typed half-closeable wrapper
- [x] Observe resource close/cancellation before raw-resource teardown, without patching Trio cleanup
- [x] Verify early bytes, half-close, read limits, deadlines and ownership (213 tests; just ci; 100% coverage; 10/10 mutants)
- **Status:** complete

### Phase 13: Server Boundary
- [x] Replace patched serve_tcp delegation with real custom-host port-0 startup, CONNECT, cancellation and resource-closure assertions
- [x] Reuse default-host round-trip coverage; all six live TCP tests pass ten consecutive runs
- **Status:** complete

### Phase 14: Migration Validation
- [x] Verify no monkeypatch/mock.patch/manual dependency replacement in all 13 test/support modules (source and AST audit)
- [x] Run just ci (212 tests), 100% source branch coverage and ten assertion-based mutants; refresh mcpls on 12 affected Python files (three helper-import errors remain only in the live server; fresh CLI Pyright is clean)
- [x] Update all planning records and leave work uncommitted
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Reject ASCII control chars/whitespace (`<= 0x20`, `>= 0x7f`), `%`, `\`, and IDNA-invalid hostnames in `parse_connect_target` | Prevents `urlsplit` from silently stripping `\r`/`\n`/`\t`, rejects embedded NUL/spaces/IPv6 zone IDs, and prevents `socket.getaddrinfo` from raising uncaught `UnicodeEncodeError` that crashes `trio.serve_tcp` |
| Catch `UnicodeError` alongside `OSError` in `tunnel()` | Defense-in-depth so even if a custom or future path reaches `open_tcp_stream` with an unencodable host, it replies `502` and is caught by `http_proxy` instead of crashing the server nursery |
| Reject `Content-Length` and `Transfer-Encoding` headers on `CONNECT` in `ConnectParser._poll()` | RFC 9110 §9.3.6 forbids content on `CONNECT`; prevents HTTP request smuggling / framing desynchronization |
| Suppress `(trio.BrokenResourceError, trio.ClosedResourceError, OSError)` in `_reply_error` | Honors `tunnel()`'s "send an HTTP error when possible" contract without masking the primary `ProxyProtocolError`, `TooSlowError`, or upstream `OSError` |
| Move `if not chunk: break` before `on_receive()` in `pipe()` | Prevents EOF (`b""`) from transitioning `PipeTimeouts` into the `"send"` phase when no chunk will be sent |
| Preserve `RequestTooLargeError -> 414`, `receive_data(b"")` no-op, single handshake `send_timeout`, and per-direction `idle_timeout` | Explicitly pinned by existing tests (`test_sansio.py`, `test_tunnel_forwarding.py`, `test_invariants.py`) and `docs/configuration.md` |

## Errors Encountered
| Error | Resolution |
|-------|------------|
| Live mcpls reports new .streams imports unresolved in three tests | Published, non-indexing diagnostics refreshed on all 12 affected Python files; nine clean. Fresh CLI Pyright checks src and tests with zero errors and all runtime imports pass. Recorded the live-server discrepancy without suppressions or config workarounds. |
| Ruff I001 after changing shared-helper imports to relative imports | Grouped relative imports after third-party imports; final just ci is clean. |
| Ruff ISC004 in new subprocess command literal | Parenthesized the intentional adjacent strings; just ci then passed all 213 stream-stage tests. |
| Entry-point false arc missing after removing patches (212 tests passed, 99% coverage) | runpy globals named imported_entry fall outside the source-package filter; use trio_http_proxy.imported_entry. Real child coverage is explicitly merged via CoverageData.update; all source branches restored without exclusions. |
| `ImportError: cannot import name 'StapledStream' from 'trio.testing'` during initial repro | Used `trio.StapledStream` |
| `mcpls` `get_diagnostics` reported `bytes \| bytearray` return type mismatch on `recording_receive_some` in `tests/test_tunnel_forwarding.py` | Annotated return type as `bytes \| bytearray`; the later approved migration replaces that historical method patch with RecordingStream |
| `ruff check` `PLR0124` (`now == now`) in `timeouts.py` | Replaced with `math.isnan(...)` |
