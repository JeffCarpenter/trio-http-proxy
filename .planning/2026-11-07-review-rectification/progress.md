# Progress Log

## Session: 2026-11-07

### Current Status
- **Phase:** 14 - Complete (original follow-ups and approved testability migration)
- **Started:** 2026-11-07

### Commit Authorization
- After implementation was delivered, the user requested a local commit when ready. Final pre-commit review and validation passed: just ci (212 tests, clean Pyright/Ruff), 100% source coverage, all ten assertion-killed mutants, and diff whitespace checks. Local commit authorized; no push requested.
- Earlier uncommitted/no-commit notes below describe the state before this authorization.

### Approved Testability Migration — Complete
- Final `just ci`: Ruff formatting/lint and sans-I/O scan pass; Pyright reports zero errors/warnings; **212 tests pass** (2.84 s). Source coverage run: **100%** across all eight modules, **279 statements and 68 branches**, no misses/partial branches (3.04 s).
- Final scratch-copy verifier: clean-copy control passes; **10/10 mutants killed by assertions**, 0.38–0.44 s each; no collection errors, crashes or timeouts.
- Source and AST audit of all 13 test/support modules finds no monkeypatch fixture/API, patch call, setattr/delattr, or dependency-method replacement. Non-self attribute writes only configure failure/stall state on test-owned fakes. Removed even the documented receive-count hooks in favor of the shared wrapper.
- CLI complete: main(argv=None, *, runner=trio.run), typed callable, unchanged default invocation. Eleven CLI cases use explicit args and runners, explicit subprocess env mappings and real entry-point execution. Public CoverageData merging measures child execution without instrumentation patches.
- Shared RecordingStream requires a HalfCloseableStream, delegates checkpoints/receive limits/EOF, accepts one send hook and records successful writes, close calls and cancellation. Used by cleanup, forwarding and large-payload tests. Existing _connect seam reused; no cleanup/clock factories or containers.
- Failed handshakes assert exactly one upstream close before raw-resource teardown. Successful handshake verifies early bytes, 200 reply and caller ownership. Stalls/cancellation, bufmaxsz round trips and EOF remain observable.
- Removed patched serve_tcp delegation. Existing real default-host round trip remains; the port-0 cancellation test checks distinct 127.0.0.2 binding, complete CONNECT response, closed listeners and both tunnel endpoints before teardown. All six live TCP tests pass **ten consecutive runs** (0.03–0.04 s per run), with no sleeps or fixed-port races.
- Refreshed published, non-indexing mcpls diagnostics on all 12 affected Python files: nine clean; three tests report unresolved new .streams helper imports in the live server. Fresh CLI Pyright checks src and tests with zero errors; pytest imports work. Discrepancy recorded, not hidden through suppressions/config changes.
- Coverage investigation: imported_entry runpy namespace was outside the source-package filter (99%); package-qualified run name restores the real false arc without exemptions. Verified Coverage APIs against current docs and installed-library hover. mcpls references/call hierarchy confirm CLI and serve_tcp boundaries.
- Fixed transient Ruff ISC004 (adjacent strings) and I001 (relative-import ordering). Stream-stage gate passed 213 tests; deleting the redundant delegation test yields the final 212.
- User-approved plan delivered with existing step IDs and prior work preserved. Baseline was 209 tests, full source coverage and ten assertion-killed mutants. Planning records synchronized; no commit or push requested or performed.

### Original Follow-up Actions (before approved DI migration)
- Final validation: just ci passes (209 tests, zero Pyright errors); coverage is 100% for all eight modules (279 statements, 68 measured branches); all six modified/new Python files have published, non-indexing, empty mcpls diagnostics; git diff --check passes.
- Added IPv6/IDNA assertions with explicit historical failures and valid targets. Added bounded deterministic nursery fuzzing with real memory streams, six connector outcomes, and an assertion that a sibling survives. New property requires no monkeypatching or network.
- Documented concrete DI seams, migration order and acceptance gates in testability_plan.md; next_steps.yaml marks the original four items complete and the DI work proposed. No production DI refactor, commit, or push in this pass.
- Added reproducible scratch-copy mutation verifier and mutation_report.md. All ten listed mutants die by assertion in 0.40–0.43 s each; clean scratch control passes; no timeouts.
- Added direct error-reply suppression assertions with a fault-injecting stream (no patching). Existing invalid-port tests use an inert runner so missing validation cannot start a server.
- User clarified the long-term target: no monkeypatching in tests. Proposed small argv/runner and stream seams, reuse of _connect, and real loopback server tests; defer implementation rather than expanding this backlog.
- Audited coverage: baseline 202 tests/97%; added package-dir and module-entry tests; 205 tests now give 100% line and branch coverage.
- Confirmed Python 3.14 phantom nursery arcs via Coverage.py changelog and an isolated newer-version run. Require >=7.16.2 (lock: 7.17.2); only the impossible runtime TYPE_CHECKING branch is annotated.
- Removed shadowing pyrightconfig.json; pyproject.toml includes src/ and tests/, explicit Python 3.14, and the project venv.
- just lint-ci now checks formatting and lint. just ci passes with zero type errors and 205 tests.
- Used mcpls hover, document symbols and tunnel incoming/outgoing call hierarchy to verify ownership and buffer-forwarding paths and identify regression tests.

### Actions Taken
- Completed full code review of `src/trio_http_proxy/` using `mcpls` (`get_document_symbols`, `get_diagnostics`, `get_hover`, `get_references`, `get_definition`) and targeted Python repro probes.
- Verified live crash reproducer (`CONNECT .example.com:443 HTTP/1.1` crashing `start_server` with unhandled `ExceptionGroup(UnicodeEncodeError)`).
- Verified `_reply_error` exception-masking reproducer on `tunnel()`.
- Implemented fixes across `protocol.py`, `timeouts.py`, `streaming.py`, `proxy.py`, and `docs/configuration.md`.
- Added 36 new regression test cases across `tests/test_protocol.py`, `tests/test_sansio.py`, `tests/test_pipe_deadlines.py`, `tests/test_tunnel_cleanup.py`, `tests/test_tunnel_forwarding.py`, `tests/test_cli.py`, and `tests/test_proxy_tcp_integration.py`.
- Verified zero LSP diagnostics via `mcpls` `get_diagnostics` on all modified files and ran `just ci` (`ruff format --check`, `ruff check`, `just sansio`, `pyright`, `pytest`).
- Committed fixes (`b13f8bd`) and recorded retrospective surprises in `findings.md` (`5c959fe`).
- Pushed `nano` to `origin/nano` (`45cdf3f..5c959fe`) and wrote ROI-ranked follow-up backlog to `next_steps.yaml`.

### Test Results
| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| `mcpls` `get_diagnostics` on all modified `src/` and `tests/` files | 0 diagnostics | 0 diagnostics | ok |
| `just ci` (`ruff`, `sansio`, `pyright`, `pytest`) | pass | 0 errors, 202 passed | ok |

### Errors
| Error | Resolution |
|-------|------------|
| `StapledStream` import from `trio.testing` in one-liner probe | Imported from `trio.StapledStream` |
| `mcpls` `get_diagnostics` caught `bytes \| bytearray` return type in `test_tunnel_forwarding.py` | Annotated `bytes \| bytearray` and used `monkeypatch.setattr` |
| `ruff check` `PLR0124` (`now == now`) in `timeouts.py` | Replaced with `math.isnan(...)` |
