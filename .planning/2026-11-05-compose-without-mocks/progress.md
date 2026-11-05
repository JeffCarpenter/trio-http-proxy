# Progress Log

## Actions Taken
- Audited dynamic mocks, handwritten stand-ins, and monkeypatches; established the 360-test baseline.
- Added local startup-channel composition to ProxyCache and ProxyCacheBuilder, with public structural contracts and unchanged queue defaults.
- Added trusted stream-identity composition to http_proxy, preserving socket-based extraction by default.
- Replaced mock permit/recorder tests with real breaker state/admission tests, including probe finalization and multi-address CONNECT failure accounting.
- Replaced recovery AsyncMock with real Cashews backend delegation, operation tracing, controlled outages, and a manual monotonic clock.
- Repaired the broken-resource reproduction so it provably reaches the 429 path.
- Replaced socket stand-ins with real sockets and DNS cache subclasses/no-op caches with injected resolution.
- Added nine boundary tests for builder forwarding, factory failures, no-I/O startup, stream identities, and real-TCP rate limiting.
- Documented dependency ownership, startup handoff, trusted identity, compatibility adapters, and scheduler limits.
- Completed diff review, repeated regressions, coverage, type checks, lint, and LSP diagnostics.

## Files Created or Modified
- Production: src/trio_http_proxy/{dependencies.py,proxy.py,proxy_cache.py,__init__.py,cache/__init__.py,config/builder.py}.
- Tests: tests/{cache_fakes.py,test_builder.py,test_cache.py,test_cache_backend_recovery.py,test_cache_lifecycle_refactor.py,test_composition_boundaries.py,test_findings_repro.py,test_proxy_invariants.py,test_simplification_contracts.py}.
- Documentation: docs/testing.md and this plan directory.

## Test Results
- Baseline: 360 passed in 10.17s.
- Focused initial migration: 93 passed.
- Boundary/lifecycle/invariant regressions: 49 passed.
- `just ci`: Ruff passed, Pyright 0 errors/warnings, 373 tests passed in 6.19s.
- Full coverage run: 373 passed in 6.49s; 90% overall; destination breaker/permits 100%.
- Repeated concurrency-sensitive subset: 96 passed five times, 480 successful test executions.
- Changed production modules have published, empty LSP diagnostics; no indexing or notification degradation.
- Final diff whitespace check clean; no unittest.mock imports or application monkeypatches remain.

## Error Log
- Local venv has no pyright executable; just/uv finds the installed checker.
- Real TCP rejects unread request bytes with a reset; the transport test now verifies pre-request quota admission directly.
- Trio virtual time must not autojump while waiting for real asyncio bridge work; autojump begins at the destination-connect boundary in the bounded-timeout test.
- Existing formatter violations in proxy.py and test_builder.py reproduce from HEAD; unrelated layout remains untouched.

## Reboot-check Summary
Audit, refactor, and verification complete. No unresolved failures.
Pre-commit `just ci` rerun: Ruff clean, Pyright 0 errors/warnings, 373 passed in 6.20s.
