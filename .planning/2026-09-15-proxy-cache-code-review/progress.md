# Progress Log

## 2026-09-16
- Initialized refactoring plan from findings.md.
- Verified test suite status: 78 tests passing via `uv run pytest`.
- Phase 1 completed: Created `tests/test_findings_repro.py` containing 15 reproduction tests covering C1-C6, R1, R3, R4, R5, and Tier 3 nits.
- Executed `uv run pytest tests/test_findings_repro.py` and confirmed all 15 reproduction tests FAIL, matching the findings report precisely.
- Phase 2 completed: Refactored `proxy.py`, `cache.py`, `__init__.py`, `__main__.py`, and `pyproject.toml`:
  - C1: Forward trailing pipelined bytes from `h11_conn.trailing_data` to `dest_stream`.
  - C2: Isolated `http_proxy` with `except* (BrokenResourceError, ClosedResourceError)` and `except* Exception` so connection errors never crash `serve_tcp`.
  - C3: Rewrote circuit breaker state check to directly inspect `:open` and `:halfopen` lock keys without calling `_circuit_probe` or registering false successes.
  - C4: Handled `TooSlowError` and all exceptions in `record_connection_result` so 504 Gateway Timeout is sent reliably.
  - C5: Updated `start_server` to `except* KeyboardInterrupt:` to catch `ExceptionGroup`/`BaseExceptionGroup`.
  - C6: Added `__main__.py` and registered entry points in `pyproject.toml` (`trio_http_proxy = "trio_http_proxy.proxy:main"`).
  - R1: Implemented half-close in `pipe()` calling `await to_stream.send_eof()` upon reading EOF.
  - R2: Replaced `to_thread.run_sync` in `ProxyCache.run()` with cross-thread event notification (`token.run_sync_soon(event.set)`), eliminating worker thread usage and supporting clean cancellation/timeouts with `fut.cancel()`.
  - R3: Protected `self._stopped` state and synchronized teardown with `self._lock` to prevent race conditions or silent restarts.
  - R4: Implemented `resolve_hosts` in `ProxyCache` and fallback connection attempts across multiple IP addresses in `tunnel()`.
  - R5: Removed `_is_mock_clock()` test conditionals from library code and configured test fixtures appropriately.
  - R6 & Nits: Corrected `CONN_LIMITER` comment, added startup validation for `BUFMAXSZ >= 1`, added `Connection: close` and `Content-Length: 0` headers, dynamic `Retry-After` header, early `CV_DESTHOST` tracking, warning logging without tracebacks for client-triggered errors, and explicit `__all__` definitions.
- Phase 3 completed: Verified all 93 tests pass (15 reproduction tests + 78 existing tests), pyright passes with 0 errors/warnings, and ruff format/check pass cleanly.
