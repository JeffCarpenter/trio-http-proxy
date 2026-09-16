# Task Plan: Refactor Findings from proxy.py and cache.py Code Review

## Overview
Refactor `src/trio_http_proxy/proxy.py`, `src/trio_http_proxy/cache.py`, and related project files to address all issues documented in `.planning/2026-09-15-proxy-cache-code-review/findings.md`. Every testable concern will be tested with a reproduction/regression test before implementing the fix.

## Phases

### Phase 1: Test Suite Preparation & Reproduction Tests
- [x] 1.1 Add reproduction tests for Tier 1 items:
  - C1: Test that early pipelined TLS ClientHello bytes after CONNECT request are forwarded to dest_stream.
  - C2: Test that BrokenResourceError, client RST, rate limit, and unexpected exceptions inside `http_proxy` do not escape or crash server.
  - C3: Test that circuit breaker with `errors_rate > 50` (e.g. 80%) trips after consecutive failures.
  - C4: Test that destination connect timeout (TooSlowError) with proxy_cache enabled sends HTTP 504 to client.
  - C5: Test that KeyboardInterrupt in start_server (wrapped in ExceptionGroup by serve_tcp) is caught and handled.
  - C6: Test module execution (`python -m trio_http_proxy`) and entry point script.
- [x] 1.2 Add reproduction tests for Tier 2 items:
  - R1: Test that `pipe()` calls `send_eof()` on the destination stream when source reaches EOF.
  - R3: Test that calling `stop()` prevents auto-restart and doesn't hang.
  - R4: Test that DNS resolution fallback works across multiple IPs.
- [x] 1.3 Add tests for Tier 3 nits:
  - BUFMAXSZ validation at startup.
  - Error response headers: Connection: close and Content-Length: 0.
  - Retry-After in 429 response matching configured rate limit period.
  - CV_DESTHOST populated on error paths.
  - Warning instead of exception traceback on connect/DNS errors.

### Phase 2: Implementation of Fixes
- [x] 2.1 Fix C1: Update `process_as_connect_request` to return trailing data from h11, and `tunnel()` to `send_all` trailing data to `dest_stream`.
- [x] 2.2 Fix C2: Wrap entire `http_proxy` body in `except* (BrokenResourceError, ClosedResourceError)` and `except* Exception`.
- [x] 2.3 Fix C3: Implement `_is_circuit_open` checking the `:open` and `:halfopen` lock keys directly without calling `_circuit_probe`.
- [x] 2.4 Fix C4: Ensure `record_connection_result` catches `Exception` and never propagates to caller.
- [x] 2.5 Fix C5: Update `start_server` to `except* KeyboardInterrupt:`.
- [x] 2.6 Fix C6: Add `src/trio_http_proxy/__main__.py` and `[project.scripts]` in `pyproject.toml`.
- [x] 2.7 Fix R1: In `pipe()`, call `await to_stream.send_eof()` on EOF if available.
- [x] 2.8 Fix R2 & Nit: Rewrite `ProxyCache.run()` using cross-thread `token.run_sync_soon` event notification with `fut.cancel()` on timeout/cancellation, eliminating uncancellable worker threads.
- [x] 2.9 Fix R3: Add `self._stopped` flag and hold `self._lock` in `run()` / `run_coroutine()` / `stop()` so `stop()` cannot race or restart.
- [x] 2.10 Fix R4: Implement `resolve_hosts` in `ProxyCache` returning all IPs and update `tunnel` to iterate/fallback through target hosts.
- [x] 2.11 Fix R5: Remove `_is_mock_clock()` from `cache.py`; configure test fixtures appropriately.
- [x] 2.12 Fix R6 & Tier 3 Nits:
  - Update CONN_LIMITER comment.
  - Clean up `_int_env` with range/bounds validation.
  - Replace `assert self._loop is not None` with runtime check.
  - Set `CV_DESTHOST` early in `tunnel()`.
  - Use `logger.warning` for DNS and connect errors.
  - Clean imports in `proxy.py` and define explicit `__all__` in `proxy.py` and `__init__.py`.
  - Update `tunnel()` docstring.
  - Update `HTTP_429` Retry-After header.
  - Add `Connection: close\r\nContent-Length: 0\r\n` to error responses.
  - Document `get_client_key` limitations.
  - Update `CLAUDE.md`.

### Phase 3: Verification & Full Test Suite Pass
- [x] 3.1 Run full test suite with `uv run pytest`.
- [x] 3.2 Verify test coverage and check all reproduction tests pass.
- [x] 3.3 Verify CLI commands (`python -m trio_http_proxy`, `uv run trio_http_proxy`).
- [x] 3.4 Verify linters and type checkers (`ruff check`, `ruff format --check`, `pyright`).
