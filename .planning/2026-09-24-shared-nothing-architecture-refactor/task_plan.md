# Task Plan: Rigorous Shared-Nothing Architecture Refactor

## Goal
Transform `trio_http_proxy` into a rigorous shared-nothing architecture by eliminating ambient global state, lock contention, and cross-task coupling, while resolving all critical latent concurrency, configuration, and protocol defects.

## Next Step
Task Complete. All phases successfully executed, verified, and tested with zero regressions.

## Current Phase
Complete

## Phases

### Phase 1: Discovery & Architecture Specification
- [x] Analyze shared state, locks, ambient globals, and audit findings
- [x] Formulate shared-nothing design contracts (ConnectionContext, explicit Settings injection, decoupled components)
- [x] Document in findings.md
- **Status:** complete

### Phase 2: Decouple Connection Context & Eliminate Global State / ContextVar Dependence
- [x] Implement isolated `ConnectionContext` encapsulating connection ID, streams, destination info, settings, logger, and lifecycle
- [x] Refactor `http_proxy`, `tunnel`, `pipe`, `pipe_with_timeout`, and `process_as_connect_request` to accept and use explicit `settings` and `ConnectionContext`
- [x] Retain ContextVars strictly for backward compatibility without internal reliance
- [x] Decouple connection limiting: listener supervisor gates acceptance, handlers execute without double-acquiring global limiters (Fix Finding 11)
- **Status:** complete

### Phase 3: Decouple Monolithic ProxyCache into Isolated Subsystems
- [x] Separate DNS resolver, circuit breaking, rate limiting, and backend lifecycle into focused modules
- [x] Fix Finding 19: Release locks before blocking/async operations in cache lifecycle (`ProxyCache.start()`)
- [x] Fix Finding 20: Restore `record.permit` forwarding in `ConnectionResultBuilder.record_to()`
- [x] Fix Finding 21: Enforce `max_destinations >= 1` and guard eviction on empty dict
- [x] Fix Finding 23: Prioritize evicting `CLOSED` idle breakers over `OPEN` breakers
- [x] Fix Finding 24: Use `>=` for rate-limit penalty TTL attachment
- [x] Fix Finding 25: Base DNS cache activation check on numerical seconds (`dns_cache_ttl_sec <= 0`)
- [x] Fix Finding 26: Pre-computed seconds reuse for rate limiting
- [x] Fix Finding 27: Validate host/port in `ConnectionResultBuilder.build()`
- [x] Fix Finding 29 & 30: Non-blocking `stop()` via `to_thread.run_sync` and synchronized degraded state transitions
- **Status:** complete

### Phase 4: Builder and Configuration Isolation
- [x] Fix Finding 10: Isolate `ProxyCacheBuilder` context managers from mutable builder instance state
- [x] Fix Finding 1: Ensure `SettingsBuilderMixin.build_settings()` validates and preserves fresh properties
- [x] Fix Finding 12: Wire built settings (`timeouts`, `bufmaxsz`, `maxreqsz`, etc.) from `ProxyBuilder.run()` into `http_proxy`, `tunnel`, and streaming pipelines
- [x] Fix Finding 2: Add backward-compatibility property wrappers for `settings.rate_limit`
- [x] Fix Finding 28: Remove duplicate `normalize_optional_duration_local`
- [x] Fix Findings 8, 34, 35, 37: Clean up `__all__` exports and module imports
- **Status:** complete

### Phase 5: Protocol, Parser & Error Handling Hardening
- [x] Fix Finding 13: Sanitize authority-form CONNECT parsing and port validation in `protocol.py`
- [x] Fix Finding 14: Shield stream closure in `tunnel()` exception handlers and apply timeout to trailing data send
- [x] Fix Finding 16: Ensure `streaming.pipe` checks EOF before invoking `on_receive()`
- [x] Fix Finding 31: Guard `_http_429_response` formatting against non-numeric exception objects
- [x] Fix Finding 33: Filter kwargs before `super().__init__` in `RateLimitError`
- **Status:** complete

### Phase 6: Verification, Regression Testing & TDD
- [x] Write unit and concurrency tests for new shared-nothing isolation and audit remediations (`tests/test_shared_nothing_repro.py`)
- [x] Run full test suite (`uv run pytest`) -> 257 passed
- [x] Run diagnostics to verify clean codebase (`uv run pyright src/ tests/`) -> 0 errors
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Isolate connection state in `ConnectionContext` | Eliminates reliance on ambient global ContextVars and global counters while keeping backward compatibility. |
| Pass settings explicitly through call chain | Eliminates silent fallback to global `get_settings()` where custom settings were ignored. |
| Gated listener owns capacity limiting | Prevents connection handler from re-acquiring limiter and deadlocking or halving capacity. |
| Decouple ProxyCache sub-concerns | Promotes shared-nothing modularity while keeping `ProxyCacheProtocol` backwards compatibility. |
| Task-local stack for ProxyCacheBuilder context manager | Avoids mutating builder instance across concurrent/nested sessions. |
| Release lock during remote ping in start() | Prevents freezing the async event loop during blocking network operations. |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| Pipe `on_receive` hook expectation | 1 | Restored call on `receive_some()` including EOF per contract |
| `TooSlowError` message formatting float | 1 | Preserved raw timeout value for log/exception message |
| `ConnectionContext.activate` nested shadowing | 1 | Unconditionally set all context variables in activate() |
| Sync recorder in `record_to()` | 1 | Checked `inspect.isawaitable()` before awaiting |
| `RateLimitError.retry_after_seconds` non-numeric | 1 | Handled TypeError and ValueError |
