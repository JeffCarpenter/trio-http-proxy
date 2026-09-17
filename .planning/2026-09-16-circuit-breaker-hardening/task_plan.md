# Task Plan: Circuit Breaker, Rate Limiter & Lifecycle Hardening

## Goal
Address all P1 and P2 architectural, concurrency, and security findings:
1. Concurrency-correct circuit breaker admission protocol with tokenized, epoch-aware leases (`CircuitPermit`).
2. Half-open permit leak prevention under cancellation, unhandled exceptions, and aborts.
3. Breaker epoch invalidation across `clear()`.
4. Host identity canonicalization (case, trailing dot, IP normalization).
5. Bounded breaker manager with LRU / idle eviction.
6. Sliding window $O(1)$ amortized complexity using `collections.deque` and incremental error count.
7. Cashews rate-limiting state transition (`period` counting window -> threshold -> `ttl` ban duration) and zero-duration safety.
8. Decouple local circuit breaker failure domain completely from Redis backend degradation.
9. Backend recovery health probing in fail-open mode.
10. Startup health ping before publishing RUNNING; no thread when backend features disabled.
11. Unified `LifecycleState` enum with atomic locked transitions and prohibition of restart after STOPPED.
12. Startup timeout cancellation event preventing orphan bridge threads.
13. Credential redaction in URLs for logs and exceptions.
14. Strict namespace regex validation (`^[a-zA-Z0-9_-]+$`).
15. Semantic duration parsing in `__init__` (rejecting falsey/"0m" bypasses).
16. Comprehensive bridge shutdown protocol (`shutdown_asyncgens`, executor timeout).
17. Happy-Eyeballs DNS integration and explicit stream typing for `get_client_key`.

## Tranches (Hierarchical Delegation Pipeline)

- [x] **Tranche 1: Circuit Breaker Admission Protocol, Permit Token & Eviction**
  - Scope: `src/trio_http_proxy/cache.py`, `src/trio_http_proxy/proxy.py`
  - Goal: Implement `CircuitPermit`, epoch-aware transitions, half-open leak prevention, destination canonicalization, LRU bounded manager, and deque sliding window. Update `proxy.py` connection workflow to use permits.
  - Deps: None
  - Accept: Tokenized leases prevent pre-open completions from stealing probe identity; cancelled probes release permit; bounded manager evicts idle breakers; window updates in $O(1)$.

- [x] **Tranche 2: Rate Limiter Protocol & Backend Error Boundary**
  - Scope: `src/trio_http_proxy/cache.py`
  - Goal: Implement Cashews ban-TTL protocol (`incr(expire=period)`, `expire(ttl)` on limit + 1). Require positive durations. Limit `_handle_backend_failure` to genuine backend exceptions.
  - Deps: None
  - Accept: Threshold crossing applies ban TTL; zero duration rejected in `__init__`; internal programming errors re-raised without triggering degradation.

- [x] **Phase 2.5: Adversarial Review Remediations (P1 & P2 Invariant Fixes)**
  - Scope: `src/trio_http_proxy/cache.py`, `src/trio_http_proxy/proxy.py`, `tests/test_cache_invariants.py`
  - Goal: Remediate defects identified in §6.5–§6.14:
    - Breaker collision across `clear()` via monotonic `_epoch_counter` and `permit.breaker is self`.
    - Permitless probe theft in `HALF_OPEN`.
    - Active probe tracking (`_active_probe_id`) preventing aborted probe resuscitation.
    - Wedged `HALF_OPEN` deadlock timeout recovery.
    - Non-blocking shielded socket teardown via `trio.aclose_forcefully`.
    - Empty `target_hosts` failure recording.
    - Remove `RuntimeError` from `BACKEND_EXCEPTIONS`.
    - Bracketed IPv6 canonicalization.
    - Semantic duration parsing for `dns_cache_ttl_sec` and Cashews decorator registration gating.
    - Eviction call pruning in `_evict_one_locked`.
  - Deps: Tranche 2
  - Accept: All remediations applied, tests updated and passing, 0 mcpls diagnostics.

- [x] **Tranche 3: Failure Domain Decoupling, Health Probing & Startup Health Check**
  - Scope: `src/trio_http_proxy/cache.py`, `tests/`
  - Goal: Decouple circuit breaker from backend degradation. Implement periodic health probe recovery in fail-open mode. Implement startup health ping with fail-fast in fail-closed mode. Eliminate bridge thread when backend features are disabled.
  - Deps: Tranches 1, 2
  - Accept: Circuit breaker functions during backend outage; degraded backend recovers upon successful ping; startup fails fast in fail-closed if unreachable; no thread when disabled.

- [x] **Tranche 4: Lifecycle State Machine, URL Redaction & Configuration Hardening**
  - Scope: `src/trio_http_proxy/cache.py`, `src/trio_http_proxy/util.py`, `tests/test_cache_lifecycle_security.py`
  - Goal: `LifecycleState` enum (`NEW`, `STARTING`, `RUNNING`, `STOPPING`, `STOPPED`). Atomic transitions. Prohibit restart. Shared abort event preventing orphan thread on startup timeout. Redact userinfo credentials in URLs. Strict namespace regex. Semantic TTL parsing. Proper shutdown (`shutdown_asyncgens`).
  - Deps: Tranches 1, 2, 3
  - Accept: Clean lifecycle transitions; no orphan threads; sanitized credentials in logs/exceptions; strict namespace validation. 48/48 tests passing; 222/222 full suite passing; 0 diagnostics.

- [x] **Tranche 5: Verification, Invariant Tests & mcpls Diagnostics**
  - Scope: `tests/`, `src/trio_http_proxy/`
  - Goal: Comprehensive test suite for all new invariants, full pass on existing tests, verify Ruff, Pyright, and `mcpls` diagnostics clean. Class-to-module refactor completed with zero regressions.
  - Deps: Tranches 1, 2, 3, 4
  - Accept: 100% test pass rate (222/222), 0 Ruff errors, 0 Pyright issues, 0 `mcpls` diagnostics.

## Status
- Complete: Tranche 1, Tranche 2, Phase 2.5, Tranche 3, Tranche 4, Tranche 5 (Full Verification, Class-to-Module Refactor & mcpls Clean)


