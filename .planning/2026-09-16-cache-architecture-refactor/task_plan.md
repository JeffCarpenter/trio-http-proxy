# Task Plan: Cache Architecture Refactor & Invariant Hardening

## Goal
Resolve all 11 architectural and correctness findings across `ProxyCache`:
1. Trustworthy failure policy (`suppress=False` on backend, `ProxyCache` sole owner).
2. Safe namespaced `clear()` (scoped to owned prefix, no database-wide flush).
3. Trio-native circuit breaker state machine (explicit CLOSED/OPEN/HALF_OPEN, gating probe discipline, no Cashews private keys).
4. Remove Cashews private storage schema dependencies (owned rate limit & circuit breaker protocols).
5. Documented bounded timeout/cancellation semantics in `run()`.
6. Bounded startup rendezvous (`Ready(loop)` or `Failed(exc)`).
7. Non-blocking async context managers (`__aenter__`/`__aexit__` via `trio.to_thread.run_sync`).
8. Proper lifecycle degradation vs configured-disabled vs running vs stopped.
9. Strict boolean environment parsing (reject typos like `ture` with `ValueError`).
10. Semantic boundary validation in `ProxyCache.__init__`.
11. Clean architectural split (DNS caching -> Cashews; rate limiting -> owned protocol; circuit breaking -> Trio-native state machine).

## Tranches (Hierarchical Delegation Pipeline)

- [x] **Tranche 1: Configuration, Validation & Lifecycle State Tracking**
  - Scope: `src/trio_http_proxy/cache.py` (env parsing, `__init__` validation, lifecycle state attributes)
  - Goal: Strict boolean env parsing, input validation in `ProxyCache.__init__`, distinct lifecycle states (`configured_disabled`, `running`, `degraded`, `stopped`).
  - Deps: None
  - Accept: Unit tests for env parsing errors, constructor validation, state transitions.

- [x] **Tranche 2: Bounded Bridge Rendezvous & Non-blocking Trio Async Context Manager**
  - Scope: `src/trio_http_proxy/cache.py` (`start`, `stop`, `__aenter__`, `__aexit__`, `run` cancellation docs)
  - Goal: Thread startup uses bounded rendezvous queue returning `Ready(loop)` or `Failed(exc)`. `__aenter__` and `__aexit__` offload blocking calls to `trio.to_thread.run_sync`. Documented cancellation semantics in `run`.
  - Deps: Tranche 1
  - Accept: Tests for startup failure timeout/exception propagation, async context manager execution without stalling Trio loop.

- [x] **Tranche 3: Trio-Native Circuit Breaker State Machine**
  - Scope: `src/trio_http_proxy/cache.py`, `tests/`
  - Goal: Implement explicit Trio-side circuit breaker state machine (`DestinationCircuitBreaker` / `CircuitBreakerManager` / states `CLOSED`, `OPEN`, `HALF_OPEN`). True gating, half-open single-probe discipline, zero private Cashews key layout dependence.
  - Deps: Tranche 1
  - Accept: Tests verifying state transitions, half-open probe gating, recovery on success, cool-off on failure.

- [x] **Tranche 4: Owned Rate Limiting, Namespaced Clear, and Failure Propagation**
  - Scope: `src/trio_http_proxy/cache.py`
  - Goal: Configure backend with `suppress=False`. Implement owned rate-limiting using public `incr`/`get_expire`. Namespace all keys under `self.namespace`. Safe `clear()` deleting only `self.namespace:*`. `ProxyCache` owns fail-open vs fail-closed.
  - Deps: Tranche 1, Tranche 2
  - Accept: Tests for rate limit expiry, safe namespace clearing, failure propagation and fail-open/closed behavior.

- [x] **Tranche 5: Full Integration, Test Suite Updates & Mcpls Diagnostics**
  - Scope: `tests/`, `src/trio_http_proxy/`
  - Goal: Update existing tests for new invariants, ensure 100% test pass rate, verify Ruff linting, Pyright type check, and Mcpls diagnostics clean.
  - Deps: Tranches 1, 2, 3, 4
  - Accept: `pytest` passes, `ruff check` passes, `pyright` passes, `mcpls` diagnostics clean.

## Status
All 5 tranches complete. All 144 tests passing. Ruff and Pyright completely clean. Mcpls diagnostics reporting 0 issues.
