# Findings: Cache Architecture Refactor & Invariant Hardening

## Background & Problem Analysis

1. **Failure Policy Trustworthiness**:
   - `fail-closed` depends on Cashews surfacing backend failures as exceptions.
   - Cashews' Redis backend suppresses many errors by default via `suppress=True` (which uses `SafeRedis`).
   - Fix: Pass `suppress=False` to backend settings during setup. `ProxyCache` becomes the sole arbiter of failure policy (`fail-open` degrades gracefully; `fail-closed` raises `CacheBackendUnavailable`).

2. **Broad `clear()` Scope**:
   - `self.cache.clear()` calls Redis `FLUSHDB`, wiping all keys across the entire database.
   - Fix: Define `self.namespace = "trio_http_proxy"`. All keys are namespaced `f"{self.namespace}:{type}:{key}"`. `clear()` uses `self.cache.delete_match(f"{self.namespace}:*")`.

3. **Circuit Breaker Architecture**:
   - Currently, a dummy function `_circuit_probe` is decorated with `@c.circuit_breaker`, and `check_circuit` manually inspects undocumented keys `circuit_breaker:cb:{host}:{port}:open` and `:halfopen`.
   - Consequence: Connections leak through, no true gating, no half-open probe discipline, coupled to internal Cashews keys.
   - Fix: Replace Cashews circuit breaker with an explicit Trio-native state machine (`CLOSED`, `OPEN`, `HALF_OPEN`) with single-probe discipline during half-open state.

4. **Cashews Private Schema Coupling**:
   - Private key reconstruction (`circuit_breaker:cb:...`, `rate_limit:rl:...`).
   - Fix: Cleaner split:
     - DNS caching: Cashews decorator `@c(ttl=..., key=f"{namespace}:dns:{host}")`.
     - Rate limiting: Owned protocol using public `incr(key, expire)` and `get_expire(key)`.
     - Circuit breaking: Explicit Trio-native state machine.

5. **Weak Timeout/Cancellation**:
   - `Future.cancel()` does not guarantee coroutine termination or side effect rollback.
   - Fix: Explicitly document this in `run()` docstrings and boundedly await cancellation completion.

6. **Startup Deadlock**:
   - `self._ready.wait()` can hang forever if the thread fails before signaling.
   - Fix: Startup rendezvous using a thread-safe bounded `queue.Queue` carrying `(loop, None)` or `(None, exc)` with a timeout.

7. **Blocking Async Context Manager**:
   - `__aenter__` and `__aexit__` call blocking `self.start()` and `self.stop()`.
   - Fix: Offload blocking lifecycle methods to worker thread using `trio.to_thread.run_sync`.

8. **Degraded vs Configured-Disabled**:
   - Fail-open setup failure permanently mutated `self.enabled = False`.
   - Fix: Distinguish `self.enabled` (user configuration) from `self._degraded` (transient backend failure in fail-open mode) and lifecycle states `STOPPED`, `STARTING`, `RUNNING`, `STOPPING`.

9. **Permissive Boolean Parsing**:
   - Typo like `ture` silently parsed as `False`.
   - Fix: Accept explicit true `{"1", "true", "yes", "on"}` and false `{"0", "false", "no", "off"}`, reject everything else with `ValueError`.

10. **Semantic Boundary Validation**:
    - Validate invariants in `ProxyCache.__init__` directly (e.g. `1 <= circuit_breaker_errors_rate <= 99`, `circuit_breaker_min_calls >= 1`, `rate_limit >= 0`, `failure_policy in {"fail-open", "fail-closed"}`).
