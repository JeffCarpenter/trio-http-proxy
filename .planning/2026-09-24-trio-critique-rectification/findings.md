# Research & Architectural Findings: Trio Critique Rectification

## 1. Context & Architectural Goals
Following a rigorous code review by Leslie Lamport, Martin Fowler, and Guido van Rossum, several critical design smells, latent concurrency vulnerabilities, and API ergonomics issues in `trio_http_proxy` were identified. This refactor rectifies these concerns both *in letter* (fixing the specific lines and anti-patterns) and *in spirit* (deep architectural integrity, formal invariant modeling, and idiomatic Python/Trio design).

## 2. Detailed Findings & Rectifications

### A. Leslie Lamport: Concurrency, Invariants & Circuit Breaker Formal State Model
1. **Probe Lease Invalidation & Epoch Monotonicity in `DestinationCircuitBreaker`:**
   - *Problem:* In `HALF_OPEN`, if a probe permit times out (`now - self._probe_started_at >= self.ttl`), `acquire_permit()` admits a new probe lease, but `self.epoch` was not incremented, and `record_result` used `if self._active_probe_id is not None and permit.permit_id != self._active_probe_id: return`. If `self._active_probe_id` became `None` (or if an aborted/timed-out probe returned later during another probe cycle), an expired/delayed probe could transition state or corrupt accounting.
   - *Rectification:*
     - Formalize Invariant I1 (At most one valid probe lease per epoch in HALF_OPEN) and Invariant I2 (Epoch Monotonicity).
     - When a probe lease expires/times out in `HALF_OPEN`, increment `self.epoch += 1` (or explicitly revoke prior probe lease). This cryptographically/sequentially invalidates any delayed probe permit in flight.
     - In `record_result`, enforce strict identity: `if self._active_probe_id is None or permit.permit_id != self._active_probe_id: return`. An inactive or expired probe ID can never mutate circuit breaker state.
     - Document the formal state machine, state invariants, and transitions in the module docstring.

2. **Synchronization Model:**
   - *Problem:* `DestinationCircuitBreaker` relied on `threading.Lock()`.
   - *Clarification & Hardening:* `DestinationCircuitBreaker` is purely CPU-bound (in-memory state machine, O(1) sliding window queue, timestamp math). While it runs on the Trio event loop, `threading.Lock` provides thread safety in case `ProxyCache` interacts across thread boundaries (e.g. background Redis/Cashews bridge). Ensure that no lock is ever held across async yield points (`await`), document this explicitly, and ensure all methods remain strictly non-blocking.

### B. Guido van Rossum: Pythonic Idioms, Typing, Sentinel Flattening & Non-Silent Logging
1. **Public API Signature Pollution & Weak Typing in `http_proxy`:**
   - *Problem:* `http_proxy` exposed `_nextid: Any = _conn_ids.__next__` and `_connect: Any = trio.open_tcp_stream`. This leaked private implementation details and mutable iterator methods into the public API signature, and typed them as `Any`.
   - *Rectification:*
     - Replace signature defaults with `_nextid: Callable[[], int] | None = None` and `_connect: Callable[..., Awaitable[trio.abc.Stream]] | None = None`.
     - Resolve defaults internally inside `http_proxy`.
     - Maintain full backward compatibility for callers passing positional or keyword arguments while presenting clean, strongly-typed signatures.

2. **Silent Exception Swallowing in `proxy.py`:**
   - *Problem:* In `proxy.py`, handshake failures caught `(ValueError, h11.ProtocolError, OSError, trio.TooSlowError, CircuitBreakerOpen)` and exited with a bare `return`. No logs were produced. Operators in production could not diagnose why connections were abruptly dropped.
   - *Rectification:*
     - Replace the bare `return` with structured, distinct logging:
       - `CircuitBreakerOpen`: `logger.warning("Circuit breaker open for %s: %s; closing connection", desthost, exc)`
       - `trio.TooSlowError`: `logger.warning("Handshake timed out after %ss: %s", timeout, exc)`
       - `h11.ProtocolError`: `logger.warning("HTTP protocol error during handshake: %s", exc)`
       - `(ValueError, OSError)`: `logger.warning("Network or configuration error during handshake: %s", exc)`
     - Preserve Zen of Python: "Errors should never pass silently. Unless explicitly silenced."

3. **Sentinel Object & Limiter Ternary Simplification:**
   - *Problem:* `_DefaultLimiterSentinel` defined custom `__repr__` and `__bool__` returning `False`, requiring a nested ternary expression in `http_proxy`.
   - *Rectification:*
     - Introduce a clean `resolve_capacity_limiter()` helper function that flattens the resolution logic and types cleanly.

### C. Martin Fowler: Single Source of Truth & Architecture Integrity
1. **Dual-State Strangler Pattern in Connection Context:**
   - *Problem:* `ConnectionContext` updated 6 disparate ContextVars (`CV_CLIENT_STREAM`, `CV_DEST_STREAM`, `CV_CONN_ID`, `CV_DESTHOST`, `CV_PIPE_FROM`, `CV_PROXY_CACHE`), creating two parallel sources of truth prone to divergence.
   - *Rectification:*
     - `ConnectionContext` is established as the canonical *single source of truth*.
     - `_connection_context()` and logging formatters prioritize the active `ConnectionContext.current()` directly, treating ambient `CV_*` strictly as legacy fallbacks.
     - Context property synchronization ensures that changes to `ConnectionContext` are atomic.
