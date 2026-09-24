# Progress: Rectifying the Trio's Concerns in Letter and in Spirit

## Completed Work
- Completed all phases of the refactor rectifying the concerns raised by Leslie Lamport, Martin Fowler, and Guido van Rossum:
  - **Lamport (Concurrency & State Invariants):**
    - Enforced Invariant I2 (Epoch Monotonicity): probe lease timeout expiration now increments `epoch`, sequentially fencing off delayed or late-arriving probe completions.
    - Enforced Invariant I1 & I4 (Completion Identity): `record_result` in `HALF_OPEN` strictly rejects any permit where `self._active_probe_id is None` or `permit.permit_id != self._active_probe_id`.
    - Documented formal state invariants (I1: Mutual Exclusion of Probes, I2: Epoch Monotonicity, I3: Atomicity & Thread Safety, I4: Completion Identity) in `DestinationCircuitBreaker` docstring.
  - **Fowler (Single Source of Truth):**
    - `_connection_context()` now directly queries and returns `ctx.to_dict()` whenever `ConnectionContext.current()` is active, treating ambient `ContextVars` strictly as a fallback for legacy context-less callers.
    - Eliminated state drift between explicit connection parameters and ambient task-local storage.
  - **Guido (Pythonic Cleanliness & Visibility):**
    - Eliminated private parameter leaks and `Any` annotations from `http_proxy` and `tunnel`: signatures now use `_nextid: Callable[[], int] | None = None` and `_connect: Callable[..., Awaitable[Any]] | None = None` with internal default resolution.
    - Extracted clean, flat `resolve_capacity_limiter()` helper function, removing nested ternary contortions.
    - Replaced silent exception swallowing (`except (ValueError, ...): return`) in `http_proxy` with distinct, structured `logger.warning` messages for `CircuitBreakerOpen`, `trio.TooSlowError`, protocol errors (`h11.ProtocolError`, `ProxyProtocolError`, `RequestTooLargeError`), and network connection failures (`ValueError`, `OSError`).
- Added comprehensive unit and invariant test suite in `tests/test_trio_critique_rectification.py`.

## Test Results
| Test | Expected | Actual | Status |
|------|----------|--------|--------|
| Baseline test suite (`uv run pytest`) | 261 passed | 261 passed | PASS |
| Baseline pyright (`uv run pyright src/ tests/`) | 0 errors | 0 errors | PASS |
| Rectification test suite (`tests/test_trio_critique_rectification.py`) | 7 passed | 7 passed | PASS |
| Full pytest test suite (`uv run pytest`) | 268 passed | 268 passed | PASS |
| Full pyright audit (`uv run pyright src/ tests/`) | 0 errors | 0 errors | PASS |
