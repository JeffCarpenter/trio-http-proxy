# Task Plan: Rectifying the Trio's Concerns in Letter and in Spirit

## Goal
Rigorously refactor `trio_http_proxy` to address the core architectural, concurrency, API design, and error handling concerns raised by Leslie Lamport, Martin Fowler, and Guido van Rossum, both in letter and in spirit.

## Next Step
Complete. All phases implemented, verified via TDD unit tests, full pytest suite passing (268 tests), and zero pyright errors.

## Current Phase
Complete

## Phases

### Phase 1: Architecture & Plan Specification
- [x] Analyze critique points: Lamport (circuit breaker invariants, probe race, epoch monotonicity), Fowler (dual-state strangler pattern, single source of truth), Guido (public API parameters, sentinels, swallowed exceptions)
- [x] Document in findings.md
- [x] Initialize task_plan.md and progress.md
- [x] Point `.planning/.active_plan` to `2026-09-24-trio-critique-rectification`
- **Status:** complete

### Phase 2: Formal Concurrency & State Machine Hardening (Lamport)
- [x] Enforce monotonic epoch increments on probe lease timeout in `DestinationCircuitBreaker`
- [x] Tighten probe permit identity verification in `record_result` (`self._active_probe_id is None or permit.permit_id != self._active_probe_id`)
- [x] Clarify and document formal invariants (I1: mutual exclusion of probes, I2: epoch monotonicity, I3: critical section atomicity, I4: completion identity) in docstrings
- **Status:** complete

### Phase 3: Clean API, Type Safety & Sentinel Simplification (Guido)
- [x] Clean `http_proxy` signature in `proxy.py`: type `_nextid: Callable[[], int] | None = None` and `_connect: Callable[..., Awaitable[Any]] | None = None` with internal resolution
- [x] Simplify `_DEFAULT_LIMITER` and introduce clean `resolve_capacity_limiter` helper
- [x] Replace silent exception swallowing (`except (ValueError, ...): return`) in `proxy.py` with structured, distinct logging per error condition
- **Status:** complete

### Phase 4: Single Source of Truth for Connection State (Fowler)
- [x] Consolidate connection state in `ConnectionContext`
- [x] Prioritize active `ConnectionContext.current()` in `_connection_context()` and logging formatters
- [x] Ensure seamless synchronization without risk of state drift
- **Status:** complete

### Phase 5: Verification, TDD & Regression Testing
- [x] Add unit tests in `tests/test_trio_critique_rectification.py` verifying:
  - Probe lease timeout invalidation & epoch monotonicity in `DestinationCircuitBreaker`
  - Handshake failure logging in `proxy.py`
  - Clean `http_proxy` typing & parameter defaults
  - Single source of truth in `ConnectionContext`
- [x] Run full pytest test suite (268 passed)
- [x] Run pyright across `src/` and `tests/` (0 errors)
- **Status:** complete
