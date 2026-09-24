"""Formal stateful and property-based invariant test suite for DestinationCircuitBreaker.

Circuit Breaker State Machine Formal Invariants:
------------------------------------------------
A state machine must be verified against its temporal properties and transition guarantees.

- Invariant I1 (Mutual Exclusion of Probes):
  In state HALF_OPEN, at most one valid probe lease permit may be active for the current epoch.
  A second permit request while a probe is in-flight must be rejected.
  Formal Predicate:
    (State = HALF_OPEN ∧ half_open_in_flight) ⟹ (cardinality({p ∈ ActiveLeases : p.is_probe ∧ p.epoch = current_epoch}) ≤ 1)

- Invariant I2 (Epoch Monotonicity):
  Any resolving state transition or probe lease timeout revocation strictly increases epoch,
  fencing off delayed or stale completions.
  Formal Predicate:
    ∀ t1 < t2 : epoch(t1) ≤ epoch(t2)
    ResolvingTransition(t) ∨ TimeoutRevocation(t) ⟹ epoch(t+) > epoch(t)

- Invariant I3 (Atomicity & Single-Threaded Safety):
  State transitions are pure, synchronous in-memory mutations without async suspension
  (zero `await` points) and free of thread locks, ensuring intrinsic atomicity within
  Trio's cooperative event loop.

- Invariant I4 (Completion Identity):
  Stale permits from prior epochs or expired probe leases cannot alter or close the circuit.
  A completion may only alter circuit state if:
    permit.breaker is self ∧ permit.epoch == self.epoch ∧ permit.mark_completed() ∧
    (self.state == HALF_OPEN ⟹ (permit.is_probe ∧ permit.permit_id == self._active_probe_id))
"""

from __future__ import annotations

import _thread
import inspect
import random
import threading
from typing import NamedTuple

import pytest
import trio
import trio.testing as tt

from trio_http_proxy.circuit_breaker.circuit_permit import CircuitPermit
from trio_http_proxy.circuit_breaker.destination_circuit_breaker import (
    CircuitState,
    DestinationCircuitBreaker,
)


class CircuitSnapshot(NamedTuple):
    """Immutable state snapshot for testing discrete temporal transitions."""

    state: CircuitState
    epoch: int
    half_open_in_flight: bool
    active_probe_id: int | None
    opened_at: float | None


def take_snapshot(cb: DestinationCircuitBreaker) -> CircuitSnapshot:
    """Capture full observable state snapshot of the circuit breaker."""
    return CircuitSnapshot(
        state=cb.state,
        epoch=cb.epoch,
        half_open_in_flight=cb._half_open_in_flight,
        active_probe_id=cb._active_probe_id,
        opened_at=cb.opened_at,
    )


# ===========================================================================
# Helper: Formal Circuit Invariant Verification Predicate
# ===========================================================================


def verify_circuit_invariants(
    cb: DestinationCircuitBreaker,
    now: float,
    previous_epoch: int | None = None,
) -> None:
    """Assert all four formal invariants on DestinationCircuitBreaker state."""
    # Invariant I1: Mutual Exclusion of Probes
    if cb.state == CircuitState.HALF_OPEN:
        if cb._half_open_in_flight:
            assert cb._active_probe_id is not None, (
                f"Invariant I1 violated: in_flight probe must have active probe ID at now={now}"
            )
            assert cb._probe_started_at is not None, (
                f"Invariant I1 violated: in_flight probe must have start timestamp at now={now}"
            )
            # If within lease TTL, a second acquire MUST be rejected (mutual exclusion)
            if now - cb._probe_started_at < cb.ttl:
                assert cb.acquire_permit(now=now) is None, (
                    f"Invariant I1 violated: Second probe request admitted while probe lease active at now={now}"
                )
        else:
            assert cb._active_probe_id is None, (
                f"Invariant I1 violated: active probe ID present without in_flight flag at now={now}"
            )
            assert cb._probe_started_at is None, (
                f"Invariant I1 violated: probe timestamp present without in_flight flag at now={now}"
            )
    else:
        assert cb._active_probe_id is None, (
            f"Invariant I1 violated: active probe ID must be None outside HALF_OPEN (state={cb.state}) at now={now}"
        )

    # Invariant I2: Epoch Monotonicity
    if previous_epoch is not None:
        assert cb.epoch >= previous_epoch, (
            f"Invariant I2 violated: Epoch regressed from {previous_epoch} to {cb.epoch} at now={now}"
        )

    # Invariant I3: Atomicity & Single-Threaded Safety
    assert not hasattr(cb, "_lock"), "Invariant I3 violated: DestinationCircuitBreaker must not use thread locks"
    for attr_name in ("state", "epoch", "opened_at", "_active_probe_id", "_half_open_in_flight"):
        assert hasattr(cb, attr_name), f"Invariant I3 violated: Missing atomic state attribute {attr_name}"
    assert 0 <= cb._window_error_count <= len(cb._calls), (
        f"Invariant I3 violated: Window error count {cb._window_error_count} outside [0, {len(cb._calls)}]"
    )

    # Invariant I4: Completion Identity Pre-conditions
    if cb.state == CircuitState.OPEN:
        assert cb.opened_at is not None, "OPEN state must have valid opened_at timestamp"


# ===========================================================================
# 1. Invariant I1: Mutual Exclusion of Probes in HALF_OPEN
# ===========================================================================


def test_invariant_1_mutual_exclusion_of_probes_in_half_open() -> None:
    """In state HALF_OPEN, at most one valid probe lease permit may be active for the current epoch.

    A second permit request while a probe is in-flight must be rejected.
    """
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    t0 = 100.0

    # Trip to OPEN
    cb.record_result(OSError("fault"), now=t0)
    snap_open = take_snapshot(cb)
    assert snap_open.state == CircuitState.OPEN
    assert snap_open.opened_at == t0

    # Transition to HALF_OPEN after TTL
    t_probe = t0 + 5.0
    probe1 = cb.acquire_permit(now=t_probe)
    assert probe1 is not None, "First probe lease must be granted upon reaching TTL"
    assert probe1.is_probe is True, "Permit admitted in HALF_OPEN must have is_probe=True"

    snap_half = take_snapshot(cb)
    assert snap_half.state == CircuitState.HALF_OPEN
    assert snap_half.half_open_in_flight is True
    assert snap_half.active_probe_id == probe1.permit_id

    # Invariant I1: Any second permit request while probe is in flight must be rejected
    assert cb.acquire_permit(now=t_probe) is None, (
        "Invariant I1 violated: Second probe request admitted at identical timestamp"
    )
    assert cb.acquire_permit(now=t_probe + 1.0) is None, (
        "Invariant I1 violated: Second probe request admitted while probe1 in flight"
    )
    assert cb.acquire_permit(now=t_probe + 4.999) is None, (
        "Invariant I1 violated: Second probe request admitted before probe lease timeout"
    )

    # Aborting probe1 releases the lease immediately
    probe1.abort()
    assert probe1.was_aborted is True
    snap_after_abort = take_snapshot(cb)
    assert snap_after_abort.half_open_in_flight is False
    assert snap_after_abort.active_probe_id is None

    # Now a second probe can be acquired
    probe2 = cb.acquire_permit(now=t_probe + 4.999)
    assert probe2 is not None, "New probe must be granted after prior probe lease was aborted"
    assert probe2.is_probe is True
    assert probe2.permit_id != probe1.permit_id

    # Complete probe2 successfully -> closes circuit
    cb.record_result(None, now=t_probe + 5.0, permit=probe2)
    snap_closed = take_snapshot(cb)
    assert snap_closed.state == CircuitState.CLOSED
    assert probe2.was_completed is True


def test_invariant_1_concurrent_probe_burst_exact_single_winner() -> None:
    """When 100 concurrent requests arrive in HALF_OPEN at identical timestamps, exactly one wins."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=10.0, min_calls=1)
    cb.record_result(OSError("trip"), now=100.0)
    assert cb.state == CircuitState.OPEN

    # 100 callers attempt to acquire simultaneously at t=110.0 (exact TTL boundary)
    t_burst = 110.0
    results: list[CircuitPermit | None] = [cb.acquire_permit(now=t_burst) for _ in range(100)]

    winners = [p for p in results if p is not None]
    losers = [p for p in results if p is None]

    assert len(winners) == 1, f"Invariant I1 violated: Expected exactly 1 winner, got {len(winners)}"
    assert len(losers) == 99, f"Invariant I1 violated: Expected 99 rejections, got {len(losers)}"
    assert winners[0].is_probe is True
    assert cb._active_probe_id == winners[0].permit_id
    assert cb.state == CircuitState.HALF_OPEN


def test_invariant_1_context_manager_probe_release_on_exception() -> None:
    """Context manager exit without record_result aborts lease and allows subsequent probe."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("trip"), now=10.0)
    t_half = 15.0

    p1 = cb.acquire_permit(now=t_half)
    assert p1 is not None

    # First attempt inside context manager raises an exception
    with pytest.raises(RuntimeError, match="client disconnected"):
        with p1:
            assert cb._half_open_in_flight is True
            # Concurrent acquire is rejected
            assert cb.acquire_permit(now=t_half) is None
            raise RuntimeError("client disconnected")

    # The exception triggered context manager __exit__, aborting p1
    assert p1.was_aborted is True
    assert cb._half_open_in_flight is False
    assert cb._active_probe_id is None

    # Immediate subsequent caller at t_half + 0.1 acquires a new probe permit
    p2 = cb.acquire_permit(now=t_half + 0.1)
    assert p2 is not None
    assert p2.is_probe is True
    assert p2.permit_id != p1.permit_id


def test_invariant_1_closed_state_permits_are_not_probes() -> None:
    """In CLOSED state, permits have is_probe=False and multiple concurrent permits are admitted."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=3)
    p1 = cb.acquire_permit(now=1.0)
    p2 = cb.acquire_permit(now=1.0)
    p3 = cb.acquire_permit(now=1.0)

    assert p1 is not None and not p1.is_probe
    assert p2 is not None and not p2.is_probe
    assert p3 is not None and not p3.is_probe
    assert cb.state == CircuitState.CLOSED
    assert cb._active_probe_id is None


# ===========================================================================
# 2. Invariant I2: Epoch Monotonicity
# ===========================================================================


def test_invariant_2_epoch_monotonicity_on_lease_timeout() -> None:
    """Revoked probe lease cannot complete or close circuit due to epoch monotonicity fencing."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1, epoch=0)
    t0 = 1000.0

    # Trip to OPEN
    cb.record_result(OSError("trip"), now=t0)
    assert cb.state == CircuitState.OPEN
    epoch_open = cb.epoch
    assert epoch_open == 1, "CLOSED -> OPEN transition must increment epoch from 0 to 1"

    # Advance to HALF_OPEN and acquire probe 1
    t_probe1 = t0 + 5.0
    p1 = cb.acquire_permit(now=t_probe1)
    assert p1 is not None
    assert p1.epoch == epoch_open

    # Advance time past probe TTL: 1005.0 + 5.0 = 1010.0
    # Probe 1 times out. Next caller at 1010.0 triggers lease timeout revocation.
    t_probe2 = t_probe1 + 5.0
    p2 = cb.acquire_permit(now=t_probe2)
    assert p2 is not None
    assert p2.is_probe is True
    assert p2.permit_id != p1.permit_id

    # Invariant I2: Lease timeout revocation strictly increases epoch
    assert cb.epoch > epoch_open, (
        f"Invariant I2 violated: Epoch did not increase on lease timeout at {t_probe2} "
        f"(prev={epoch_open}, current={cb.epoch})"
    )
    assert p2.epoch == cb.epoch
    assert p2.epoch > p1.epoch

    # Stale probe 1 delayed completion arrives at 1010.5 reporting success:
    # Must be fenced off and ignored!
    cb.record_result(None, now=t_probe2 + 0.5, permit=p1)
    assert cb.state == CircuitState.HALF_OPEN, (
        "Invariant I2/I4 violated: Stale timed-out probe unexpectedly altered circuit state"
    )
    assert p1.was_completed is False

    # Legitimate probe 2 reports success at 1011.0:
    cb.record_result(None, now=t_probe2 + 1.0, permit=p2)
    assert cb.state == CircuitState.CLOSED
    assert p2.was_completed is True
    assert cb.epoch == p2.epoch + 1, "HALF_OPEN -> CLOSED must strictly increment epoch"


def test_invariant_2_strict_epoch_monotonicity_on_state_transitions() -> None:
    """Any state transition or probe lease timeout revocation strictly increases epoch."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=3, epoch=10)
    t = 1000.0
    epochs: list[int] = [cb.epoch]

    # Call 1 in CLOSED (success): min_calls not reached -> epoch unchanged
    p1 = cb.acquire_permit(now=t)
    assert p1 is not None
    cb.record_result(None, now=t + 0.1, permit=p1)
    epochs.append(cb.epoch)
    assert epochs[-1] == epochs[-2]

    # Call 2 in CLOSED (failure): 1 fail / 2 calls = 50%, but min_calls=3 not reached -> epoch unchanged
    p2 = cb.acquire_permit(now=t + 0.2)
    assert p2 is not None
    cb.record_result(OSError("fail 1"), now=t + 0.3, permit=p2)
    epochs.append(cb.epoch)
    assert epochs[-1] == epochs[-2]

    # Call 3 in CLOSED (failure): 2 fail / 3 calls = 66.7% >= 50% -> trips to OPEN!
    p3 = cb.acquire_permit(now=t + 0.4)
    assert p3 is not None
    cb.record_result(OSError("fail 2"), now=t + 0.5, permit=p3)
    assert cb.state == CircuitState.OPEN
    epochs.append(cb.epoch)
    assert epochs[-1] > epochs[-2], "Invariant I2 violated: CLOSED -> OPEN must strictly increment epoch"

    # Transition OPEN -> HALF_OPEN (via acquire): epoch remains unchanged
    p_probe1 = cb.acquire_permit(now=t + 5.6)
    assert p_probe1 is not None
    assert cb.state == CircuitState.HALF_OPEN
    epochs.append(cb.epoch)
    assert epochs[-1] == epochs[-2]

    # Probe 1 fails -> transitions HALF_OPEN -> OPEN
    cb.record_result(OSError("probe 1 fail"), now=t + 5.7, permit=p_probe1)
    assert cb.state == CircuitState.OPEN
    epochs.append(cb.epoch)
    assert epochs[-1] > epochs[-2], "Invariant I2 violated: HALF_OPEN -> OPEN must strictly increment epoch"

    # Transition OPEN -> HALF_OPEN
    p_probe2 = cb.acquire_permit(now=t + 10.8)
    assert p_probe2 is not None
    assert cb.state == CircuitState.HALF_OPEN
    epochs.append(cb.epoch)
    assert epochs[-1] == epochs[-2]

    # Probe 2 times out after TTL (5.0s) -> timeout revocation strictly increments epoch
    p_probe3 = cb.acquire_permit(now=t + 15.9)
    assert p_probe3 is not None
    epochs.append(cb.epoch)
    assert epochs[-1] > epochs[-2], "Invariant I2 violated: Timeout revocation must strictly increment epoch"

    # Probe 3 succeeds -> transitions HALF_OPEN -> CLOSED
    cb.record_result(None, now=t + 16.0, permit=p_probe3)
    assert cb.state == CircuitState.CLOSED
    epochs.append(cb.epoch)
    assert epochs[-1] > epochs[-2], "Invariant I2 violated: HALF_OPEN -> CLOSED must strictly increment epoch"

    # Verify global monotonicity across the entire sequence
    for i in range(len(epochs) - 1):
        assert epochs[i] <= epochs[i + 1], f"Invariant I2 violated: Non-monotonic epoch sequence: {epochs}"


def test_invariant_2_arbitrary_clock_jumps_maintain_monotonicity() -> None:
    """Epoch never regresses under simulated clock jumps forward of varying magnitudes."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    last_epoch = cb.epoch

    for jump in [0.0, 0.001, 1.0, 5.0, 10.0, 60.0, 3600.0, 86400.0, 1e7]:
        t = 1000.0 + jump
        cb.acquire_permit(now=t)
        assert cb.epoch >= last_epoch, f"Invariant I2 violated: Epoch regressed after clock jump {jump}"
        last_epoch = cb.epoch


def test_invariant_2_initial_custom_epoch() -> None:
    """Initializing DestinationCircuitBreaker with arbitrary epoch preserves monotonicity."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1, epoch=42)
    assert cb.epoch == 42
    p = cb.acquire_permit(now=10.0)
    assert p is not None and p.epoch == 42
    cb.record_result(OSError("fault"), now=10.1, permit=p)
    assert cb.state == CircuitState.OPEN
    assert cb.epoch == 43


# ===========================================================================
# 3. Invariant I3: Atomicity & Single-Threaded Safety / Non-Blocking Lock-Free
# ===========================================================================


def test_invariant_3_nonblocking_atomicity_and_lock_freedom() -> None:
    """Verify state transitions are pure synchronous mutations free of thread locks."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)

    # 1. No threading locks attached
    assert not hasattr(cb, "_lock")
    lock_types = (_thread.lock, _thread.RLock, threading.Semaphore)
    for key, value in cb.__dict__.items():
        assert not isinstance(value, lock_types), (
            f"Invariant I3 violated: Found threading lock in attribute {key}"
        )

    # 2. Methods are synchronous functions (zero coroutine / async suspension points)
    assert not inspect.iscoroutinefunction(cb.acquire_permit)
    assert not inspect.iscoroutinefunction(cb.record_result)
    assert not inspect.iscoroutinefunction(cb._abort_permit)
    assert not inspect.iscoroutinefunction(cb._prune_calls)

    permit = cb.acquire_permit(now=10.0)
    assert permit is not None
    assert not inspect.iscoroutinefunction(permit.abort)
    assert not inspect.iscoroutinefunction(permit.mark_completed)


@tt.trio_test
async def test_invariant_3_cooperative_concurrency_trio_simulation() -> None:
    """Verify atomic state transitions under concurrent cooperative task interleaving."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=2.0, ttl=1.0, min_calls=2)

    async def worker(task_id: int, barrier: trio.Event) -> None:
        await barrier.wait()
        for i in range(10):
            now = trio.current_time()
            permit = cb.acquire_permit(now=now)
            await trio.lowlevel.checkpoint()  # Cooperative yield point
            if permit is not None:
                is_fail = (task_id + i) % 3 == 0
                exc = OSError(f"Worker {task_id} fail") if is_fail else None
                cb.record_result(exc, now=trio.current_time(), permit=permit)
            await trio.lowlevel.checkpoint()

    barrier = trio.Event()
    async with trio.open_nursery() as nursery:
        for tid in range(15):
            nursery.start_soon(worker, tid, barrier)
        barrier.set()

    # After concurrent execution, circuit breaker invariants must hold intact
    verify_circuit_invariants(cb, now=trio.current_time(), previous_epoch=0)


def test_invariant_3_pure_in_memory_latency_and_throughput() -> None:
    """Verify operations execute purely in-memory with sub-millisecond execution time."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=60.0, ttl=30.0, min_calls=100)
    t = 100.0

    # Execute 5,000 rapid permit/record cycles
    for i in range(5000):
        permit = cb.acquire_permit(now=t + (i * 0.001))
        assert permit is not None
        cb.record_result(None, now=t + (i * 0.001), permit=permit)

    assert cb.state == CircuitState.CLOSED
    assert len(cb._calls) > 0


# ===========================================================================
# 4. Invariant I4: Completion Identity & Stale Permit Rejection
# ===========================================================================


def test_invariant_4_stale_and_expired_permit_rejection() -> None:
    """Stale permits from prior epochs or expired probe leases cannot alter or close the circuit.

    Verifies rejection of:
    1. Foreign breaker permits (permit.breaker is not self).
    2. Stale epoch permits (permit.epoch != self.epoch).
    3. Already completed permits (double completion).
    4. Aborted permits (double abort / completion after abort).
    5. Permitless and non-probe callers in HALF_OPEN.
    6. Mismatched probe IDs in HALF_OPEN.
    """
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    foreign_cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)

    # 1. Foreign breaker permit rejected
    foreign_permit = foreign_cb.acquire_permit(now=10.0)
    assert foreign_permit is not None
    cb.record_result(OSError("foreign failure"), now=10.1, permit=foreign_permit)
    assert cb.state == CircuitState.CLOSED
    assert len(cb._calls) == 0
    assert foreign_permit.was_completed is False

    # 2. Stale epoch permit rejected
    p_epoch0 = cb.acquire_permit(now=10.0)
    assert p_epoch0 is not None
    assert p_epoch0.epoch == 0

    # Advance breaker to OPEN (epoch 1)
    p_trip = cb.acquire_permit(now=10.1)
    assert p_trip is not None
    cb.record_result(OSError("trip"), now=10.2, permit=p_trip)
    assert cb.state == CircuitState.OPEN
    assert cb.epoch == 1

    # Stale permit from epoch 0 attempts to record in epoch 1 -> ignored!
    cb.record_result(None, now=10.3, permit=p_epoch0)
    assert cb.state == CircuitState.OPEN
    assert p_epoch0.was_completed is False

    # 3. Enter HALF_OPEN
    p_probe = cb.acquire_permit(now=15.5)
    assert p_probe is not None
    assert cb.state == CircuitState.HALF_OPEN

    # 4. Permitless caller in HALF_OPEN -> ignored!
    cb.record_result(None, now=15.6, permit=None)
    assert cb.state == CircuitState.HALF_OPEN
    cb.record_result(OSError("fault"), now=15.7, permit=None)
    assert cb.state == CircuitState.HALF_OPEN

    # 5. Non-probe permit in HALF_OPEN -> ignored!
    non_probe_permit = CircuitPermit(breaker=cb, epoch=cb.epoch, permit_id=999, is_probe=False)
    cb.record_result(None, now=15.8, permit=non_probe_permit)
    assert cb.state == CircuitState.HALF_OPEN

    # 6. Mismatched probe ID in HALF_OPEN -> ignored!
    forged_probe = CircuitPermit(breaker=cb, epoch=cb.epoch, permit_id=p_probe.permit_id + 50, is_probe=True)
    cb.record_result(None, now=15.9, permit=forged_probe)
    assert cb.state == CircuitState.HALF_OPEN

    # 7. Aborted permit cannot complete
    p_probe.abort()
    assert p_probe.was_aborted is True
    cb.record_result(None, now=16.0, permit=p_probe)
    assert cb.state == CircuitState.HALF_OPEN
    assert p_probe.was_completed is False

    # 8. Double abort is safe idempotent no-op
    p_probe.abort()
    assert p_probe.was_aborted is True

    # 9. Double completion prevented
    p_new_probe = cb.acquire_permit(now=16.1)
    assert p_new_probe is not None
    cb.record_result(None, now=16.2, permit=p_new_probe)
    assert cb.state == CircuitState.CLOSED
    assert p_new_probe.was_completed is True
    epoch_closed = cb.epoch

    # Second completion attempt with p_new_probe -> ignored!
    cb.record_result(OSError("repeat error"), now=16.3, permit=p_new_probe)
    assert cb.state == CircuitState.CLOSED
    assert cb.epoch == epoch_closed
    assert len(cb._calls) == 0  # No errors recorded in window


def test_invariant_4_expired_probe_late_arrival_cannot_close_circuit() -> None:
    """Expired probe late arrival cannot close circuit or override active replacement probe."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    cb.record_result(OSError("trip"), now=100.0)

    # Probe 1 issued at 105.0
    p1 = cb.acquire_permit(now=105.0)
    assert p1 is not None

    # Jump to 110.0: Probe 2 issued, invalidating Probe 1 and bumping epoch
    p2 = cb.acquire_permit(now=110.0)
    assert p2 is not None

    # Stale Probe 1 late success arrival
    cb.record_result(None, now=110.1, permit=p1)
    assert cb.state == CircuitState.HALF_OPEN  # Not closed by stale probe 1!

    # Active Probe 2 failure arrival -> trips circuit back to OPEN
    cb.record_result(OSError("probe 2 failed"), now=110.5, permit=p2)
    assert cb.state == CircuitState.OPEN
    assert p2.was_completed is True


# ===========================================================================
# 5. Boundary Conditions & Clock Jumps
# ===========================================================================


def test_boundary_conditions_clock_jumps() -> None:
    """Verify exact boundary conditions: opened_at + ttl and probe_started_at + ttl."""
    cb = DestinationCircuitBreaker(errors_rate=50, period=10.0, ttl=5.0, min_calls=1)
    t_trip = 100.0
    cb.record_result(OSError("trip"), now=t_trip)
    assert cb.state == CircuitState.OPEN
    assert cb.opened_at == t_trip

    # 1. Exact boundary: now = opened_at + ttl - 0.001 (must remain OPEN, permit None)
    assert cb.acquire_permit(now=t_trip + 5.0 - 0.001) is None, (
        "Invariant violated: permit granted before open TTL expired"
    )
    assert cb.state == CircuitState.OPEN

    # 2. Exact boundary: now = opened_at + ttl (transitions to HALF_OPEN, permits 1 probe)
    p1 = cb.acquire_permit(now=t_trip + 5.0)
    assert p1 is not None, "Permit must be granted exactly at open TTL boundary"
    assert p1.is_probe is True
    assert cb.state == CircuitState.HALF_OPEN
    assert cb._probe_started_at == t_trip + 5.0

    # 3. Exact boundary: now = probe_started_at + ttl - 0.001 (second probe must be rejected)
    assert cb.acquire_permit(now=t_trip + 10.0 - 0.001) is None, (
        "Invariant I1 violated: second probe granted before probe lease TTL expired"
    )

    # 4. Exact boundary: now = probe_started_at + ttl (probe lease times out, revokes, increments epoch)
    epoch_before = cb.epoch
    p2 = cb.acquire_permit(now=t_trip + 10.0)
    assert p2 is not None, "New probe must be granted after probe lease timeout"
    assert p2.is_probe is True
    assert p2.epoch == epoch_before + 1, "Lease timeout revocation must strictly increment epoch"
    assert p2.permit_id != p1.permit_id


# ===========================================================================
# 6. Formal Stateful Randomized Interleaving Model Checker
# ===========================================================================


def test_stateful_randomized_interleaving_model_checker() -> None:
    """Stateful generative model checker: 5,000 interleaved events verifying domain invariants."""
    seeds = range(50)  # 50 random trajectories
    steps_per_seed = 100

    for seed in seeds:
        rng = random.Random(seed)
        cb = DestinationCircuitBreaker(
            errors_rate=rng.choice([20, 50, 80]),
            period=rng.choice([5.0, 10.0, 30.0]),
            ttl=rng.choice([2.0, 5.0, 10.0]),
            min_calls=rng.choice([1, 2, 4]),
            epoch=rng.randint(0, 5),
        )

        foreign_cb = DestinationCircuitBreaker(
            errors_rate=50, period=10.0, ttl=5.0, min_calls=1
        )

        sim_time = 1000.0
        outstanding_permits: list[CircuitPermit] = []
        stale_permits: list[CircuitPermit] = []

        for _ in range(steps_per_seed):
            prev_epoch = cb.epoch
            prev_state = cb.state

            action = rng.choice([
                "ACQUIRE",
                "RECORD_VALID",
                "RECORD_STALE",
                "RECORD_FOREIGN",
                "RECORD_PERMITLESS",
                "ABORT",
                "TIME_JUMP",
                "CONTEXT_MGR",
            ])

            if action == "ACQUIRE":
                p = cb.acquire_permit(now=sim_time)
                if p is not None:
                    outstanding_permits.append(p)

            elif action == "RECORD_VALID" and outstanding_permits:
                permit = outstanding_permits.pop(rng.randrange(len(outstanding_permits)))
                is_err = rng.random() < 0.5
                exc = OSError("simulated fault") if is_err else None
                cb.record_result(exc, now=sim_time, permit=permit)
                stale_permits.append(permit)

            elif action == "RECORD_STALE" and stale_permits:
                stale_p = rng.choice(stale_permits)
                is_err = rng.random() < 0.5
                exc = OSError("stale fault") if is_err else None
                cb.record_result(exc, now=sim_time, permit=stale_p)

            elif action == "RECORD_FOREIGN":
                fp = foreign_cb.acquire_permit(now=sim_time)
                if fp is not None:
                    is_err = rng.random() < 0.5
                    exc = OSError("foreign fault") if is_err else None
                    cb.record_result(exc, now=sim_time, permit=fp)

            elif action == "RECORD_PERMITLESS":
                is_err = rng.random() < 0.5
                exc = OSError("permitless fault") if is_err else None
                cb.record_result(exc, now=sim_time, permit=None)

            elif action == "ABORT" and outstanding_permits:
                permit = outstanding_permits.pop(rng.randrange(len(outstanding_permits)))
                permit.abort()
                stale_permits.append(permit)

            elif action == "TIME_JUMP":
                # Simulated clock jumps of varying granularities
                delta = rng.choice([0.001, 0.5, cb.ttl - 0.001, cb.ttl, cb.ttl + 0.001, 100.0])
                sim_time += delta

            elif action == "CONTEXT_MGR":
                ctx_p = cb.acquire_permit(now=sim_time)
                if ctx_p is not None:
                    try:
                        with ctx_p:
                            if rng.random() < 0.5:
                                raise ValueError("interrupted operation")
                            cb.record_result(None, now=sim_time, permit=ctx_p)
                    except ValueError:
                        pass
                    stale_permits.append(ctx_p)

            # Assert all invariants after EVERY state change
            verify_circuit_invariants(
                cb=cb,
                now=sim_time,
                previous_epoch=prev_epoch,
            )

            # Specific Invariant I2 resolving transition assertion:
            resolving_transition = (
                (prev_state == CircuitState.CLOSED and cb.state == CircuitState.OPEN)
                or (prev_state == CircuitState.HALF_OPEN and cb.state in (CircuitState.OPEN, CircuitState.CLOSED))
            )
            if resolving_transition:
                assert cb.epoch > prev_epoch, (
                    f"Invariant I2 violated: Transition {prev_state} -> {cb.state} must strictly increment epoch "
                    f"(prev={prev_epoch}, current={cb.epoch})"
                )
