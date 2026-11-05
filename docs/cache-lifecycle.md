# Cache lifecycle and bridge ownership

`ProxyCache` owns its backend and asyncio bridge thread. Backend health is separate
from lifecycle: a fail-open cache can remain `RUNNING` while degraded. Recovery
changes health without starting another bridge or changing lifecycle.

## Lifecycle

| State | Meaning | Next state |
| --- | --- | --- |
| `NEW` | No startup has been attempted. | `STARTING`, or `STOPPING` if stopped before use. |
| `STARTING` | Bridge publication, backend setup, and startup ping are in progress. | `RUNNING`, `STOPPING`, or `STOPPED` after an aborted bridge startup. |
| `RUNNING` | Cache operations are admitted, including degraded fail-open operation. A cache with backend features disabled needs no bridge thread. | `STOPPING` |
| `STOPPING` | New work and restart are rejected; cleanup may still be running. | `STOPPED` after cleanup succeeds. |
| `STOPPED` | Terminal state; the instance cannot restart. | None. |

`is_running` means **running and healthy**, not merely that a worker thread exists.
Use `lifecycle_state` and `is_degraded` to distinguish a degraded running cache from
a stopped cache.

For an established bridge, `STOPPED` is published only after its thread exits and
its event loop closes. If bridge startup aborts before acquiring a loop, the cache
becomes terminal immediately; a delayed worker observes the startup-abort event
and exits. A loop published at the startup deadline is explicitly stopped.

## Synchronization and shutdown

- The state lock protects lifecycle transitions and bridge submission. Submission
  checks admission and schedules the coroutine under the same lock. A rejected
  coroutine factory is not called; an already-created rejected coroutine is closed.
- Startup does not hold the state lock while waiting for the backend ping. A stop
  during that wait cannot be overwritten by startup completion. Timed-out pings
  receive a cancellation request.
- The shutdown lock gives one caller ownership of cleanup at a time. Waiting for
  ownership and joining the worker each have a two-second bound.
- One retained shutdown future cancels pending bridge tasks, finalizes async
  generators, closes the backend, shuts down the default executor, and stops the
  loop. The bridge worker then closes the loop.
- A join timeout or join exception leaves the cache in `STOPPING`. A later `stop()`
  retries the wait on the same cleanup operation; it does not cancel or restart
  backend close. Successful repeated `stop()` calls are no-ops.

Cancellation remains cooperative. A timeout requests cancellation; it does not
promise immediate rollback of backend effects. A blocked executor job or backend
cleanup can outlive a stop attempt, which reports failure rather than pretending
that resources have been released.

## Recorder compatibility

The proxy and deprecated `ConnectionResultBuilder` use one dispatcher. Modern
`record_connection_result(record)` takes precedence over legacy
`record_result(host, port, error, permit=...)`. Either method may return an awaitable
or complete synchronously.

Compatibility intentionally preserves one difference: direct records ignore a
missing or non-callable legacy recorder; the deprecated builder raises if a legacy
recorder is present but non-callable. A present but non-callable modern recorder
raises in both paths. Passing no cache bypasses recording without building a
legacy record. Deprecated-builder warnings and public re-exports are unchanged.

See [configuration](configuration.md) for failure-policy and timeout settings.
Regression coverage lives in `tests/test_cache_lifecycle_refactor.py`, the existing
cache invariant/security suites, and `tests/test_simplification_contracts.py`.
