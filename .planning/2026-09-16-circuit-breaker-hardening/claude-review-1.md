# Review: HEAD (0da1e1e) "harden admission protocol, leases, and failure boundaries (Phase 2.5)"

Second pass (review-1), done independently before reading `claude-review-0.md`
in full detail (I read it afterward to cross-check). Written incrementally.
Scope: `git diff HEAD~1 HEAD` — `src/trio_http_proxy/cache.py`,
`src/trio_http_proxy/proxy.py`, `tests/test_cache_invariants.py`.

Note: LSP tooling (`cclsp`/`mcpls`) requested by the user was not actually
reachable in this session (ToolSearch found no matching deferred tools for
either); fell back to Read/Bash/grep/python for navigation.

## Blockers

### 1. Permit protocol never wired into ProxyCache (confirms review-0's blocker)

`src/trio_http_proxy/cache.py:941-956`. Independently re-derived by reading
the full file (not just the diff hunks): `ProxyCache.check_circuit()` still
calls `self._circuit_manager.can_execute(host, port)` and always returns
`None` (never calls `acquire_permit()`, never returns a `CircuitPermit`).
`ProxyCache.record_connection_result()` still has signature
`(self, host, port, exc=None)` — no `permit` parameter at all.

`proxy.py:565`: `permit = await proxy_cache.check_circuit(...)` — so `permit`
is always `None` in the real (non-test-double) path. `proxy.py:493-503`
(`_record_connection_result`) calls `record_connection_result(host, port,
exc, permit=permit)`, hits the resulting `TypeError` (unexpected kwarg
against the real `ProxyCache`), and silently retries without `permit`. So
`CircuitBreakerManager.record_result(..., permit=None)` is what actually runs
end to end.

Per the new `DestinationCircuitBreaker.record_result` (cache.py:342-350): in
`HALF_OPEN`, `if permit is None or not permit.is_probe: return` drops the
call with no state change. Combined with `can_execute()`'s own TTL-based
dropped-probe recovery (cache.py:290-299, which self-heals
`_half_open_in_flight` after `ttl` seconds regardless of any permit), the net
runtime effect: once a breaker trips OPEN once, it can never transition back
to CLOSED (full traffic resumes) or re-OPEN from a failed probe — it is
permanently wedged admitting exactly one connection attempt per `ttl` window,
forever, independent of whether that attempt succeeds or fails. This is a
functional regression vs. pre-commit behavior (old `record_result` had no
permit requirement and would close normally on a successful probe).

Not caught by tests: every new invariant test in
`tests/test_cache_invariants.py` drives `DestinationCircuitBreaker`/
`CircuitBreakerManager` directly with hand-built permits (e.g.
`test_destination_circuit_breaker_state_machine`,
`test_invariant_half_open_unleased_caller_rejected`) — none exercise
`ProxyCache.check_circuit()` → `tunnel()` → `record_connection_result()` end
to end with a real, enabled circuit breaker.

Fix: make `check_circuit` call `self._circuit_manager.acquire_permit(host,
port)`, raise `CircuitBreakerOpen()` on `None`, and return the permit; give
`record_connection_result` a `permit: CircuitPermit | None = None` parameter
threaded to `self._circuit_manager.record_result(host, port, exc,
permit=permit)`. Once fixed, delete `proxy.py`'s `_record_connection_result`
`TypeError`-swallowing shim (cache.py:493-503 in proxy.py, i.e.
`proxy.py:493-503`) — it exists only to paper over the missing kwarg and will
silently hide the next signature mismatch too.

(This matches `claude-review-0.md`'s Blocker #1 exactly — independently
reproduced, same file/line, same mechanism. Treat as confirmed, not
duplicate noise: two independent passes landed on the identical root cause.)

## Concerns

### 2. `_evict_one_locked` mutates `DestinationCircuitBreaker` internals without the breaker's own lock (CORRECTED from an earlier wrong dismissal in this same file)

`cache.py:427-445`. First pass through this method I wrongly concluded "no bug"
(dict-mutation-during-iteration is fine because of the immediate `return`).
That part is fine. But a second read caught the real issue: this method reads
`b.state` / `b._half_open_in_flight` and calls `b._prune_calls(now)` (which
mutates `b._calls` and `b._window_error_count`) for breaker `b` **without
ever acquiring `b._lock`** — it only holds the *manager's* `self._lock`
(`_evict_one_locked` is called from inside `get_breaker`'s `with self._lock:`
block). Contrast with `CircuitBreakerManager.clear()` (cache.py:465-477),
which correctly does `with breaker._lock:` before touching each breaker's
internals. `DestinationCircuitBreaker` uses `threading.Lock` (not a Trio
lock), which only makes sense if the class is meant to be safe under real
OS-thread concurrency (the repo has stress tests using `pyreqwest`, i.e.
`tests/test_stress_pyreqwest.py`, that plausibly exercise this from multiple
threads). If a `record_result()`/`acquire_permit()`/`can_execute()` call on
the same breaker (all of which correctly take `b._lock`) races with
`_evict_one_locked()`'s unlocked `_prune_calls()` on that same breaker,
`_calls`/`_window_error_count` can be read and mutated concurrently without
synchronization — CPython's GIL prevents a segfault, but the two mutations
(deque `popleft()` vs. `append()`, and the paired `_window_error_count`
increment/decrement) aren't atomic as a pair, so the error-rate accounting
can drift, corrupting future trip/no-trip decisions. This only triggers at
`max_destinations` capacity (default 10,000 distinct destinations), so it's
easy to miss in normal testing, but it is a real, diff-introduced violation
of the class's own locking contract (eviction is new in this diff — the old
`get_breaker` did an unconditional dict insert with no eviction path at
all).

### 3. `CircuitBreakerManager.clear()` epoch bump is on the manager, not synced to `get_breaker`'s per-destination epoch use consistently — checked, this is fine: `clear()` bumps each breaker's own `.epoch` field directly under `breaker._lock`, and any outstanding `CircuitPermit` closes over the breaker instance + the epoch value captured at `acquire_permit()` time, so `permit.epoch != self.epoch` correctly fails stale permits post-clear. `test_invariant_cross_clear_epoch_isolation` covers the manager-level `clear()` replacing the breaker object entirely (via a fresh `get_breaker()` call after `clear()`), but doesn't cover the case this method actually implements: clearing without evicting the breaker object (same instance, bumped epoch). Minor test-gap, not a bug: `CircuitBreakerManager.clear()` (cache.py:465-477) keeps the *same* breaker instances (only resets their internal state and bumps epoch), then calls `self._breakers.clear()` — wait: it does bump epoch/reset each breaker's fields under lock, **and then also calls `self._breakers.clear()`**, discarding the dict entries entirely. So the per-breaker epoch-bump-and-reset work (lines 467-475) is redundant with immediately clearing the dict on the next line (476: `self._epoch_counter += 10`, 477: `self._breakers.clear()`) — the mutated breaker objects are dropped anyway since `get_breaker()` will just build fresh ones with fresh epochs from `self._epoch_counter`. The only thing that survives is: outstanding permits from *before* `clear()` still hold a reference to the old breaker object, so bumping `old_breaker.epoch` in place is what actually invalidates them (their `permit.breaker is self` check still passes since it's the same object, but `permit.epoch != self.epoch` now fails). So the per-breaker mutation is NOT redundant — it's the mechanism that invalidates in-flight permits after a `clear()`. Correction: not a finding, this is intentional and correct. Leaving this paragraph in as a documented dead-end from the removed-behavior angle.

## Notes on angles that came up empty

- **Removed-behavior audit**: the old `_calls = [(t, err) for t, err in self._calls if t >= cutoff]` list-rebuild was replaced by `deque` + `_prune_calls` + `_window_error_count`. Behavior preserved: `_prune_calls` still uses `>=`/`<` cutoff consistently (`while self._calls and self._calls[0][0] < cutoff: popleft()`), matching the old comprehension's `t >= cutoff` retention. No off-by-one introduced.
- **Cross-file tracer**: `CircuitPermit` is exported and imported into `proxy.py`'s type hints but the only two producers of a real (non-None) permit are `DestinationCircuitBreaker.acquire_permit`/`CircuitBreakerManager.acquire_permit` — neither is called anywhere in `proxy.py` or `ProxyCache`. Confirms Blocker #1 from a different angle (dead code path: `acquire_permit` is exercised only by tests, never by production code).
- **Reuse**: `canonicalize_destination` duplicates IPv6-bracket-stripping logic that likely also exists in host/port parsing elsewhere (`process_as_connect_request` / `urlsplit` handling in proxy.py) — not confirmed as true duplication since `urlsplit(f"//{decoded_target}").hostname` already unwraps brackets for a different purpose (connecting), so this is a distinct concern (breaker key identity vs. connect target) rather than copy-paste. No finding.
- **Efficiency**: `_evict_one_locked`'s first loop calls `b._prune_calls(now)` (which mutates the deque) for every CLOSED breaker it scans until it finds one that's empty post-prune — worst case O(n) prunes on every eviction when at capacity, but this only runs at `max_destinations` capacity (default 10,000) and is O(1) amortized per breaker's own deque, so not flagged as a real hot-path cost.
- **Altitude**: the `permit is None` degrade-to-no-op design in `record_result` (rather than raising/asserting) is itself a symptom of the wiring gap in Blocker #1 being invisible at runtime — a stricter contract (e.g. `record_result` asserting a permit is required whenever the breaker is HALF_OPEN, or `check_circuit`/`record_connection_result` type-checking against `CircuitPermit` at the `ProxyCache` boundary) would have made Blocker #1 fail loudly (a raised error) instead of silently wedging. Recommend: once wiring is fixed, make the missing-permit-in-HALF_OPEN case log a warning rather than silently `return`, so a future regression is visible in production logs instead of only in behavior.
- **Conventions (CLAUDE.md)**: no clear rule violation found. The repo CLAUDE.md's "Before committing, run pyright and pytest" is a process note, not a diff-checkable rule; nothing in the diff itself contradicts a quotable line from `~/.claude/CLAUDE.md`, the repo CLAUDE.md, or any ancestor CLAUDE.md of the changed files.

## Cross-check against claude-review-0.md

Agree with review-0's Blocker #1 (permit wiring gap — see above, independently
confirmed) and its rate-limit Concern (`check_rate_limit` using `ttl_sec` as
the counting-window expiry instead of `period_sec`, despite `findings.md`
describing `period_sec` as the counting window) — re-verified: `cache.py`'s
`check_rate_limit` calls `self.cache.incr(rate_key, expire=ttl_sec)`, and
`period_sec` is computed but only ever read as the fallback default for
`ttl_sec`, never passed to `incr`. This is pre-existing (not touched by this
diff) but the diff's own `findings.md`/`progress.md` claim it was hardened
per a different contract, which review-0 correctly flags as a documentation/
behavior mismatch worth resolving alongside Blocker #1.

Also agree with review-0's Nit re: `_http_429_response`'s bare-comma `except
TypeError, ValueError:` in proxy.py being valid-but-confusable PEP 758 syntax
sitting right next to freshly-parenthesized `except (trio.BrokenResourceError,
trio.ClosedResourceError):` tuples elsewhere in the same diff — same
inconsistency, worth a pass for consistency (either parenthesize everywhere
or leave bare everywhere, but the diff currently does both in the same file).

## Bottom line

No new blocker beyond review-0's; this pass corroborates it via independent
code tracing (cross-file "producer of CircuitPermit" trace) rather than
finding anything additional. The permit-wiring gap (Blocker #1) is the one
fix that matters before this commit's stated goal ("harden admission
protocol, leases, and failure boundaries") is actually true in production —
right now the hardening exists only in `DestinationCircuitBreaker` and its
direct unit tests, not on the path real traffic takes.

## Additional findings

### 3. `_record_connection_result` TypeError fallback can double-record

`proxy.py:493-503`:
```python
async def _record_connection_result(proxy_cache, host, port, exc, permit):
    try:
        await proxy_cache.record_connection_result(host, port, exc, permit=permit)
    except TypeError:
        await proxy_cache.record_connection_result(host, port, exc)
```
For the real `ProxyCache`, the `TypeError` fires at call time (bad kwarg),
before the coroutine body runs — safe. But for any `proxy_cache` whose
`record_connection_result` *does* accept `permit` and begins executing (a
custom cache, a test double, a future subclass), a `TypeError` raised from
inside the method body for an unrelated reason (bad input further down,
an internal bug) is indistinguishable from "kwarg not supported" and
triggers a second, duplicate call with fewer arguments — double-recording
the connection result (double-counting a success or failure in the sliding
window / calling `record_result` twice). Prefer `inspect.signature(...)` or
a capability check over exception-based dispatch here.

### 4. `CircuitPermit.abort()` / `mark_completed()` race on unlocked flags

`cache.py:161-169`:
```python
def abort(self) -> None:
    if self.was_aborted or self.was_completed:
        return
    self.was_aborted = True
    self.breaker._abort_permit(self)
```
and `record_result()` (cache.py:332-333) unconditionally calls
`permit.mark_completed()` before taking any lock. Both `was_aborted` and
`was_completed` are plain instance attributes with no lock guarding the
check-then-act sequence on the `CircuitPermit` object itself (only the
*breaker's* state transitions are protected by `b._lock`). Concurrent
`abort()` + `record_result(permit=...)` calls on the same permit from
different threads can both pass their guard check before either flag is
set, each then calling into the breaker (idempotent there, so low impact),
but the design intent ("was_aborted/was_completed make abort/record
mutually exclusive") is not actually enforced by anything at the
`CircuitPermit` level.

### 5. Rate-limit duration validation added unconditionally, even when `rate_limit_enabled=False`

`cache.py:544-556`: the new `rate_limit_period`/`rate_limit_ttl` > 0
validation runs in `ProxyCache.__init__` regardless of
`self.rate_limit_enabled`. A caller that sets `RATE_LIMIT_PERIOD=0s` (or
`RATE_LIMIT_TTL=0s`) while leaving rate limiting disabled — previously a
no-op, since `check_rate_limit` short-circuits on `not self.rate_limit_enabled`
— now raises `ValueError` and prevents `ProxyCache` from constructing at
all. `test_rate_limit_zero_duration_rejection` only tests the
enabled-implicitly case (rate_limit_enabled defaults to `False`, so the test
itself is actually exercising exactly this: construction fails even though
rate limiting is off by default in that test). Whether that's desired is a
judgment call, but it's a behavior change worth calling out: it turns a
previously-inert config value into a hard startup failure independent of
whether the feature is on.

### 6. Rate-limit counting window uses `ttl_sec`, not `period_sec` (pre-existing, but contradicts this diff's own docs)

`cache.py` `check_rate_limit()`: `await self.cache.incr(rate_key,
expire=ttl_sec)` — the counting window's expiry is `ttl_sec` (the ban
duration), not `period_sec` (computed and validated by this diff at
cache.py:544-549 but never otherwise used except as `ttl_sec`'s fallback
default). This line itself isn't touched by the diff, but the diff adds
`rate_limit_period_sec`/`rate_limit_ttl_sec` and a new test
(`test_rate_limit_cashews_ban_ttl_protocol`) whose commit describes this as
implementing a documented two-value protocol (counting window vs. ban TTL)
in `findings.md`/`progress.md`. With default config (`period == ttl == 60s`)
this is unobservable; set them to different values and the counting window
silently follows `ttl_sec` instead of the documented `period_sec`.

## Bottom line

Confirmed blocker (permit-wiring gap, #1) plus a second, independently-found
concurrency bug in eviction (#2) that review-0 didn't catch. Both are
diff-introduced and both undermine this commit's stated goal of hardening
the circuit breaker's admission/completion protocol: #1 means the new lease
machinery never runs on the real request path; #2 means the one new feature
that *is* wired up (bounded destination eviction) can corrupt breaker state
under concurrent load. Recommend fixing both before closing Phase 2.5.
