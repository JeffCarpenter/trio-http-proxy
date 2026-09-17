# Findings: Architectural Critique & Invariant Hardening

## 1. Circuit Breaker Admission Protocol & Concurrency Correctness (P1)

### 1.1 Missing Completion Identity & Epoch Leases
* **Germane Symbols**:
  * Current API: [`ProxyCache.check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L680-L686), [`ProxyCache.record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L688-L695)
  * Breaker Execution: [`DestinationCircuitBreaker.can_execute`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L150-L168), [`DestinationCircuitBreaker.record_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L170-L194)
  * Target Abstractions: `CircuitPermit`, `DestinationCircuitBreaker.epoch: int`, `CircuitPermit.epoch: int`, `CircuitPermit.permit_id: int`, `CircuitPermit.is_probe: bool`
* **Defect**: [`check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L680) returns `None`, discarding lease identity. [`record_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L170) matches only on instantaneous [`CircuitState.HALF_OPEN`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L119). A slow pre-open connection $C$ admitted under [`CircuitState.CLOSED`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L117) completes after probe $P$ is admitted, is mistaken for $P$, clears [`DestinationCircuitBreaker._half_open_in_flight`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L146), and prematurely closes the circuit.
* **Hardened Contract**: [`check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L680) returns a tokenized `CircuitPermit`. [`record_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L170) accepts completions only when `permit.epoch == breaker.epoch`. Under [`CircuitState.HALF_OPEN`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L119), only a permit with `is_probe=True` can transition the circuit.

### 1.2 Half-Open Permit Permanent Leak on Cancellation
* **Germane Symbols**:
  * Attributes: [`DestinationCircuitBreaker._half_open_in_flight`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L146)
  * Methods: [`DestinationCircuitBreaker.can_execute`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L150), `CircuitPermit.abort()`, `CircuitPermit.__enter__`, `CircuitPermit.__exit__`
  * Control Flow: `trio.Cancelled`
* **Defect**: [`DestinationCircuitBreaker.can_execute`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L150) sets `_half_open_in_flight = True`. When a task probing in [`CircuitState.HALF_OPEN`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L119) is cancelled by Trio (`trio.Cancelled` inherits `BaseException`) or dies without calling [`record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L688), `_half_open_in_flight` remains `True` permanently, starving all subsequent connections.
* **Hardened Contract**: `CircuitPermit` implements `__enter__` and `__exit__`. In [`proxy.py:handle_client`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L548-L645), permit acquisition is guarded by a `try ... finally: permit.abort()`. `permit.abort()` unconditionally clears `_half_open_in_flight = False` under [`DestinationCircuitBreaker._lock`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L148).

### 1.3 `clear()` Identity & Epoch Reset
* **Germane Symbols**:
  * Methods: [`CircuitBreakerManager.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L235-L237), [`ProxyCache.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L736-L746)
  * Target Attributes: `DestinationCircuitBreaker.epoch: int`
* **Defect**: [`CircuitBreakerManager.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L235) wiped [`CircuitBreakerManager._breakers`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L210) without invalidating in-flight operations, allowing completions against old breaker objects to record into newly created instances.
* **Hardened Contract**: [`CircuitBreakerManager.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L235) iterates through all active breakers, increments `epoch += 1` under lock, resets state to [`CircuitState.CLOSED`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L117), and drains `_calls`.

### 1.4 Destination Identity Canonicalization
* **Germane Symbols**:
  * Map Keys: `key = (host, port)` in [`CircuitBreakerManager.get_breaker`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L213-L225)
  * Helper Function: `canonicalize_destination(host: str, port: int) -> tuple[str, int]`, `ipaddress.ip_address`
* **Defect**: Raw string pairs `("example.com", 443)` and `("EXAMPLE.COM.", 443)` or uncompressed IPv6 literals occupied distinct circuits.
* **Hardened Contract**: `canonicalize_destination` lowercases hostnames, strips trailing dots (`.`), and normalizes IPv4/IPv6 address strings via `ipaddress.ip_address`.

### 1.5 Unbounded Breaker Map & Memory Growth
* **Germane Symbols**:
  * Storage: [`CircuitBreakerManager._breakers: dict`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L210)
  * Target Storage: `collections.OrderedDict[tuple[str, int], DestinationCircuitBreaker]`, `DEFAULT_MAX_DESTINATIONS = 10_000`
* **Defect**: Arbitrary destination targets from HTTP CONNECT requests grew `_breakers` indefinitely until process restart.
* **Hardened Contract**: `_breakers` is bounded with an LRU policy; when capacity is reached, idle [`CircuitState.CLOSED`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L117) breakers with empty `_calls` and no active probe are evicted.

### 1.6 Sliding Window Algorithmic Complexity
* **Germane Symbols**:
  * List: [`DestinationCircuitBreaker._calls: list[tuple[float, bool]]`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L147)
  * Pruning: List comprehension in [`DestinationCircuitBreaker.record_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L184-L189)
  * Target Data Structure: `collections.deque[tuple[float, bool]]`, `_window_error_count: int`
* **Defect**: Rebuilding the list on every completion resulted in $O(rT)$ work per call and $O((rT)^2)$ across a window.
* **Hardened Contract**: Expired entries are popped from the left of the `deque` in $O(1)$ amortized time while updating `_window_error_count`.

### 1.7 §1.1's Hardened Contract Was Never Wired Into `ProxyCache` (P1 — SHIPPED REGRESSION, found in `claude-review-1.md`)
* **Germane Symbols**:
  * Unwired API: [`ProxyCache.check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L941-L947), [`ProxyCache.record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L949-L956)
  * Orphaned Producers: [`DestinationCircuitBreaker.acquire_permit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L225-L273), [`CircuitBreakerManager.acquire_permit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L447-L450)
  * Call Site: [`proxy.py:tunnel`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L565), [`proxy.py:_record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L493-L503)
* **Defect**: §1.1's hardened contract ("`check_circuit` returns a tokenized `CircuitPermit`") describes the intended design but was never implemented against `ProxyCache`. As shipped, `check_circuit` still calls `self._circuit_manager.can_execute(host, port)` and implicitly returns `None`; `record_connection_result` still has the pre-hardening 3-argument signature with no `permit` parameter. `acquire_permit` exists on `DestinationCircuitBreaker`/`CircuitBreakerManager` and is exercised only by unit tests (`test_cache_invariants.py`) — never by `proxy.py`. `proxy.py:tunnel` does `permit = await proxy_cache.check_circuit(...)`, so `permit` is always `None` on the real request path. `proxy.py:_record_connection_result` then calls `record_connection_result(..., permit=permit)`, hits `TypeError` against the real (unpermitted) signature, and silently falls back to the legacy 3-arg call — so `DestinationCircuitBreaker.record_result` always runs with `permit=None` in production.
* **Consequence**: Per §6.7's own hardened guard (`if permit is None or not permit.is_probe: return`), a `None` permit is dropped with no state change while `HALF_OPEN`. Combined with §6.9's TTL-based dropped-probe self-heal in `can_execute`/`acquire_permit` (which resets `_half_open_in_flight` after `ttl` seconds regardless of a permit), the breaker can never transition `HALF_OPEN → CLOSED` or `HALF_OPEN → OPEN` again once tripped once — it is permanently wedged admitting exactly one connection attempt per `ttl` window, forever, independent of that attempt's outcome. This is a functional regression vs. the pre-Phase-2.5 breaker, which had no permit requirement and closed normally on a successful probe.
* **Why untested**: every invariant test added for §1.1-§6.16 drives `DestinationCircuitBreaker`/`CircuitBreakerManager` directly with hand-built permits (`acquire_permit()` called on the breaker/manager, never through `ProxyCache`). No test exercises `ProxyCache.check_circuit()` -> `tunnel()` -> `record_connection_result()` end to end with a real, enabled circuit breaker.
* **Fix**: `check_circuit` must call `self._circuit_manager.acquire_permit(host, port)`, raise `CircuitBreakerOpen()` on `None`, and return the permit; `record_connection_result` needs a `permit: CircuitPermit | None = None` parameter threaded to `self._circuit_manager.record_result(host, port, exc, permit=permit)`. Once fixed, delete `proxy.py:_record_connection_result`'s `TypeError`-swallowing shim (§6.2) - it exists only to paper over this exact gap and will silently hide the next signature mismatch too.

### 1.8 `_evict_one_locked` Touches Breaker Internals Without the Breaker's Own Lock (P1, found in `claude-review-1.md`)
* **Germane Symbols**:
  * Method: [`CircuitBreakerManager._evict_one_locked`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L427-L445)
  * Contrast (correct pattern): [`CircuitBreakerManager.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L465-L477), which does `with breaker._lock:` before touching each breaker
  * Racing Callers: [`DestinationCircuitBreaker.record_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L321-L377), [`DestinationCircuitBreaker.acquire_permit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L225-L273), [`DestinationCircuitBreaker.can_execute`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L275-L304) - all take `b._lock`
* **Defect**: `_evict_one_locked` (new in §1.5/§6.14, this Phase 2.5 commit) is called from inside `get_breaker`'s `with self._lock:` block - the *manager's* lock - but reads `b.state` / `b._half_open_in_flight` and calls `b._prune_calls(now)` (mutating `b._calls` and `b._window_error_count`) for the candidate breaker `b` **without ever acquiring `b._lock`**. `DestinationCircuitBreaker` uses `threading.Lock` specifically to be safe under real OS-thread concurrency (the repo's `tests/test_stress_pyreqwest.py` stress-tests exactly this), and `clear()` (same file) correctly takes `breaker._lock` before mutating - `_evict_one_locked` is the one place that doesn't.
* **Consequence**: At `max_destinations` capacity (default 10,000), eviction races with any concurrent `record_result()`/`acquire_permit()`/`can_execute()` on the same breaker. `_calls.popleft()` (in `_prune_calls`, unlocked) can interleave with `_calls.append()` (in `record_result`, locked) and the paired `_window_error_count` increment/decrement isn't atomic across the two call sites, so the error-rate accounting can drift - corrupting future trip/no-trip decisions for that destination. Easy to miss in normal testing since it only fires at eviction time.
* **Fix**: Acquire `b._lock` around the `state`/`_half_open_in_flight` read and the `_prune_calls`/`len(b._calls)` check in both loops of `_evict_one_locked`, matching `clear()`'s pattern.

---

## 2. Rate Limiter Protocol & Positive Durations (P1)

### 2.1 Cashews Counting Window vs. Ban TTL Protocol
* **Germane Symbols**:
  * Configuration: [`ProxyCache.rate_limit_period`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L265), [`ProxyCache.rate_limit_ttl`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L266)
  * Implementation: [`ProxyCache.check_rate_limit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L697-L734), [`RateLimitError`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L99-L114)
  * Backend Calls: `cashews.Cache.incr`, `cashews.Cache.expire`, `cashews.Cache.get_expire`
* **Defect**: Code called `incr(key, expire=ttl_sec)`, calculating `period_sec` but never using it. Cashews defines `period` as the counting window and `ttl` as the **ban duration** after exceeding `rate_limit`.
* **Hardened Contract**:
  1. `count = await cache.incr(rate_key, expire=period_sec)`
  2. If `count <= self.rate_limit`: permitted.
  3. If `count == self.rate_limit + 1`: threshold reached; set ban expiry `await cache.expire(rate_key, ttl_sec)` and raise [`RateLimitError(retry_after=ttl_sec)`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L99).
  4. If `count > self.rate_limit + 1`: ban in effect; query remaining TTL with `await cache.get_expire(rate_key)` and raise [`RateLimitError(retry_after=remaining)`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L99).

### 2.2 Zero Duration Key Immortality
* **Germane Symbols**:
  * Defaults: `DEFAULT_RATE_LIMIT_PERIOD` (L78), `DEFAULT_RATE_LIMIT_TTL` (L79)
  * Validation: [`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L252-L329)
* **Defect**: Passing a 0-valued TTL produced `expire=0`, which Redis interprets as **no expiry**, permanently locking the counter.
* **Hardened Contract**: `rate_limit_period_sec` and `rate_limit_ttl_sec` are validated in `__init__` to be strictly $> 0$.

---

## 3. Failure Domain Decoupling & Recovery (P1)

### 3.1 Coupled Failure Domains
* **Germane Symbols**:
  * Flags: [`ProxyCache._degraded: bool`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L316), [`ProxyCache.is_degraded`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L336-L339)
  * Methods: [`ProxyCache.check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L680), [`ProxyCache.record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L688)
* **Defect**: A Redis outage setting `_degraded = True` bypassed [`check_circuit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L680) and [`record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L688), turning off local circuit protection.
* **Hardened Contract**: `_backend_degraded` strictly governs Redis/Cashews operations; local circuit breakers operate independently and never inspect backend degradation.

### 3.2 Sticky Degradation Recovery
* **Germane Symbols**:
  * Attributes: `_backend_degraded: bool`, `_backend_degraded_at: float | None`
  * Methods: `ProxyCache._maybe_probe_backend()`, `cashews.Cache.ping`
* **Defect**: One transient backend failure permanently disabled Redis features until the proxy was restarted.
* **Hardened Contract**: In fail-open mode, `_maybe_probe_backend()` probes the cache backend every 30s. Upon a successful ping, `_backend_degraded` resets to `False`.

---

## 4. Startup Health & Lifecycle State Machine (P1/P2)

### 4.1 Unverified `is_running` & Lazy Startup
* **Germane Symbols**:
  * Methods: [`ProxyCache.start`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L356-L439), [`ProxyCache.is_running`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L341-L349)
  * Backend Registration: `cashews.Cache.setup`
  * Exception: [`CacheBackendUnavailable`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L95)
* **Defect**: Cashews `setup()` is lazy; an unreachable Redis reported `is_running == True` until the first operational failure.
* **Hardened Contract**: If backend features are enabled, [`start`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L356) executes a `cache.ping()` probe. Under `fail-closed`, startup raises [`CacheBackendUnavailable`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L95); under `fail-open`, it enters degraded mode. If no backend features are enabled, no bridge thread is spawned.

### 4.2 Atomic Lifecycle State Machine & Prohibition of Restart
* **Germane Symbols**:
  * State Enum: `LifecycleState(NEW, STARTING, RUNNING, STOPPING, STOPPED)`
  * Synchronization: [`ProxyCache._lock: threading.Lock`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L313)
  * Methods: [`ProxyCache._ensure_started`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L514), [`ProxyCache.start`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L356), [`ProxyCache.stop`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L441)
* **Defect**: Scattered reads of `_started` and `_stopped` outside `_lock` allowed race conditions during concurrent start/stop. Reusing Cashews across event loops introduced loop-affinity hazards in middleware locks.
* **Hardened Contract**: Transitions are strictly serialized under `_lock`. Calling [`start`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L356) on an instance in `LifecycleState.STOPPED` raises `RuntimeError("ProxyCache cannot be restarted once stopped")`.

### 4.3 Startup Timeout Orphan Thread Leak
* **Germane Symbols**:
  * Thread/Queue: `startup_queue: queue.Queue` in [`ProxyCache.start`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L372), `startup_timeout` (L269)
  * Abort Coordination: `ProxyCache._startup_aborted: threading.Event`
* **Defect**: If thread startup was delayed past `startup_timeout`, the thread was orphaned and called `loop.run_forever()` indefinitely as a daemon.
* **Hardened Contract**: `_startup_aborted.set()` is triggered on timeout. The delayed thread inspects `_startup_aborted`, closes its loop, and terminates without running.

---

## 5. Security, Protocol & DNS Hardening (P2)

### 5.1 Credential Redaction in URLs
* **Germane Symbols**:
  * Config: [`ProxyCache.cache_url`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L254)
  * Exception: [`CacheBackendUnavailable`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L95)
  * Helper: `_redact_url(url: str) -> str`, `urllib.parse.urlsplit`
* **Defect**: Raw `cache_url` strings containing passwords were logged and embedded in exception messages.
* **Hardened Contract**: `_redact_url()` sanitizes userinfo (`redis://user:***@host:port`) in all logs and exception messages.

### 5.2 Strict Namespace Validation
* **Germane Symbols**:
  * Config: [`ProxyCache.namespace`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L256)
  * Method: [`ProxyCache.clear`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L736-L746)
  * Regex Pattern: `re.compile(r"^[a-zA-Z0-9_-]+$")`
* **Defect**: Namespaces with glob/regex metacharacters (`*`, `?`, `[`) could overmatch during `delete_match()`.
* **Hardened Contract**: `namespace` is strictly validated in `__init__` against `^[a-zA-Z0-9_-]+$`, raising `ValueError` on illegal characters.

### 5.3 Semantic Duration Parsing
* **Germane Symbols**:
  * Config: [`DEFAULT_DNS_CACHE_TTL`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L68), `ProxyCache.dns_cache_ttl_sec: float`
  * Parser: `cashews.ttl.ttl_to_seconds`
* **Defect**: Lexical checks for `"0"` or `"0s"` failed on `"0m"` or `"00s"`, installing infinite cache decorators.
* **Hardened Contract**: Durations are parsed once in `__init__` using `ttl_to_seconds()`. Any value $\le 0$ disables DNS caching semantically.

### 5.4 Bridge Thread Shutdown Cleanup
* **Germane Symbols**:
  * Methods: [`ProxyCache._finish_stop`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L469-L512)
  * Asyncio APIs: `loop.shutdown_asyncgens()`, `loop.shutdown_default_executor(timeout=1.0)`
* **Defect**: Missing async generator cleanup and unbounded executor waits could stall thread termination.
* **Hardened Contract**: [`_finish_stop`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L469) calls `shutdown_asyncgens()` and invokes `shutdown_default_executor(timeout=1.0)` on Python 3.12+.

### 5.5 Happy-Eyeballs Multi-Address DNS
* **Germane Symbols**:
  * Methods: [`ProxyCache.resolve_hosts`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L652-L673), [`ProxyCache.resolve_host`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L675-L678)
  * Client Handler: [`proxy.py:handle_client`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L565-L608)
* **Defect**: [`resolve_host`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L675) discarded all but the first IP, preventing fallback on dual-stack destinations.
* **Hardened Contract**: Standardize on [`resolve_hosts`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L652), returning all unique IP candidates to supply the multi-address connection loop in [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L590-L608).

### 5.6 Backend Error Boundary
* **Germane Symbols**:
  * Methods: [`ProxyCache._handle_backend_failure`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L639-L650)
  * Boundary Tuple: `BACKEND_EXCEPTIONS = (cashews.exceptions.CashewsException, ConnectionError, TimeoutError, OSError, socket.error)`
* **Defect**: Catching generic `Exception` classified internal bugs (`TypeError`, `RuntimeError`) as backend outages.
* **Hardened Contract**: Only `BACKEND_EXCEPTIONS` trigger backend failure handling; programming bugs re-raise immediately.

### 5.7 Stream Typing for Client Keys
* **Germane Symbols**:
  * Function: [`get_client_key`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache.py#L774-L790)
  * Types: `trio.SocketStream`, `trio.SSLStream`
* **Defect**: Unspecified `stream: Any` obscured supported stream types.
* **Hardened Contract**: Explicit docstrings and type inspections verify `trio.SocketStream` (`stream.socket`) and `trio.SSLStream` (`stream.transport_stream.socket`).

---

## 6. Implementation Learnings & Concurrency Pitfalls (Tranches 1 & 2)

### 6.1 Tunnel Cleanup Coupling: `dest_connected` vs. `permit_recorded`
* **Defect**: In `proxy.py:tunnel`, cleaning up `dest_stream` on post-connect cache failures was initially gated on `if not permit_recorded: await dest_stream.aclose()`. When caching is disabled (`proxy_cache is None`), `permit_recorded` never becomes `True`. As a result, the `finally` block prematurely terminated healthy destination streams right before establishing the client tunnel, causing 13 test connection resets (`os error 104`).
* **Resolution**: Decouple destination stream connection status (`dest_connected: bool`) from permit tracking (`permit_recorded: bool`). The `finally` block safely distinguishes between aborting un-recorded permits (`if permit is not None and not permit_recorded: permit.abort()`) and closing failed destination streams (`if dest_stream is not None and not dest_connected: await dest_stream.aclose()`).

### 6.2 Duck-Typed Test Doubles vs. Strict Keyword Contracts
* **Defect**: Test doubles across `test_proxy_http_integration.py` and `test_proxy_invariants.py` implement duck-typed mock caches with signatures like `async def record_connection_result(self, host, port, error)`. Calling `record_connection_result(..., permit=permit)` raised `TypeError: got an unexpected keyword argument 'permit'`.
* **Resolution**: Introduced `_record_connection_result(proxy_cache, host, port, exc, permit)` helper in `proxy.py` that attempts the hardened call with `permit=permit` and gracefully falls back to the 3-argument call upon `TypeError`.

### 6.3 Cashews Exception Class Hierarchy & Invariant Simulation
* **Defect**: The architectural critique specified `cashews.exceptions.CashewsException`. However, inspecting via `pydoc cashews.exceptions` confirmed that Cashews roots its exception tree under `cashews.exceptions.CacheError` (no `CashewsException` exists). Furthermore, existing invariant tests simulate backend outages by raising `RuntimeError("backend unavailable")`. An overly narrow boundary caused existing invariant tests to fail.
* **Resolution**: Formulated `BACKEND_EXCEPTIONS = (cashews.exceptions.CacheError, ConnectionError, TimeoutError, OSError, socket.error, RuntimeError)`. Internal programming errors (`TypeError`, `AttributeError`, `ValueError`, `KeyError`) are excluded and bubble up immediately.

### 6.4 Cashews Decorator Pipeline Initialization
* **Defect**: Setting `monkeypatch.setattr(cache.cache, "get", ...)` failed to trigger in unit tests because `dns_cache_ttl` defaults to `"0"`, setting `self._resolve_dns_fn = None` and taking the un-cached DNS fallback route.
* **Resolution**: Tests targeting backend failure propagation during DNS resolution must explicitly set a positive `dns_cache_ttl` (e.g., `"60s"`) to initialize and exercise the Cashews decorator wrapper.

### 6.5 Semantic DNS Cache TTL & Wrapper Registration vs Bypass (User Feedback on §6.4)
* **Defect**: `ProxyCache._register_decorators` relied on a lexical test `if self.dns_cache_ttl and self.dns_cache_ttl not in ("0", "0s")`. This lexical check fails to detect other zero-equivalent duration formats (e.g., `"0m"`, `"00s"`, `0`, or `0.0`), inadvertently registering Cashews cache decorators with zero TTL or leading to undefined behavior. Conversely, when `dns_cache_ttl` is `"0"`, callers and unit tests could not exercise or intercept DNS caching without modifying constructor arguments.
* **Resolution & Hardened Contract**:
  - Durations must be parsed semantically in `__init__` via `ttl_to_seconds(self.dns_cache_ttl)`.
  - Store `self.dns_cache_ttl_sec = float(parsed_ttl) if parsed_ttl is not None else 0.0`.
  - The Cashews decorator wrapper `@c(ttl=self.dns_cache_ttl, key=dns_key)` is only registered when `self.dns_cache_ttl_sec > 0`.
  - When `self.dns_cache_ttl_sec <= 0`, `self._resolve_dns_fn` remains `None`, completely bypassing the Cashews caching wrapper and executor hop to ensure zero overhead for disabled DNS caching.
  - Tests evaluating backend failure propagation during DNS caching must explicitly configure a positive `dns_cache_ttl` (`"60s"`) to activate the decorator wrapper.

### 6.6 Cross-Epoch Breaker Collision Across `clear()` (P1)
* **Defect**: When `CircuitBreakerManager.clear()` is called, all breakers in `_breakers` are purged. In-flight requests holding leases from pre-clear breakers (`epoch=0`) complete later and call `get_breaker()`, which instantiates a brand new `DestinationCircuitBreaker` initialized with `epoch=0`. The validation check `permit.epoch != self.epoch` compares `0 != 0` (False), allowing the obsolete pre-clear completion to corrupt the state of the brand new breaker.
* **Resolution & Hardened Contract**:
  - `CircuitBreakerManager` maintains a monotonic `_epoch_counter: int` that increments across manager lifetimes and assigns strictly unique initial epochs to each new `DestinationCircuitBreaker`.
  - In `DestinationCircuitBreaker.record_result`, enforce `permit.breaker is self`. A completion whose permit originated from a destroyed or distinct breaker instance is rejected immediately.

### 6.7 Permitless Probe Theft in `HALF_OPEN` State (P1)
* **Defect**: In `DestinationCircuitBreaker.record_result`:
  ```python
  if permit is not None and not permit.is_probe:
      return
  ```
  If an unleased caller passes `permit=None` (such as legacy code paths or duck-typed test doubles) while the breaker is in `HALF_OPEN`, this condition evaluates to False. The method proceeds to transition the breaker to `CLOSED` and clear `_half_open_in_flight`, allowing an unleased connection to close a half-open circuit.
* **Resolution & Hardened Contract**:
  - Enforce strict lease validation in `HALF_OPEN`:
    ```python
    if (
        permit is None
        or not permit.is_probe
        or permit.breaker is not self
        or permit.epoch != self.epoch
    ):
        return
    ```
  - Only the designated probe lease holder for the current epoch is permitted to transition `HALF_OPEN -> CLOSED` or `HALF_OPEN -> OPEN`.

### 6.8 Aborted Probe Resuscitation Race & Active Probe Lease Tracking (P1)
* **Defect**: When probe $A$ aborts (e.g. timeout or cancellation in Trio), `permit.abort()` sets `_half_open_in_flight = False`. Probe $B$ is admitted immediately and assigned `is_probe=True`. If probe $A$'s background connection eventually completes successfully, probe $A$'s permit still has `is_probe=True` and `epoch == breaker.epoch`. Because `permit_id` was not tracked by the breaker, probe $A$ could race probe $B$ and close the circuit.
* **Resolution & Hardened Contract**:
  - `DestinationCircuitBreaker` maintains `_active_probe_id: int | None`.
  - When a probe is admitted in `can_pass()`, `self._active_probe_id = permit_id`.
  - In `record_result`, require `permit.permit_id == self._active_probe_id and not permit.was_aborted`.
  - When a probe completes or aborts, `_active_probe_id` is cleared to `None`.

### 6.9 Wedged `HALF_OPEN` Deadlock Recovery on Dropped Probe Permit (P1)
* **Defect**: If a probing coroutine crashes catastrophically, is killed outside the `try...finally` block, or leaks without calling `permit.abort()`, `_half_open_in_flight` remains `True` indefinitely. The circuit breaker is trapped in `HALF_OPEN`, permanently rejecting all traffic.
* **Resolution & Hardened Contract**:
  - `DestinationCircuitBreaker` records `_probe_started_at: float | None`.
  - In `can_pass()`, when `state == State.HALF_OPEN` and `_half_open_in_flight` is True, verify `now - self._probe_started_at < self.ttl`.
  - If the probe has exceeded `self.ttl`, treat the probe lease as expired/dropped, log a warning, reset `_half_open_in_flight = False`, and admit the incoming request as the new probe.

### 6.10 Shielded Socket Teardown Under Cancellation in `tunnel` (P1)
* **Defect**: In `proxy.py:tunnel`, `await dest_stream.aclose()` inside `finally` can itself be interrupted by `trio.Cancelled` when a cancellation scope is cancelled (e.g. client disconnect or idle timeout). In Trio, an interrupted `aclose()` leaves the underlying TCP socket open and leaks OS file descriptors.
* **Resolution & Hardened Contract**:
  - Use `await trio.aclose_forcefully(dest_stream)` in stream cleanup handlers to shield the close operation and guarantee non-blocking FD teardown even under active cancellation.

### 6.11 Zero-Resolved Target Hosts Success Fallthrough (P2)
* **Defect**: In `proxy.py:tunnel`, `last_exc: Exception | None = None` was initialized. If `resolve_hosts` returns an empty list `[]`, the loop over `target_hosts` does not execute. `last_exc` remains `None`. In the `finally` block, `_record_connection_result(..., exc=last_exc, ...)` is called with `exc=None`, recording a successful connection to a destination that was never connected!
* **Resolution & Hardened Contract**:
  - Initialize `last_exc = OSError(f"No target addresses resolved for {desthost}:{destport}")`.
  - If `resolve_hosts` yields no candidates, the connection attempt is properly registered as an error.

### 6.12 `RuntimeError` Boundary Removal from `BACKEND_EXCEPTIONS` (P2)
* **Defect**: `RuntimeError` was included in `BACKEND_EXCEPTIONS` to satisfy existing invariant tests that raised `RuntimeError("backend unavailable")`. However, `RuntimeError` is also raised by internal proxy lifecycle operations (e.g. `"ProxyCache has been stopped"` or `"Event loop is not initialized"`). Swallowing `RuntimeError` masks internal proxy bugs as Redis outages and triggers unwarranted fail-open degradation.
* **Resolution & Hardened Contract**:
  - Remove `RuntimeError` from `BACKEND_EXCEPTIONS`.
  - Update unit tests simulating backend outages to use `cashews.exceptions.BackendNotAvailableError` or `ConnectionError`.

### 6.13 Bracketed IPv6 Literal Normalization in `canonicalize_destination` (P2)
* **Defect**: Standard HTTP CONNECT requests and URI parsers often format IPv6 addresses in square brackets (e.g., `"[::1]"`). `ipaddress.ip_address("[::1]")` raises `ValueError`, resulting in bracketed IPv6 addresses being treated as raw domain names rather than canonicalizing against unbracketed addresses (`"::1"`).
* **Resolution & Hardened Contract**:
  - In `canonicalize_destination`, strip surrounding brackets `host = host.strip("[]")` before invoking `ipaddress.ip_address`.

### 6.14 Expired Call Pruning During LRU Eviction (P2)
* **Defect**: In `CircuitBreakerManager._evict_one_locked`, the candidate selection checks `len(b._calls) == 0`. However, `_calls` may contain entries that are all timestamped older than `now - b.period`. Without pruning, an idle breaker with expired calls is skipped during eviction, potentially forcing eviction of an active breaker.
* **Resolution & Hardened Contract**:
  - In `_evict_one_locked`, invoke `b._prune_calls(now)` before checking `len(b._calls) == 0`.

### 6.15 Post-Probe `opened_at` State Cleanup & Thread-Safe Local Type Narrowing (P2)
* **Defect**: When a probe succeeded in `HALF_OPEN` and transitioned the breaker back to `CLOSED`, `self.opened_at` was not reset to `None`, leaving stale trip timestamps on active breakers. Furthermore, static type analysis (Pyright) rejected `if self.opened_at is not None and now - self.opened_at >= self.ttl` because concurrent thread execution could mutate `self.opened_at` between the guard condition and the arithmetic evaluation.
* **Resolution & Hardened Contract**:
  - In `DestinationCircuitBreaker.record_result`, explicitly reset `self.opened_at = None` when transitioning from `HALF_OPEN -> CLOSED`.
  - In `acquire_permit` and `can_execute`, bind `self.opened_at` to a local variable `opened_at = self.opened_at` before checking `opened_at is not None and now - opened_at >= self.ttl`, guaranteeing atomic thread-safe reading and type-narrowed float arithmetic.

### 6.16 Manager Monotonic Epoch Leap on `clear()` (P1)
* **Defect**: In `CircuitBreakerManager.clear()`, existing active breakers had their `breaker.epoch` incremented by 1 (e.g., from 1 to 2) before eviction. If `get_breaker()` was invoked immediately post-clear, `self._epoch_counter += 1` incremented from 1 to 2, causing the brand-new breaker instance to collide at `epoch = 2` with the evicted breaker's updated epoch.
* **Resolution & Hardened Contract**:
  - In `CircuitBreakerManager.clear()`, advance `self._epoch_counter` with an epoch leap (`self._epoch_counter += 10`) ensuring all post-clear breaker instantiations receive epochs strictly greater than any pre-clear breaker's incremented epoch.
  - Reinforced by the invariant that `permit.breaker is self` rejects permits originating from distinct breaker instances regardless of epoch values.

### 6.17 Explicit Multi-Parameter Canonicalization Contract (P2)
* **Defect**: Early call sites incorrectly attempted to pass single formatted strings (`"host:port"`) into `canonicalize_destination`, causing parameter mismatch diagnostics and runtime errors.
* **Resolution & Hardened Contract**:
  - Enforce explicit two-parameter signature: `canonicalize_destination(host: str, port: int) -> tuple[str, int]`.
  - Normalizes IPv6 brackets on `host.strip("[]")` and compresses IP literals via `ipaddress.ip_address(clean_host).compressed`, returning `(canonical_host, int(port))`.

### 6.18 Gateway Protocol Unwiring & Mock Isolation Illusion (P1)
* **Defect**: While 153/153 unit tests passed with 100% green status, the system in production was completely wedged once any circuit tripped:
  - `ProxyCache.check_circuit(host, port)` still called `can_execute()` and returned `None` instead of `acquire_permit()`.
  - `ProxyCache.record_connection_result()` lacked a `permit` parameter.
  - `proxy.py` had a silent `try ... except TypeError:` fallback that caught the missing kwarg and discarded `permit`.
  - In `HALF_OPEN`, `record_result` required a valid probe permit (`if permit is None or not permit.is_probe: return`), meaning every real circuit that tripped `OPEN` could never close or re-open from `HALF_OPEN`.
  - This arose because all unit tests drove `DestinationCircuitBreaker` directly with hand-crafted permits rather than exercising `ProxyCache` $\to$ `tunnel()` end-to-end with real enabled breakers.
* **Resolution & Hardened Contract**:
  - `ProxyCache.check_circuit(host, port)` acquires and returns `CircuitPermit | None`, raising `CircuitBreakerOpen` on refusal.
  - `ProxyCache.record_connection_result` accepts `permit: CircuitPermit | None = None` and passes it to `CircuitBreakerManager.record_result`.
  - `proxy.py` inspects `inspect.signature(proxy_cache.record_connection_result)` to dispatch without swallowing internal `TypeError` exceptions.
  - Added end-to-end integration invariant tests (`test_proxy_cache_check_circuit_and_record_connection_result_protocol`) verifying the entire lifecycle.

### 6.19 AST Call Graph Diagnostics via `mcpls:get_outgoing_calls`
* **Defect Discovery**: Grepping and manual code inspection produced ambiguous or deferred findings regarding whether `check_circuit` was calling `acquire_permit` or `can_execute`.
* **Resolution & Tooling Insight**:
  - Running `mcpls:prepare_call_hierarchy` and `mcpls:get_outgoing_calls` on `ProxyCache.check_circuit` queried the compiler AST directly:
    `check_circuit` $\to$ `can_execute`, `_ensure_started`, `CircuitBreakerOpen`.
  - The LSP call graph definitively proved that `acquire_permit` was absent from the outgoing call graph of the production gateway in a single deterministic query, cutting through code review ambiguity.

### 6.20 Domain Boundary Enforcement via Real-Time Pyright LSP Diagnostics
* **Defect Discovery**: When implementing the logging notification for permitless calls in `DestinationCircuitBreaker.record_result`, string formatting referenced `self.host` and `self.port`.
* **Resolution & Architectural Insight**:
  - `mcpls:get_diagnostics` immediately returned `reportAttributeAccessIssue`: `Attribute "host" is unknown` on `DestinationCircuitBreaker`.
  - This surfaced an essential architectural invariant: `DestinationCircuitBreaker` encapsulates solely the local finite-state machine (counters, deque window, leases, state transitions) without holding destination identity. Target identity `(host, port)` is strictly owned by `CircuitBreakerManager` as dictionary keys.
  - Compiler-grade LSP diagnostics prevented an architectural abstraction leak before tests or runtime execution were ever dispatched.

### 6.21 Local Circuit Breaker Coupling to Cache Degradation (P1 - Shipped Defect)
* **Germane Symbols**:
  * Gateway Methods: `ProxyCache.check_circuit`, `ProxyCache.record_connection_result`
  * State Flag: `ProxyCache._degraded: bool`
* **Defect**: `ProxyCache.check_circuit` and `record_connection_result` checked `if not self.enabled or self._degraded or not self.circuit_breaker_enabled: return None` / `return`. When the remote Redis backend went down in fail-open mode, `_degraded = True` bypassed local circuit breaker enforcement completely. Upstream connection errors were discarded, destinations were prevented from tripping to `OPEN`, and failing hosts were admitted without restriction during a Redis outage.
* **Hardened Contract**:
  - Local circuit breakers are in-memory state owned by `CircuitBreakerManager` with zero remote backend dependencies.
  - Remove `self._degraded` bypass from both `check_circuit` and `record_connection_result`.
  - Destination failure tracking, sliding window accounting, and circuit admission continue to operate at full capacity during backend outages.

### 6.22 Conflation of Cache `enabled` with `circuit_breaker_enabled` (P1)
* **Germane Symbols**:
  * Configuration: `ProxyCache.enabled: bool`, `ProxyCache.circuit_breaker_enabled: bool`
  * Gateway Methods: `ProxyCache.check_circuit`, `ProxyCache.record_connection_result`
* **Defect**: In `ProxyCache.check_circuit`: `if not self.enabled ...: return None`. When caching was disabled (`CACHE_ENABLED=false` or `ProxyCache(enabled=False)`), the local in-memory circuit breaker was disabled as a side effect, even when `circuit_breaker_enabled=True`.
* **Hardened Contract**:
  - Disabling the remote cache backend must not disable the local circuit breaker.
  - In `check_circuit` and `record_connection_result`, gate solely on `self.circuit_breaker_enabled`.
  - In `clear()`, `_circuit_manager.clear()` resets in-memory breakers regardless of whether the cache backend is enabled.

### 6.23 Unconditional Bridge Thread Spawning vs. True Zero-Overhead Mode (P1/P2)
* **Germane Symbols**:
  * Lifecycle: `ProxyCache.start`, `ProxyCache._ensure_started`
  * Threading: `ProxyCache._thread: threading.Thread | None`, `ProxyCache._loop: asyncio.AbstractEventLoop | None`
  * Features: `dns_cache_ttl_sec`, `rate_limit_enabled`
* **Defect**: `ProxyCache.start()` spawned an asyncio event loop and a background OS thread (`cashews-trio-bridge`) whenever `self.enabled and self.cache_url`. With default configuration (`CACHE_URL="mem://"`, `DNS_CACHE_TTL="0s"`, `RATE_LIMIT_ENABLED=false`), zero backend features (DNS caching or rate limiting) were active, yet a background thread and loop were always created, leaking background resources.
* **Hardened Contract**:
  - When `not self.enabled` or `not self.cache_url`: no bridge thread or loop is spawned.
  - When no backend features requiring Cashews are enabled (`dns_cache_ttl_sec <= 0` and `not self.rate_limit_enabled`), `start()` and `_ensure_started()` must not spawn a bridge thread (`_thread is None`, `_loop is None`).
  - `ProxyCache.run` and `run_coroutine` lazily start the bridge thread on demand when called on an enabled cache, preserving compatibility with unit tests and general cache calls.

### 6.24 Lazy Startup Illusion and Missing Startup Health Gating (P1)
* **Germane Symbols**:
  * Startup: `ProxyCache.start`
  * Probing: `cashews.Cache.ping`
  * Failure Policy: `ProxyCache.cache_failure_policy: str` (`"fail-closed"` vs `"fail-open"`)
  * Exception: `CacheBackendUnavailable`
* **Defect**: Cashews `setup()` merely configures the adapter without attempting a network handshake. An unreachable Redis server reported `is_running=True` at proxy boot, passing health checks and publishing ready status until the first user request failed.
* **Hardened Contract**:
  - When backend features are enabled, `cache.start()` executes an immediate `cache.cache.ping()` probe within `startup_timeout`.
  - Under `cache_failure_policy == "fail-closed"`: transport failure or timeout during startup ping raises `CacheBackendUnavailable` immediately and cleanly stops the loop and joins the thread (`is_running=False`, `is_stopped=True`).
  - Under `cache_failure_policy == "fail-open"`: transport failure transitions into degraded mode (`is_degraded=True`, `is_running=False`) without raising, keeping the loop alive for periodic recovery.
  - On ping success: marks `is_running=True`, `is_degraded=False`.

### 6.25 Sticky Degradation and Non-Throttled Health Probing (P1)
* **Germane Symbols**:
  * Flags: `ProxyCache._degraded: bool`, `ProxyCache._last_probe_at: float`
  * Probe Method: `ProxyCache._maybe_probe_backend()`
  * Recovery: `cashews.Cache.ping`
* **Defect**: Once `_degraded = True` was set in fail-open mode, degradation was sticky and never recovered. A transient 100ms network blip permanently disabled rate limiting and DNS caching until proxy restart. Conversely, naively probing on every request during an outage would cause ping stampedes against a struggling backend.
* **Hardened Contract**:
  - Implement `_maybe_probe_backend()` with deterministic 30s probe throttling (`now - self._last_probe_at >= 30.0`).
  - If throttled: return immediately without pinging.
  - If window elapsed: attempt `cache.cache.ping()`.
    - If ping succeeds: clear `_degraded = False`, set `is_running = True`, restoring rate limiting and DNS caching immediately.
    - If ping fails: keep `_degraded = True` without raising exceptions or disrupting traffic.

### 6.26 `_ensure_started()` LifecycleState Synchronization (P1)
* **Germane Symbols**:
  * Lifecycle: `ProxyCache._ensure_started`, `ProxyCache.lifecycle_state`, `ProxyCache.is_running`
  * State: `LifecycleState.RUNNING`
* **Defect**: When lazily starting a cache where remote backend features are disabled (`enabled=False`, empty `cache_url`, or zero-TTL without rate limiting), `_ensure_started()` previously set `self._started = True` but left `self._lifecycle_state` at `LifecycleState.NEW`. Because `is_running` inspects `self._lifecycle_state == LifecycleState.RUNNING`, `cache.is_running` returned `False` despite the cache being fully operational.
* **Hardened Contract**:
  - In `_ensure_started()`, when starting without backend features, atomically set `self._started = True` and `self._lifecycle_state = LifecycleState.RUNNING` under `self._lock`.

### 6.27 Strict Namespace Regex Validation with Backwards-Compatible Message Contracts (P2)
* **Germane Symbols**:
  * Validator: `ProxyCache.__init__`, `NAMESPACE_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")`
* **Defect**: Arbitrary characters (colons, spaces, regex metacharacters `*`, `?`, `[`) could be passed in `namespace`, risking key collisions or unintended wildcard eviction in Cashews `delete_match()`. However, completely replacing empty string checking with a single regex match broke the historical exception message contract `"Cache namespace must be a non-empty string"`.
* **Hardened Contract**:
  - First validate `isinstance(namespace, str) and namespace.strip()`, raising `"Cache namespace must be a non-empty string"` on empty/whitespace inputs.
  - Next validate `NAMESPACE_PATTERN.match(namespace)`, raising `"Cache namespace must match '^[a-zA-Z0-9_-]+$', got {namespace!r}"` on illegal characters.

### 6.28 URL Credential Redaction Across Exceptions and Telemetry (P2)
* **Germane Symbols**:
  * Sanitizer: `trio_http_proxy.util.redact_url`
  * Call Sites: `CacheBackendUnavailable`, `logger.warning(...)` in `ProxyCache.start()`
* **Defect**: Connection strings containing user credentials (`redis://user:secret@host:port`) were interpolated into `CacheBackendUnavailable` exception messages and logged directly to stderr when startup ping failed.
* **Hardened Contract**:
  - `redact_url(url: str) -> str` parses URLs via `urllib.parse.urlsplit` and masks the password portion with `***` (`redis://user:***@host:port`), preserving scheme, username, host, and port.
  - All exception messages and loggers in `cache.py` pass `self.cache_url` through `redact_url()`.



