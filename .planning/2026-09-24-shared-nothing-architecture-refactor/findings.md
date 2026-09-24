# Research & Architectural Findings: Shared-Nothing Refactor

## 1. Context & Architectural Goals
The objective is to **rigorously refactor** `trio_http_proxy` pushing hard toward a **shared-nothing architecture**, eliminating shared mutable state, ambient global dependencies, lock contention, and cross-task coupling, while resolving critical latent concurrency/configuration defects (identified in the post-epigram9 audit).

## 2. Analysis of Shared State & Coupling in Current Architecture

### A. Ambient Globals & ContextVars
1. **Global `get_settings()` / `reset_settings_cache()`:**
   - `http_proxy()`, `tunnel()`, `process_as_connect_request()`, `pipe()`, `pipe_with_timeout()`, and `CircuitBreakerManager` all silently fall back to `get_settings()` (a `@cache` singleton loading from environment).
   - *Impact:* Custom settings supplied via `ProxyBuilder` or passed directly to `start_server` are completely dropped for all timeouts, buffer sizes, and limits, because subroutines fall back to global env defaults (Audit Finding 12).
   - *Shared-Nothing Solution:* Explicit dependency injection of settings/config through `ConnectionContext` or explicit parameters (`settings: Settings`). No subroutine relies on ambient global settings when a context/setting is available.

2. **Global Connection Limiter `get_conn_limiter()`:**
   - `@cache` function creates a global `trio.CapacityLimiter` based on `get_settings().maxconns`.
   - *Impact:* Passing a limiter or custom settings to `ProxyBuilder` causes `http_proxy` to fall back to `get_conn_limiter()` via `_DEFAULT_LIMITER`, acquiring a second token on the same connection. When `maxconns=1`, this permanently deadlocks; on `maxconns=N`, it halves capacity (Audit Finding 11).
   - *Shared-Nothing Solution:* Connection capacity gating belongs strictly to the listener/supervisor layer. Connection handlers are pure workers that do not redundantly acquire/release the supervisor's limiter.

3. **Global ContextVars & Global `_conn_ids` Counter:**
   - `CV_CLIENT_STREAM`, `CV_DEST_STREAM`, `CV_DESTHOST`, `CV_CONN_ID`, `CV_PROXY_CACHE`, `CV_PIPE_FROM` in `connection_context.py`.
   - Global `_conn_ids = count(1)`.
   - *Shared-Nothing Solution:* Encapsulate connection state into an isolated `ConnectionContext` object instantiated per connection. Provide a clean connection-owned logging adapter and context manager. Retain ContextVar synchronization solely for backwards compatibility with external callers.

### B. Monolithic God Object (`ProxyCache`) & Mutex Contention
1. **Coupled Subsystems:**
   - `ProxyCache` currently combines DNS resolution caching, Trio circuit breaking, rate limiting, Cashews background thread lifecycle, degraded mode state machine, and periodic probing into a single 900-line class.
   - *Impact:* Any connection needing rate limiting or circuit breaking is tightly coupled to this monolithic object and its synchronous `threading.Lock` instances (`_lock`, `_probe_lock`).
   - *Audit Finding 19:* Synchronous `self._lock` held during remote Redis ping in thread queue blocks the Trio main thread via `pthread_mutex_lock`, freezing the entire async event loop.
   - *Audit Finding 29:* Synchronous `proxy_cache.stop()` calls `thread.join(timeout=2.0)` directly on the Trio event loop thread.
   - *Audit Finding 30:* `self._degraded` state updated without holding `self._lock` while `is_running` acquires `self._lock`.
   - *Shared-Nothing Solution:*
     - Decouple components into cohesive, single-responsibility units:
       - `CircuitBreakerManager` / `DestinationCircuitBreaker` (circuit breaking)
       - `RateLimiter` (rate limiting)
       - `DnsResolver` (DNS resolution & caching)
       - `CacheBackendBridge` (Cashews thread lifecycle)
     - `ProxyCache` composes these decoupled components to preserve `ProxyCacheProtocol` backwards compatibility.
     - Release mutexes before any blocking operations or use non-blocking async synchronization.
     - Make `stop()` non-blocking on the Trio thread via `trio.to_thread.run_sync`.

### C. Builder State Mutation & Leakage
1. **Audit Finding 10:** `ProxyCacheBuilder.__enter__` / `__aenter__` mutates `self._built_cache`. Concurrent entries overwrite the reference, stopping the wrong cache on exit and leaking resources.
   - *Shared-Nothing Solution:* Separate the immutable/reusable configuration builder from the runtime context manager session.

2. **Audit Finding 1:** `SettingsBuilderMixin.build_settings()` previously used `model_copy(update=...)`, which bypassed Pydantic validation and left stale `@cached_property` values in `__dict__`.
   - *Shared-Nothing Solution:* Always rebuild clean immutable `Settings` instances through validation (`Settings.model_validate` or `Settings(**...)`).

### D. Protocol, Parsing & Robustness Defects (Audit Findings)
1. **Finding 13:** RFC 7231 authority-form validation in `protocol.py`. Empty strings in username/query/fragment evaluate falsy, allowing invalid targets (`@example.com`, `:#`, etc.). Validate characters and port range `1 <= port <= 65535`.
2. **Finding 14:** `dest_stream.aclose()` in `tunnel()` exception handler is unshielded; pipelined `dest_stream.send_all(trailing_data)` lacks a timeout scope. Wrap trailing data write in timeout and use `trio.aclose_forcefully(dest_stream)`.
3. **Finding 16:** `streaming.py`: `on_receive()` is invoked on 0-byte EOF before loop break, prematurely resetting deadline to `send_timeout`.
4. **Finding 20:** `ConnectionResultBuilder.record_to()` drops `permit` on fallback dispatch. In `HALF_OPEN`, `record_result` ignores permitless calls, permanently locking the breaker in `HALF_OPEN`.
5. **Finding 21:** `CircuitBreakerManager`: If `max_destinations <= 0`, crash on empty dict with `KeyError`. Validate `max_destinations >= 1`.
6. **Finding 23:** Eviction under high destination churn: prioritize evicting `CLOSED` idle breakers over `OPEN` breakers.
7. **Finding 24:** `ProxyCache.check_rate_limit`: `if count >= self.rate_limit + 1:` instead of `==` for penalty TTL attachment during concurrent bursts.
8. **Finding 27:** `ConnectionResultBuilder.build()` must validate host and port (`1 <= port <= 65535`).
9. **Finding 31:** `_http_429_response`: safely extract positive integer from `retry_after`, fallback to default 1s if conversion fails without raising `TypeError`.
10. **Finding 33:** `RateLimitError.__init__`: filter unexpected kwargs before passing to `super().__init__`.
