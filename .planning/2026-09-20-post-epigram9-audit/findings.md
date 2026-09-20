# Code Review Findings: HEAD~3..HEAD (`src/`)

**Date:** 2026-09-20  
**Target Codebase:** `trio_http_proxy` (`src/trio_http_proxy/`)  
**Commit Range:** `HEAD~3..HEAD` (`82d1c47`, `6022abc`, `cff4afd`)  
**Methodology:** Hierarchical delegation across 4 review tranches with compiler-grade LSP diagnostics (`mcpls`), AST inspection, and runtime concurrency simulations under Python 3.14 / Trio.

---

## 1. Executive Summary & Top Risks

Across the 25 files touched in `HEAD~3..HEAD`, 37 concrete findings were identified and rated for **Surprisal (1–10)**:
- **1–2 (Routine / Expected):** Expected refactoring fallout, missing `__all__` entries, copy-paste helpers.
- **3–5 (Moderate / Notable):** Non-obvious edge cases, argument shadowing, validation omissions.
- **6–8 (High / Counter-Intuitive):** Cache leakage across tasks, Pydantic `@cached_property` shallow copying, Trio loop freezing via synchronous mutexes, silent capacity halving.
- **9–10 (Extreme / Shocking):** Connection deadlock on default listeners, complete silent dropping of builder configurations, permanent circuit-breaker wedging.

All modified files currently pass `mcpls:get_diagnostics` (0 compiler errors/warnings) and existing unit tests (283 passed), but contain severe latent concurrency, configuration, and protocol defects.

---

## 2. Comprehensive Findings Catalog (Categorized by Subsystem)

### Subsystem 1: Configuration & Builders (`src/trio_http_proxy/config/*`)

| # | Severity | Surprisal | Location | Summary & Mechanism | Remediation |
|---|---|:---:|---|---|---|
| 1 | **CRITICAL** | **8/10** | [`config/builder.py#L46-L50`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/builder.py#L46-L50) | `model_copy(update=...)` bypasses Pydantic v2 validation completely and retains stale `@cached_property` duration floats in `instance.__dict__`. | Rebuild via `Settings(**{**self._base_settings.model_dump(), **self._overrides})`. |
| 2 | **HIGH** | **4/10** | [`config/settings.py#L169`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L169), [`#L294`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L294) | `settings.rate_limit` flattened to `int`; sub-group attribute access (`.rate_limit_enabled`, `.rate_limit_period`) and `.rate_limit_count` crash with `AttributeError`. | Add backwards-compatibility property wrapper for `rate_limit`. |
| 3 | **HIGH** | **5/10** | [`config/settings.py#L307`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L307) | `Settings(proxy=..., cache=...)` raises `ValidationError: extra_forbidden` because sub-models were converted to `@property` accessors under `extra="forbid"`. | Intercept and unpack sub-model instances in `Settings.__init__`. |
| 4 | **MEDIUM** | **7/10** | [`config/settings.py#L307`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L307) | `Settings(*args)` accepts positional arguments, quietly swallowed by `BaseSettings` internal flags (`_case_sensitive`) without setting fields or raising errors. | Enforce keyword-only constructor: `def __init__(self, *, ...)`. |
| 5 | **MEDIUM** | **3/10** | [`config/_base.py#L63-L71`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/_base.py#L63-L71) | `normalize_optional_duration` tests `stripped == "0s"` before parsing with `allow_zero=False`; valid zero TTLs (`"0m"`, `"0h"`) raise `ValueError`. | Parse with `allow_zero=True` and check `seconds > 0`. |
| 6 | **MEDIUM** | **3/10** | [`config/builder.py#L85-L93`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/builder.py#L85-L93) | Dual kwargs (`url` vs `cache_url`, `rate_limit` vs `limit`) silently drop the second argument without error if both are provided. | Validate mutual exclusivity on conflicting arguments. |
| 7 | **LOW** | **5/10** | [`config/builder.py#L122`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/builder.py#L122) | `with_dns_cache(ttl="0s")` defaults to `"0s"`, which disables DNS caching rather than enabling it. | Make `ttl` required or default to a positive duration (e.g. `"60s"`). |
| 8 | **LOW** | **1/10** | [`config/builder.py#L265`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/builder.py#L265) | `SettingsBuilderMixin` missing from `__all__` in `builder.py` and omitted from `config/__init__.py`. | Export `SettingsBuilderMixin` in `__all__`. |
| 9 | **LOW/NOTE** | **2/10** | [`config/settings.py#L136-L155`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L136-L155) | Inconsistent schema constraints: half use declarative `Field(ge=..., le=...)`, half use imperative `@field_validator`, dropping JSON schema metadata. | Standardize on declarative constraints in `Field()`. |
| 10 | **LOW/NOTE** | **6/10** | [`config/builder.py#L248-L263`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/builder.py#L248-L263) | `ProxyCacheBuilder` context manager mutates `self._built_cache`; concurrent entries overwrite the reference, stopping the wrong cache on exit and leaking resources. | Decouple active runtime instance from reusable builder. |

---

### Subsystem 2: Proxy Core, Protocol & Streaming (`proxy.py`, `protocol.py`, `streaming.py`)

| # | Severity | Surprisal | Location | Summary & Mechanism | Remediation |
|---|---|:---:|---|---|---|
| 11 | **CRITICAL** | **9/10** | [`proxy.py#L109-L120`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L109-L120), [`#L390-L395`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L390-L395) | `ProxyBuilder.run()` passes `limiter=None` to signal listener gating; `http_proxy` interprets `None` as "use default", acquiring a *second* token on the same connection. Deadlocks permanently on `MAXCONNS=1`, halves capacity on `MAXCONNS=N`. | Use explicit sentinel `_DEFAULT = object()` in `http_proxy`. |
| 12 | **HIGH** | **9/10** | [`proxy.py#L365-L396`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L365-L396) | `ProxyBuilder.run()` builds settings but reads only `settings.port`. Configured timeouts, buffer sizes, and connection limits are completely dropped; subroutines read global env defaults. | Pass built settings into `http_proxy`, `tunnel()`, and `pipe_with_timeout()`. |
| 13 | **HIGH** | **7/10** | [`protocol.py#L85-L105`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/protocol.py#L85-L105) | Truthiness checks on `username`, `password`, `query`, and `fragment` evaluate falsy on empty strings `""`. Bypasses RFC 7231 authority-form validation on targets like `@example.com:443`, `:@`, `?`, `#`, and port `0`. | Reject targets containing `any(c in target for c in ("@", "?", "#", "/"))` and enforce `1 <= port <= 65535`. |
| 14 | **MEDIUM** | **6/10** | [`proxy.py#L673-L685`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L673-L685) | `dest_stream.aclose()` in `tunnel()` exception handler is unshielded and cancelled mid-flight if the parent nursery is cancelled. Pipelined `dest_stream.send_all(trailing_data)` lacks a timeout scope and can stall indefinitely. | Use `trio.aclose_forcefully(dest_stream)` and wrap trailing data writes in `dest_connect_timeout`. |
| 15 | **MEDIUM** | **5/10** | [`proxy.py#L397-L424`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L397-L424) | Fragile bytecode sniffing (`co_varnames`) and `tb_next is None` inspection replacing reflection fails on wrapped callables, `partial` objects, and Cython extensions. | Restore standard `inspect.signature` during cold server startup. |
| 16 | **LOW** | **4/10** | [`streaming.py#L39-L44`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/streaming.py#L39-L44) | `on_receive()` is invoked on 0-byte EOF before loop break, prematurely resetting deadline to `send_timeout`. | Check `if not chunk: break` before calling `on_receive()`. |
| 17 | **LOW** | **2/10** | [`protocol.py#L22`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/protocol.py#L22), [`streaming.py#L17`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/streaming.py#L17) | Duplicate `ConnectionFilter` instances attached to shared package root logger. | Use `logging.getLogger(__name__)` in submodules. |
| 18 | **NOTE** | **5/10** | [`proxy.py#L76`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L76), [`streaming.py#L51`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/streaming.py#L51) | Unparenthesized exception tuples (`except A, B:`) trigger `SyntaxError` on Python 3.10–3.13. | Parenthesize exception lists: `except (A, B):`. |

---

### Subsystem 3: Cache, Lifecycle & Circuit Breaker (`proxy_cache.py`, `cache/*`, `circuit_breaker/*`)

| # | Severity | Surprisal | Location | Summary & Mechanism | Remediation |
|---|---|:---:|---|---|---|
| 19 | **CRITICAL** | **8/10** | [`proxy_cache.py#L385-L483`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L385-L483) | Synchronous `self._lock` held during remote Redis ping in thread queue blocks Trio main thread via OS `pthread_mutex_lock`, freezing the entire async event loop. | Release `self._lock` before awaiting remote ping futures. |
| 20 | **CRITICAL** | **9/10** | [`proxy_cache.py#L164-L171`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L164-L171), [`proxy.py#L488`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L488) | `record_to()` drops `permit` on fallback dispatch. In `HALF_OPEN`, `record_result` ignores permitless calls, permanently locking the breaker in `HALF_OPEN`. | Forward `permit=record.permit` in fallback calls. |
| 21 | **CRITICAL** | **6/10** | [`circuit_breaker/circuit_breaker_manager.py#L77-L111`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_breaker_manager.py#L77-L111) | If `max_destinations <= 0`, `len >= max` triggers on empty dict, calling `popitem(last=False)` and crashing with `KeyError: 'dictionary is empty'`. | Enforce `max_destinations >= 1` and guard `popitem`. |
| 22 | **HIGH** | **7/10** | [`circuit_breaker/destination_circuit_breaker.py#L130-L142`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/destination_circuit_breaker.py#L130-L142) | Legacy `can_execute()` marks probe in-flight but returns only `bool`. Callers cannot obtain a `CircuitPermit`, locking the circuit in `HALF_OPEN`. | Deprecate `can_execute()` or support permitless probe completion when probe ID is active. |
| 23 | **HIGH** | **6/10** | [`circuit_breaker/circuit_breaker_manager.py#L110`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_breaker_manager.py#L110) | Under high destination churn, `_evict_one_locked` evicts active `OPEN` breakers via `popitem(last=False)`. Next request creates a `CLOSED` breaker, dropping protection. | Prioritize evicting `CLOSED` idle breakers over `OPEN` breakers. |
| 24 | **HIGH** | **7/10** | [`proxy_cache.py#L868-L871`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L868-L871) | `if count == self.rate_limit + 1:` triggers penalty TTL only on exact boundary; concurrent bursts incrementing past boundary bypass TTL attachment entirely. | Change condition to `if count >= self.rate_limit + 1:`. |
| 25 | **MEDIUM** | **5/10** | [`proxy_cache.py#L287`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L287), [`#L343`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L343) | Checking `"dns_cache_ttl" in model_fields_set` fails on default builder instances where TTL is `"0s"`. Spawns an unneeded OS daemon thread and asyncio loop. | Base check on numerical seconds: `dns_cache_ttl_sec <= 0`. |
| 26 | **MEDIUM** | **2/10** | [`proxy_cache.py#L855-L864`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L855-L864) | Redundant `parse_duration_seconds()` called on every rate-limited request despite pre-computed seconds existing on `self`. | Use `self.rate_limit_period_sec` and `self.rate_limit_ttl_sec`. |
| 27 | **MEDIUM** | **3/10** | [`proxy_cache.py#L120-L154`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L120-L154) | `ConnectionResultBuilder.build()` does not validate port ranges (`1 <= port <= 65535`) or non-empty host strings. | Add range and non-empty string validation. |
| 28 | **LOW** | **2/10** | [`proxy_cache.py#L54`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L54) | Copy-paste helper `normalize_optional_duration_local` duplicates `config._base.normalize_optional_duration`. | Import directly from `config._base`. |
| 29 | **LOW/NOTE** | **5/10** | [`proxy.py#L437`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L437) | Synchronous `proxy_cache.stop()` calls `thread.join(timeout=2.0)` directly on Trio event loop thread. | Execute `stop()` via `trio.to_thread.run_sync`. |
| 30 | **LOW/NOTE** | **3/10** | [`proxy_cache.py#L734`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L734), [`#L762`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L762) | `self._degraded` state updated without holding `self._lock` while `is_running` acquires `self._lock`. | Synchronize degraded state transitions under `self._lock`. |

---

### Subsystem 4: Error Hierarchy & Public API Surface (`errors.py`, `__init__.py`)

| # | Severity | Surprisal | Location | Summary & Mechanism | Remediation |
|---|---|:---:|---|---|---|
| 31 | **HIGH** | **8/10** | [`proxy.py#L65-L83`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L65-L83) | `_http_429_response` calls `http_429_too_many_requests % retry_after`. If `retry_after` is an exception instance without positive integer seconds, `%d` raises `TypeError`, crashing the tunnel. | Return a static 429 response without `Retry-After` if integer conversion fails. |
| 32 | **HIGH** | **3/10** | [`errors.py#L1-L51`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors.py#L1-L51) | Deleting `errors/` package and creating `errors.py` breaks `from trio_http_proxy.errors.proxy_protocol_error import ProxyProtocolError` with `ModuleNotFoundError`. | Retain `errors/` package with backward-compatible deprecation shims. |
| 33 | **MEDIUM** | **4/10** | [`errors.py#L31-L42`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors.py#L31-L42) | Forwarding `**kwargs` to `super().__init__` in `RateLimitError` crashes on unexpected kwargs with `TypeError: Exception.__init__() takes no keyword arguments`. | Provide explicit defaults and filter kwargs before `super().__init__`. |
| 34 | **MEDIUM** | **3/10** | [`__init__.py#L63-L120`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/__init__.py#L63-L120) | Primary modules `"proxy"`, `"cache"`, `"http"`, context variable `"CV_PROXY_CACHE"`, and `"reset_settings_cache"` omitted from `__init__.__all__`. | Add omitted symbols to `__all__`. |
| 35 | **MEDIUM** | **2/10** | [`__init__.py#L2-L61`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/__init__.py#L2-L61) | Daisy-chained re-exports: `__init__.py` imports errors and protocols from `.cache` and `.proxy` rather than their defining modules. | Import directly from `.errors`, `.protocol`, `.streaming`, and `.circuit_breaker`. |
| 36 | **LOW** | **1/10** | [`proxy.py#L42`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L42) | Redundant `ConnectionFilter` attachment on root logger. | Guard filter attachment against duplicates. |
| 37 | **LOW** | **1/10** | [`__init__.py#L69`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/__init__.py#L69) | `"NAMESPACE_PATTERN"` breaks alphabetical sort order in `__all__`. | Move to correct alphabetical position. |

---

## 3. Recommended Remediation Order

1. **Immediate P0 Fixes (Deadlocks & Silent Data Drops):**
   - Fix Finding 11: Decouple listener gating limiter from `http_proxy` default fallback.
   - Fix Finding 12: Wire built settings from `ProxyBuilder` into `http_proxy` and subroutines.
   - Fix Finding 1: Replace `model_copy` with model recreation from dumped dict in `SettingsBuilderMixin`.
   - Fix Finding 20: Restore `record.permit` forwarding in `ConnectionResultBuilder.record_to()`.
   - Fix Finding 19: Release `self._lock` before blocking network ping in `ProxyCache.start()`.
2. **P1 Architectural & Protocol Hardening:**
   - Fix Finding 13: Sanitize authority-form CONNECT parsing in `protocol.py`.
   - Fix Finding 31: Guard `_http_429_response` formatting against non-numeric exception objects.
   - Fix Finding 2: Restore backwards-compatibility properties on `settings.rate_limit`.
   - Fix Finding 24: Use `>=` for rate-limit penalty TTL attachment.
3. **P2 Cleanup & Ergonomics:**
   - Eliminate duplicated helper `normalize_optional_duration_local` (Finding 28).
   - Parenthesize exception tuples to ensure Python 3.10–3.13 compatibility (Finding 18).
   - Complete and sanitize `__all__` exports across `__init__.py` and submodules (Findings 8, 34, 35, 37).
