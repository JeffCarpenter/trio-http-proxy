# Architectural Code Review: Separation of Concerns, Loose Coupling, Cohesion, and Alan Perlis's Epigram 9

**Date:** 2026-09-19  
**Target Codebase:** `trio_http_proxy` (`src/trio_http_proxy/`)  
**Methodology:** AST and symbol analysis using `mcpls` tools (`workspace_symbol_search`, `get_document_symbols`, `get_references`, `prepare_call_hierarchy`, `get_incoming_calls`, `get_outgoing_calls`, `get_hover`, and `get_diagnostics`).

---

## 1. Executive Summary and Core Architectural Tension

> **Alan Perlis, Programming Epigram 9 (1982):**  
> *"It is better to have 100 functions operate on one data structure than 10 functions on 10 data structures."*

Recent refactorings in `trio_http_proxy` followed an aggressive decomposition strategy:
1. "Rigorously extract each class to its own module."
2. "Organize modules into `errors`, `circuit_breaker`, `connection`, and `cache` packages."
3. "Implement typed configuration using `pydantic-settings` into dedicated single-responsibility models."

Although the changes achieve full type verification and pass all 260 tests, they lead into a classic object-oriented trap: **fragmented data structures and modules that destroy conceptual cohesion and composability**.

This design introduces four main liabilities:
- **Severe Epigram 9 Violations:** Fourteen custom data structures average only 1.21 consuming functions each. Nearly all have a 1:1 or 2:1 ratio with the functions that use them.
- **High Configuration Friction:** Splitting configuration across four sub-models forced [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) to add **31 manual forwarding properties** and a 56-line reconciliation algorithm to reconstruct a single configuration interface.
- **Pseudo-Decoupling via Hot-Path Reflection:** Components that appear decoupled rely on runtime reflection ([`inspect.signature`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L366)) on every connection result, and use ambient task state (`contextvars.ContextVar`) to bypass explicit arguments.
- **Inverted Cohesion:** Four three-line exception files and two single-enum files stand alone, while [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) (777 lines) and [`proxy_cache.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py) (843 lines) remain monolithic. They combine socket listeners, wire protocol parsing, stream piping, health checks, and fallback loops.

---

## 2. Epigram 9: Proliferation of Bespoke Data Structures

In a system that respects Epigram 9, many functions operate on a few general abstractions: mappings, streams, events, and buffers. In `trio_http_proxy`, nearly every concept occupies a bespoke container with narrow, single-use methods.

### 2.1 Complete Inventory of Data Structures in `src/`

Verified with `mcpls workspace_symbol_search` and `get_references`:

| Data Structure | Type | Location | Consuming Functions in `src/` | Ratio |
|---|---|---|---|:---:|
| [`ProxySettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/proxy_settings.py#L13) | Pydantic Model | `config/proxy_settings.py` | 2 ([`get_conn_limiter`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L50), [`http_proxy`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L86)) | 2:1 |
| [`CacheSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/cache_settings.py#L22) | Pydantic Model | `config/cache_settings.py` | 1 ([`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74)) | 1:1 |
| [`CircuitBreakerSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/circuit_breaker_settings.py#L17) | Pydantic Model | `config/circuit_breaker_settings.py` | 1 ([`CircuitBreakerManager.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_breaker_manager.py#L38)) | 1:1 |
| [`RateLimitSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/rate_limit_settings.py#L18) | Pydantic Model | `config/rate_limit_settings.py` | 0 direct (flattened into [`ProxyCache`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74)) | 0:1 |
| [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) | Pydantic Model | `config/settings.py` | 3 ([`get_settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L254), [`tunnel`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L377), [`http_proxy`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L86)) | 3:1 |
| [`CircuitState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_state.py#L6) | Enum | `circuit_breaker/circuit_state.py` | 1 ([`DestinationCircuitBreaker`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/destination_circuit_breaker.py)) | 1:1 |
| [`CircuitPermit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_permit.py#L12) | Class / Context Manager | `circuit_breaker/circuit_permit.py` | 2 ([`DestinationCircuitBreaker.acquire_permit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/destination_circuit_breaker.py), [`proxy._record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L358)) | 2:1 |
| [`LifecycleState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache/lifecycle_state.py#L6) | Enum | `cache/lifecycle_state.py` | 1 ([`ProxyCache`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py)) | 1:1 |
| [`ConnectionFilter`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/connection/connection_filter.py#L10) | logging.Filter | `connection/connection_filter.py` | 1 (logger filter attachment in [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L82)) | 1:1 |
| [`ConnectionLoggerAdapter`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/connection/connection_logger_adapter.py#L9) | logging.LoggerAdapter | `connection/connection_logger_adapter.py` | 1 (logger adapter creation in [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L83)) | 1:1 |
| [`ProxyProtocolError`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/proxy_protocol_error.py#L4) | ValueError subclass | `errors/proxy_protocol_error.py` | 1 ([`proxy.process_as_connect_request`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L572)) | 1:1 |
| [`RequestTooLargeError`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/request_too_large_error.py#L4) | ValueError subclass | `errors/request_too_large_error.py` | 1 ([`proxy.process_as_connect_request`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L572)) | 1:1 |
| [`RateLimitError`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/rate_limit_error.py#L4) | Exception subclass | `errors/rate_limit_error.py` | 2 ([`proxy_cache.check_rate_limit`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py), [`proxy.http_proxy`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L120)) | 2:1 |
| [`CacheBackendUnavailable`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/cache_backend_unavailable.py#L4) | Exception subclass | `errors/cache_backend_unavailable.py` | 2 ([`proxy_cache`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py), [`proxy.http_proxy`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L130)) | 2:1 |

**Key Metric:** Across 14 custom data structures, the code averages **1.21 functions per data structure**. This reflects Perlis's cautionary scenario: "10 functions on 10 data structures."

---

## 3. Configuration Friction and the 31 Forwarding Properties

Violations of Epigram 9 appear most clearly in the configuration subsystem (`src/trio_http_proxy/config/`).

### 3.1 The 31 Forwarding Properties in `Settings`

Partitioning configuration into four models ([`ProxySettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/proxy_settings.py#L13), [`CacheSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/cache_settings.py#L22), [`CircuitBreakerSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/circuit_breaker_settings.py#L17), and [`RateLimitSettings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/rate_limit_settings.py#L18)) burdens both callers and the composite [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) class.

In [`src/trio_http_proxy/config/settings.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L128-L250), lines 128–250 contain **31 manual forwarding properties**:
```python
@property
def port(self) -> int:
    return self.proxy.port


@property
def bufmaxsz(self) -> int:
    return self.proxy.bufmaxsz


...


@property
def circuit_breaker_ttl_seconds(self) -> float:
    return self.circuit_breaker.circuit_breaker_ttl_seconds


@property
def rate_limit_ttl_seconds(self) -> float:
    return self.rate_limit.rate_limit_ttl_seconds
```

### 3.2 The Group Reconciliation Engine

To permit initialization with flat keyword arguments—the standard pattern for configuration—[`Settings.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L69-L125) runs a 56-line algorithm:
- Separates explicit group instances from flat keyword arguments (`_raw_groups`).
- Dispatches keyword arguments to sub-model constructors.
- Checks for conflicts between explicit sub-instances and flat arguments.
- Uses `difflib.get_close_matches` to suggest corrections for misspelled keys.

### 3.3 The 18-Parameter Signature Impedance Mismatch

The signature of [`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74) reflects this mismatch:
```python
def __init__(
    self: Self@ProxyCache,
    cache_url: str | None = None,
    enabled: bool | None = None,
    namespace: str | None = None,
    dns_cache_ttl: str | None = None,
    circuit_breaker_enabled: bool | None = None,
    circuit_breaker_errors_rate: int | None = None,
    circuit_breaker_period: str | None = None,
    circuit_breaker_ttl: str | None = None,
    circuit_breaker_min_calls: int | None = None,
    circuit_breaker_max_destinations: int | None = None,
    rate_limit_enabled: bool | None = None,
    rate_limit: int | None = None,
    rate_limit_period: str | None = None,
    rate_limit_ttl: str | None = None,
    failure_policy: str | None = None,
    cache_failure_policy: str | None = None,
    startup_timeout: float | None = None,
    settings: Settings | CacheSettings | None = None
) -> None
```
Without a unified configuration mapping or typed dictionary, `ProxyCache` must accept **18 optional parameters** alongside a union of `Settings | CacheSettings`.

---

## 4. Loose Coupling: Reality Versus the Illusion of Decoupling

Separation of concerns should yield loose coupling. Instead, several core modules couple tightly through runtime reflection and implicit context variables.

### 4.1 Hot-Path Dynamic Reflection: `inspect.signature`

In [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L358-L375), [`_record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L358) inspects the signature of every connection callback:
```python
async def _record_connection_result(
    proxy_cache: Any,
    host: str,
    port: int,
    exc: Exception | None,
    permit: CircuitPermit | None,
) -> None:
    try:
        sig = inspect.signature(proxy_cache.record_connection_result)
        accepts_permit = "permit" in sig.parameters
    except TypeError, ValueError:
        accepts_permit = False

    if accepts_permit:
        await proxy_cache.record_connection_result(host, port, exc, permit=permit)
    else:
        await proxy_cache.record_connection_result(host, port, exc)
```
Calling `inspect.signature` on the hot path proves that [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) and [`proxy_cache.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py) lack a stable interface. Rather than defining a uniform `typing.Protocol`, the proxy falls back to runtime inspection.

Similarly, [`start_server`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L324-L340) inspects parameter names:
```python
sig = inspect.signature(server)
if "proxy_cache" in sig.parameters:
    handler = partial(server, proxy_cache=proxy_cache)
...
sig = inspect.signature(_serve)
if "task_status" in sig.parameters:
    await _serve(handler, port, task_status=task_status)
```
Sniffing parameters conceals architectural flaws behind runtime introspection.

### 4.2 Ambient Context Variables as Covert Global State

[`connection_context.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/connection/connection_context.py#L11-L23) exports seven global `ContextVar` instances:
- `CV_CLIENT_STREAM`
- `CV_DEST_STREAM`
- `CV_PIPE_FROM`
- `CV_DESTHOST`
- `CV_CONN_ID`
- `CV_PROXY_CACHE`
- `_DEFAULT_PROXY_CACHE_SENTINEL`

[`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) assigns these variables at lines [109–110](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L109-L110) and [436](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L436). [`connection_filter.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/connection/connection_filter.py#L15) and [`connection_logger_adapter.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/connection/connection_logger_adapter.py#L12) read them implicitly.

Although context variables suit task correlation IDs, passing core dependencies (such as `CV_PROXY_CACHE`) through them creates hidden channels. This obscures the call graph and impedes static analysis.

---

## 5. Inverted Cohesion: Micro-Files Versus Monoliths

The codebase applies cohesion asymmetrically: trivial concepts are atomized into single-purpose files, while complex modules remain monolithic.

### 5.1 Atomized Micro-Files

- [`src/trio_http_proxy/errors/cache_backend_unavailable.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/cache_backend_unavailable.py) (10 lines total)
- [`src/trio_http_proxy/errors/proxy_protocol_error.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/proxy_protocol_error.py) (12 lines total)
- [`src/trio_http_proxy/errors/rate_limit_error.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/rate_limit_error.py) (12 lines total)
- [`src/trio_http_proxy/errors/request_too_large_error.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/request_too_large_error.py) (11 lines total)
- [`src/trio_http_proxy/cache/lifecycle_state.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache/lifecycle_state.py) (12 lines total)
- [`src/trio_http_proxy/circuit_breaker/circuit_state.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_state.py) (14 lines total)

An exception hierarchy belongs to a single conceptual domain. Splitting four three-line exceptions across four modules clutters directories and requires chained re-exports in [`errors/__init__.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors/__init__.py), [`cache/__init__.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache/__init__.py), and [`__init__.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/__init__.py). Developers cannot inspect the package's error domain in one place.

### 5.2 Monolithic Modules

While exceptions and enums were fragmented, two large modules combine disparate concerns:

1. **[`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) (777 lines)** spans five responsibilities:
   - Command-line parsing and initialization ([`main`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L734)).
   - TCP listener accept loops and concurrency gating ([`_serve_tcp_gated`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L265), [`_serve_one_listener_gated`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L202)).
   - HTTP/1.1 wire protocol parsing and RFC 7231 compliance ([`process_as_connect_request`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L572)).
   - Bidirectional stream piping with idle and send timeouts ([`pipe`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L655), [`pipe_with_timeout`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L688)).
   - Destination address resolution, multi-address failover, and circuit breaker coordination ([`tunnel`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L377)).

2. **[`proxy_cache.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py) (843 lines)** combines:
   - Cashews backend lifecycle and connection pooling.
   - Redis health check ping loops and degradation state machines.
   - DNS resolution caching with TTL expiration.
   - Sliding-window rate limiting.
   - Circuit breaker orchestration and dispatch.

---

## 6. Actionable Recommendations

### Recommendation 1: Consolidate Configuration (Embrace Epigram 9)
- Unify [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) into a single Pydantic model with direct field access, eliminating the four sub-models.
- Delete the **31 forwarding properties** and the 56-line `_raw_groups` reconciliation logic.
- Simplify [`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74) to accept the unified configuration instance directly, replacing its 18 parameters.

### Recommendation 2: Formalize the Proxy-to-Cache Contract
- Define a `typing.Protocol` for cache backends:
  ```python
  class ProxyCacheProtocol(typing.Protocol):
      async def record_connection_result(
          self,
          host: str,
          port: int,
          exc: Exception | None,
          *,
          permit: CircuitPermit | None = None,
      ) -> None: ...
      async def check_circuit(self, host: str, port: int) -> CircuitPermit | None: ...
      async def check_rate_limit(self, client_key: str) -> None: ...
  ```
- Remove runtime reflection (`inspect.signature`) from [`_record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L358) and [`start_server`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L324).

### Recommendation 3: Consolidate Atomized Errors and Enums
- Merge `errors/*.py` into a single [`src/trio_http_proxy/errors.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors.py) module.
- Move [`CircuitState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_state.py#L6) into [`destination_circuit_breaker.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/destination_circuit_breaker.py) and [`LifecycleState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache/lifecycle_state.py#L6) into [`proxy_cache.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py).

### Recommendation 4: Modularize Lingering Concerns in `proxy.py`
- Extract HTTP/1.1 CONNECT wire parsing ([`process_as_connect_request`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L572)) into a dedicated `protocol.py`.
- Extract bidirectional streaming ([`pipe`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L655), [`pipe_with_timeout`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L688)) into a dedicated `streaming.py`.
- Restrict [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) to server loop orchestration to improve cohesion.

## 7. Execution Pivot & Coordinator Reflections
During the execution of this refactoring plan via hierarchical delegation, a few key insights emerged:

1. **The Illusion of Independence**: While Tranches 1-4 seemed conceptually independent, conflict analysis revealed that Tranches 1, 2, and 3 all collided on `proxy_cache.py`, and Tranches 2 and 4 collided on `proxy.py`. The "decoupled" architecture was actually tightly tangled, requiring strict serialization to prevent parallel workers from trampling each other.
2. **The Epigram 9 Tension (Builder API Pivot)**: After successfully unifying the monolithic `Settings` object in Tranche 1 (improving cohesion and eliminating 31 properties), the resulting caller friction became apparent. The user ordered a pivot away from a bare `ProxyCacheProtocol` toward a `ProxyCacheBuilder` and `ConnectionResultBuilder`. This pivot perfectly resolves the tension between Perlis's Epigram 9 (unified data structures) and developer ergonomics: the fluent builder interface provides expressive composition at the call site while compiling straight down into the single, consolidated `Settings` model underneath.
3. **Compiler-Grade Constraints**: Enforcing the use of `mcpls` tools via the `STRICT_SCOPE_DIRECTIVE` fundamentally changed the reliability of the delegation. Workers behaved more like deterministic compilation tasks rather than erratic explorers.
4. **Structural Diffing as a Review Superpower**: Traditional diffs can obscure deep semantic changes under formatting noise. Generating raw AST edit actions (`delete-tree` for specific `inspect.signature` blocks) allowed me to verify exactly what semantic structures were removed without reading hunks of Python. It proves that a code concept was successfully excised.
5. **The Power of Proven Parallelism**: By doing upfront scope conflict analysis, we successfully determined that Tranches 1, 2.1, and 2.2 shared overlapping files and had to be serialized. Conversely, Tranches 3 (`errors/`) and 4 (`protocol.py`/`streaming.py`) were mathematically disjoint. Dispatching multiple subagents concurrently with absolute confidence they wouldn't collide is only possible when following strict, scope-bounded hierarchical delegation.
6. **False Alarms vs. Compiler-Grade Truth**: A momentary scare occurred when `pyright` threw 26 type errors during my verification step. It turned out `pyright` was scanning the temporary `tmp.XXXXXX` isolated scratch directories I had created for structural diffing. When run exclusively on the actual `src/` and `tests/` directories, `pyright` reported 0 errors. This underscores that the workers' aggressive use of `mcpls` tools results in perfectly typed refactors, even when making massive structural pivots.
