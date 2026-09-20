# Task Plan: Architectural Refactoring (Perlis's Epigram 9)

## Goal
Execute the recommendations in [`findings.md`](file:///home/jeff/code/fork/trio_http_proxy/.planning/2026-09-19-code-review-perlis-epigram-9/findings.md):
1. Consolidate fragmented configuration models under Alan Perlis's Epigram 9: "It is better to have 100 functions operate on one data structure than 10 functions on 10 data structures."
2. Formalize the proxy-to-cache interface with `typing.Protocol`, eliminating runtime reflection (`inspect.signature`) from hot paths.
3. Merge atomized single-exception and single-enum files into cohesive modules.
4. Extract wire parsing and stream piping from [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) into dedicated modules.

---

## Tranches

- [ ] **Tranche 1: Configuration Consolidation (Epigram 9)**
  - Unify [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) into a single Pydantic model.
  - Delete the **31 manual forwarding properties** ([`settings.py#L128-L250`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L128-L250)) and the 56-line reconciliation loop in [`Settings.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L69-L125).
  - Update [`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74) to consume the unified configuration directly instead of accepting 18 unpacked parameters.
  - Preserve backward compatibility for environment variable aliases and attribute lookups.

- [ ] **Tranche 2.1: ProxyCacheBuilder API (Pivot)**
  - Implement a fluent `ProxyCacheBuilder` / `ProxyBuilder` API to replace the monolithic `Settings` object for initialization.
  - Refactor `proxy.py` and `tests/` to use this builder, prioritizing caller ergonomics while maintaining the underlying unified configuration data structure.

- [ ] **Tranche 2.2: ConnectionResultBuilder API (Pivot)**
  - Replace the hot-path `inspect.signature` sniffing in `proxy.py` (`_record_connection_result`).
  - Implement a `ConnectionResultBuilder` or similar construct to formalize how results are recorded to the cache without relying on a strict Protocol interface.

- [ ] **Tranche 3: Consolidate Atomized Error and State Modules**
  - Merge `errors/cache_backend_unavailable.py`, `errors/proxy_protocol_error.py`, `errors/rate_limit_error.py`, and `errors/request_too_large_error.py` into [`src/trio_http_proxy/errors.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors.py).
  - Move [`CircuitState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/circuit_state.py#L6) into [`destination_circuit_breaker.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/circuit_breaker/destination_circuit_breaker.py).
  - Move [`LifecycleState`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/cache/lifecycle_state.py#L6) into [`proxy_cache.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py).
  - Simplify package `__init__.py` re-export chains while preserving public imports.

- [ ] **Tranche 4: Modularize Lingering Concerns in `proxy.py`**
  - Extract RFC 7231 authority-form CONNECT parsing ([`process_as_connect_request`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L572)) into `protocol.py`.
  - Extract bidirectional stream piping and timeout scopes ([`pipe`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L655), [`pipe_with_timeout`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L688)) into `streaming.py`.
  - Limit [`proxy.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py) to listener loops, concurrency gating, and high-level tunnel orchestration.
  - Verify complete test coverage and ensure all tests pass.
