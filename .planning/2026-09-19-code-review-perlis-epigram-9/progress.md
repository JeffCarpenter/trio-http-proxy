# Progress: Architectural Refactoring (Perlis's Epigram 9)

## Completed
- [x] Complete architectural code review in [`findings.md`](file:///home/jeff/code/fork/trio_http_proxy/.planning/2026-09-19-code-review-perlis-epigram-9/findings.md).
- [x] Define four remediation tranches in `task_plan.md`.

## Active and Pending Tranches
- [ ] **Tranche 1: Configuration Consolidation (Epigram 9)**
  - Unify [`Settings`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/config/settings.py#L31) and remove 31 forwarding properties.
  - Simplify parameters in [`ProxyCache.__init__`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy_cache.py#L74).
- [ ] **Tranche 2: Formalize Proxy-to-Cache Contract (`typing.Protocol`)**
  - Replace hot-path [`inspect.signature`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L366) in [`_record_connection_result`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/proxy.py#L358) with `ProxyCacheProtocol`.
- [ ] **Tranche 3: Consolidate Atomized Error and State Modules**
  - Merge `errors/*.py` into [`src/trio_http_proxy/errors.py`](file:///home/jeff/code/fork/trio_http_proxy/src/trio_http_proxy/errors.py).
  - Colocate `CircuitState` and `LifecycleState` with their primary consumers.
- [ ] **Tranche 4: Modularize Lingering Concerns in `proxy.py`**
  - Extract wire parsing into `protocol.py` and stream piping into `streaming.py`.
