# Task Plan: Minimal Trio HTTP Proxy

## Goal
Extract a minimal HTTP CONNECT proxy to git branch `mini`. Build strictly on Trio structured concurrency and RFC 7231. Remove Cashews caching, the background asyncio thread, circuit breakers, rate limiters, and Pydantic settings. See [meta.md](meta.md) for architectural invariants and scope specifications.

## Current Phase
Phase 1: Design and Pure Core Extraction

## Phase Checklist
- [x] Phase 0: Setup
  - [x] Check out branch `mini`.
  - [x] Create planning files (`task_plan.md`, `findings.md`, `progress.md`, and `meta.md`).
  - [x] Untrack and ignore `.planning/.active_plan`.
  - [x] Record architectural invariants and scope in `meta.md`.
- [ ] Phase 1: Design and Pure Core Extraction
  - [ ] Write `src/trio_http_proxy/config.py`: frozen `ProxyConfig` with environment loader.
  - [ ] Write `src/trio_http_proxy/errors.py`: `ProxyError`, `ProxyProtocolError`, and `RequestTooLargeError`.
  - [ ] Write `src/trio_http_proxy/http.py`: wire-format byte responses (200, 400, 408, 414, 502, 504).
  - [ ] Write `src/trio_http_proxy/protocol.py`: pure RFC 7231 §4.3.6 target parser and incremental `h11` stream consumer.
  - [ ] Verify Phase 1 with unit tests and mcpls diagnostics.
- [ ] Phase 2: Structured Concurrency Core Implementation
  - [ ] Write `src/trio_http_proxy/streaming.py`: timeout-oblivious `pipe()` with `send_eof()`, and `pipe_with_timeout()` with `CancelScope` nudging.
  - [ ] Write `src/trio_http_proxy/proxy.py`: `tunnel()`, `http_proxy()` with nursery and `CapacityLimiter`, and `start_server()`.
  - [ ] Simplify `src/trio_http_proxy/__main__.py` and `src/trio_http_proxy/__init__.py`.
  - [ ] Verify Phase 2 with unit tests and mcpls diagnostics.
- [ ] Phase 3: Packaging and Dependency Pruning
  - [ ] Delete `cache/`, `circuit_breaker/`, `connection/`, `proxy_cache.py`, `dependencies.py`, and `util.py`.
  - [ ] Remove `cashews`, `pydantic-settings`, and `trio-asyncio` from `pyproject.toml`.
  - [ ] Run `uv sync` to refresh lockfile.
  - [ ] Update `README.md` and documentation.
- [ ] Phase 4: Verification Suite
  - [ ] Retain and adapt core tests; remove tests for deleted modules.
  - [ ] Test memory streams: 200, 400, 408, 414, 502, 504 status codes, buffer boundaries, and timeouts.
  - [ ] Test half-close: verify that client EOF signals upstream EOF while upstream response still streams back.
  - [ ] Test real TCP sockets via loopback `trio.serve_tcp`.
  - [ ] Test backpressure under `CapacityLimiter`.
- [ ] Phase 5: Quality Gates
  - [ ] Verify zero diagnostics via mcpls.
  - [ ] Run `ruff format` and `ruff check --fix`.
  - [ ] Run `pyright` and `ty check`.
  - [ ] Run `pytest`.
  - [ ] Perform live `curl` smoke test.

## Decisions Made
- Checked out dedicated branch `mini` from `main`.
- Moved architecture and scope specification to `meta.md` and decremented phase numbers.
- Enforce strict nursery scoping (`trio.open_nursery`): no task outlives its caller.
- Support bidirectional TCP half-close via `send_eof()`.
- Guard against Slowloris reads and stalled send buffers by nudging cancel scope deadlines.
- Use frozen dataclass for configuration; avoid mutable module state and Pydantic models.
- Untracked and ignored `.planning/.active_plan`.

## Errors Encountered
- None.
