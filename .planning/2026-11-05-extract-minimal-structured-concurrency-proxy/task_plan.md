# Task Plan: Minimal Trio HTTP Proxy

## Goal
Extract a minimal HTTP CONNECT proxy to git branch `mini`. Build strictly on Trio structured concurrency and RFC 7231. Remove Cashews caching, the background asyncio thread, circuit breakers, rate limiters, and Pydantic settings. See [meta.md](meta.md) for architectural invariants and scope specifications.

## Current Phase
Phase 3: Packaging and Dependency Pruning (Dispatched to Worker 1)

## Phase Checklist
- [x] Phase 0: Setup
  - [x] Check out branch `mini`.
  - [x] Create planning files (`task_plan.md`, `findings.md`, `progress.md`, and `meta.md`).
  - [x] Untrack and ignore `.planning/.active_plan`.
  - [x] Record architectural invariants and scope in `meta.md`.
- [x] Phase 1: Design and Pure Core Extraction
  - [x] Write `src/trio_http_proxy/config.py`: frozen `ProxyConfig` with environment loader.
  - [x] Write `src/trio_http_proxy/errors.py`: `ProxyError`, `ProxyProtocolError`, and `RequestTooLargeError`.
  - [x] Write `src/trio_http_proxy/http.py`: wire-format byte responses (200, 400, 408, 414, 502, 504).
  - [x] Write `src/trio_http_proxy/protocol.py`: pure RFC 7231 §4.3.6 target parser and incremental `h11` stream consumer.
  - [x] Write `tests/test_config.py` and `tests/test_protocol.py`.
- [x] Phase 2: Structured Concurrency Core Implementation
  - [x] Write `src/trio_http_proxy/streaming.py`: timeout-oblivious `pipe()` with `send_eof()`, and `pipe_with_timeout()` with `CancelScope` nudging.
  - [x] Write `src/trio_http_proxy/proxy.py`: `tunnel()`, `http_proxy()` with nursery and `CapacityLimiter`, and `start_server()`.
- [ ] Phase 3: Packaging and Dependency Pruning [IN PROGRESS - Tranche T0]
  - [ ] Delete legacy modules (`cache/`, `circuit_breaker/`, `connection/`, `proxy_cache.py`, `dependencies.py`, `util.py`).
  - [ ] Update `src/trio_http_proxy/__init__.py` and `src/trio_http_proxy/__main__.py`.
  - [ ] Remove `cashews`, `pydantic-settings`, and `trio-asyncio` from `pyproject.toml`.
  - [ ] Run `uv sync` to refresh lockfile.
  - [ ] Delete legacy tests in `tests/` and clean `conftest.py`.
- [ ] Phase 4: Verification Suite [QUEUED - Tranches T1, T2, T3]
  - [ ] Stream A (T1): Rewrite `tests/test_proxy.py` (200, 400, 408, 414, 502, 504, memory streams).
  - [ ] Stream B (T2): Write `tests/test_invariants.py` (half-close, idle/send timeouts, sibling cancellation).
  - [ ] Stream C (T3): Update `tests/test_proxy_tcp_integration.py` (loopback TCP sockets, CapacityLimiter).
- [ ] Phase 5: Quality Gates & Documentation [QUEUED - Tranche T4 + Final Checks]
  - [ ] Stream D (T4): Update `README.md` and `CLAUDE.md`.
  - [ ] Run `mcpls` diagnostics, `ruff`, `pyright`, `pytest`, and live `curl` smoke test.

## Scheduler Status (hierarchical-delegation)
- `pool_size`: 5
- `active_workers`: 1
- `in_flight`:
  - `T0`: Worker 1 (`63e384ef-ac35-48aa-a9b7-5a538c5114a6`) - Foundation cleanup & packaging
- `queued`:
  - `T1` (Stream A: `tests/test_proxy.py`)
  - `T2` (Stream B: `tests/test_invariants.py`)
  - `T3` (Stream C: `tests/test_proxy_tcp_integration.py`)
  - `T4` (Stream D: `README.md`, `CLAUDE.md`)

## Decisions Made
- Checked out dedicated branch `mini` from `main`.
- Moved architecture and scope specification to `meta.md`.
- Enforce strict nursery scoping (`trio.open_nursery`): no task outlives its caller.
- Support bidirectional TCP half-close via `send_eof()`.
- Guard against Slowloris reads and stalled send buffers by nudging cancel scope deadlines.
- Use frozen dataclass for configuration; avoid mutable module state and Pydantic models.
- Adopted hierarchical-delegation with `pool_size = 5`.
- Untracked and ignored `.planning/.active_plan`.

## Errors Encountered
- None.
